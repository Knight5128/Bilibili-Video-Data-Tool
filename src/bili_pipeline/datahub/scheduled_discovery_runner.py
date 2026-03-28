from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from bili_pipeline.discover.export_csv import (
    discover_entries_to_full_export_rows,
    export_discover_result_csv,
    export_rows_csv,
    rankboard_entries_to_rows,
)

from .discover_ops import (
    DEFAULT_VIDEO_POOL_OUTPUT_DIR,
    FULL_SITE_FLOORINGS_LOGS_DIR,
    CustomSeedSelection,
    build_custom_seed_result,
    build_owner_since_overrides,
    build_rankboard_result,
    build_result_from_owner_mids_with_guardrails,
    coerce_end_datetime,
    coerce_start_datetime,
    display_path,
    drop_existing_uid_expansion_duplicates,
    load_full_export_tids,
    load_rankboard_boards,
    normalize_output_root,
    prepare_uid_expansion_session,
    read_owner_mid_csv,
    record_uid_expansion_part,
    save_owner_mid_csv,
    write_uid_expansion_summary,
)
from .shared import save_timestamped_task_log


SCHEDULED_DISCOVERY_LOGS_DIR = DEFAULT_VIDEO_POOL_OUTPUT_DIR / "scheduled_runs" / "logs"


@dataclass(slots=True)
class ScheduledDiscoveryRunResult:
    started_at: datetime
    finished_at: datetime
    full_export_csv_path: str
    full_export_authors_csv_path: str
    uid_expansion_video_path: str
    uid_expansion_remaining_path: str | None
    uid_expansion_summary_path: str | None
    log_path: str | None
    tracking_ups_path: str
    tracking_owner_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": self.finished_at.isoformat(timespec="seconds"),
            "full_export_csv_path": self.full_export_csv_path,
            "full_export_authors_csv_path": self.full_export_authors_csv_path,
            "uid_expansion_video_path": self.uid_expansion_video_path,
            "uid_expansion_remaining_path": self.uid_expansion_remaining_path,
            "uid_expansion_summary_path": self.uid_expansion_summary_path,
            "log_path": self.log_path,
            "tracking_ups_path": self.tracking_ups_path,
            "tracking_owner_count": self.tracking_owner_count,
        }


def run_scheduled_discovery_cycle(
    *,
    tracking_ups_path: Path | str,
    uid_expansion_window_days: int = 14,
    output_root: Path | str | None = None,
) -> ScheduledDiscoveryRunResult:
    started_at = datetime.now().replace(microsecond=0)
    output_root_path = normalize_output_root(str(output_root or DEFAULT_VIDEO_POOL_OUTPUT_DIR))
    tracking_path = Path(tracking_ups_path)
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    if int(uid_expansion_window_days) <= 0:
        raise ValueError("uid_expansion_window_days 必须大于 0。")
    if not tracking_path.exists():
        raise FileNotFoundError(f"未找到 tracking_ups.csv：{tracking_path}")

    owner_mids = read_owner_mid_csv(tracking_path)
    if not owner_mids:
        raise ValueError(f"文件中未解析出有效 owner_mid：{tracking_path}")

    _log(f"[INFO] 已读取 tracking_ups 作者数：{len(owner_mids)}。")
    _log("[INFO] 开始执行热门 400 + 实时排行榜视频列表抓取。")

    full_selection = CustomSeedSelection(
        include_daily_hot=True,
        weekly_weeks=0,
        include_column_top=False,
        include_rankboard=False,
    )
    seed_result = build_custom_seed_result(full_selection, load_full_export_tids(), logger=_log)
    rankboard_result = build_rankboard_result(load_rankboard_boards(), logger=_log)

    full_export_rows: list[dict[str, Any]] = []
    full_export_rows.extend(discover_entries_to_full_export_rows(seed_result.entries))
    full_export_rows.extend(rankboard_entries_to_rows(rankboard_result.entries))
    full_export_csv_path = output_root_path / "full_site_floorings" / f"daily_hot-rankboard-{started_at.strftime('%Y%m%d_%H%M%S')}.csv"
    saved_full_export = export_rows_csv(full_export_rows, full_export_csv_path)
    exported_owner_mids = pd.Series(
        [row.get("owner_mid") for row in full_export_rows if row.get("owner_mid") not in (None, "")]
    ).dropna()
    unique_owner_mids = list(dict.fromkeys(int(value) for value in exported_owner_mids.tolist()))
    saved_full_export_authors = save_owner_mid_csv(unique_owner_mids, saved_full_export.with_name(f"{saved_full_export.stem}_authors.csv"))
    _log(f"[INFO] 已导出热门/排行榜视频列表：{display_path(saved_full_export)}。")

    requested_start_at = coerce_start_datetime((started_at - timedelta(days=int(uid_expansion_window_days))).date())
    requested_end_at = coerce_end_datetime(started_at.date(), reference_now=started_at)
    session = prepare_uid_expansion_session(
        output_root_path,
        [tracking_path.name],
        owner_mids,
        requested_start_at,
        requested_end_at,
        started_at,
        logger=_log,
    )
    live_log_path = session.logs_dir / f"uid_expansion_part_{session.part_number}_running.log"
    session.logs_dir.mkdir(parents=True, exist_ok=True)
    if session.is_new_session:
        session.session_dir.mkdir(parents=True, exist_ok=True)
        save_owner_mid_csv(owner_mids, session.session_dir / "original_uids.csv")
    owner_since_overrides, reused_owner_count = build_owner_since_overrides(
        owner_mids,
        output_root_path,
        requested_start_at,
        requested_end_at,
        excluded_session_dirs=[session.session_dir],
        logger=_log,
    )
    outcome = build_result_from_owner_mids_with_guardrails(
        owner_mids,
        requested_start_at,
        requested_end_at,
        owner_since_overrides=owner_since_overrides,
        logger=_log,
    )
    outcome.result, _removed_count = drop_existing_uid_expansion_duplicates(outcome.result, output_root_path, logger=_log)
    saved_uid_expansion = export_discover_result_csv(outcome.result, session.session_dir / f"videolist_part_{session.part_number}.csv")
    remaining_saved = (
        save_owner_mid_csv(outcome.remaining_owner_mids, session.session_dir / f"remaining_uids_part_{session.part_number}.csv")
        if outcome.remaining_owner_mids
        else None
    )

    finished_at = datetime.now().replace(microsecond=0)
    _log(f"[INFO] uid_expansion 导出完成：{display_path(saved_uid_expansion)}。")
    log_path = save_timestamped_task_log("scheduled_discovery_cycle", logs, log_dir=SCHEDULED_DISCOVERY_LOGS_DIR)
    summary_saved = None
    if live_log_path.exists():
        live_log_path.unlink(missing_ok=True)
    if log_path is not None:
        state = record_uid_expansion_part(
            session,
            requested_start_at,
            requested_end_at,
            owner_since_overrides,
            reused_owner_count,
            len(owner_mids),
            outcome,
            saved_uid_expansion,
            remaining_saved,
            log_path,
            started_at.isoformat(timespec="seconds"),
            finished_at.isoformat(timespec="seconds"),
        )
        if not outcome.remaining_owner_mids:
            summary_saved = write_uid_expansion_summary(session.session_dir, state)

    return ScheduledDiscoveryRunResult(
        started_at=started_at,
        finished_at=finished_at,
        full_export_csv_path=saved_full_export.as_posix(),
        full_export_authors_csv_path=saved_full_export_authors.as_posix(),
        uid_expansion_video_path=saved_uid_expansion.as_posix(),
        uid_expansion_remaining_path=remaining_saved.as_posix() if remaining_saved is not None else None,
        uid_expansion_summary_path=summary_saved.as_posix() if summary_saved is not None else None,
        log_path=log_path.as_posix() if log_path is not None else None,
        tracking_ups_path=tracking_path.as_posix(),
        tracking_owner_count=len(owner_mids),
    )
