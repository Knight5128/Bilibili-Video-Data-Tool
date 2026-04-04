from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR, crawl_bvid_list_from_csv
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy
from bili_pipeline.utils.file_merge import export_dataframe

from .background_tasks import run_batched_crawl_from_csv
from .shared import save_timestamped_task_log


MANUAL_MEDIA_CRAWLS_OUTPUT_DIR = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "manual_crawls"
MANUAL_MEDIA_STATE_FILENAME = "manual_crawl_media_state.json"
MANUAL_MEDIA_MODE_A_INPUT_FILENAME = "manual_crawl_media_mode_A_input.csv"
MANUAL_MEDIA_MODE_B_DEDUPED_FILENAME = "manual_crawl_media_mode_B_deduped.csv"
MANUAL_MEDIA_MODE_B_FILTERED_FILENAME = "manual_crawl_media_mode_B_filtered.csv"
MANUAL_MEDIA_WAITLIST_TEMPLATE = "manual_crawl_media_waitlist_{dataset_name}.csv"
MANUAL_META_WAITLIST_TEMPLATE = "manual_crawl_meta_waitlist_{dataset_name}.csv"

_RISK_MARKERS = (
    "352",
    "412",
    "429",
    "too many requests",
    "precondition failed",
    "风控",
)
_WINERROR_MARKERS = (
    "winerror",
    "insufficient system resources",
    "not enough storage",
    "system resources",
)


def _timestamp_token(value: datetime) -> str:
    return value.strftime("%Y%m%d_%H%M%S")


def _sanitize_dataset_name(dataset_name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(dataset_name).strip()).strip("._-")
    return sanitized or "dataset"


def build_manual_media_waitlist_path(
    dataset_name: str,
    *,
    manual_crawls_root_dir: Path | str | None = None,
) -> Path:
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    return root_dir / MANUAL_MEDIA_WAITLIST_TEMPLATE.format(dataset_name=_sanitize_dataset_name(dataset_name))


def build_manual_meta_waitlist_path(
    dataset_name: str,
    *,
    manual_crawls_root_dir: Path | str | None = None,
) -> Path:
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    return root_dir / MANUAL_META_WAITLIST_TEMPLATE.format(dataset_name=_sanitize_dataset_name(dataset_name))


def _normalize_bvid_frame(df: pd.DataFrame) -> pd.DataFrame:
    if "bvid" not in df.columns:
        raise ValueError("清单中缺少 `bvid` 列。")
    working = df.copy()
    working["bvid"] = working["bvid"].astype("string").fillna("").str.strip()
    working = working.loc[working["bvid"].ne("")].copy()
    if working.empty:
        return pd.DataFrame(columns=["bvid"])
    working = working.drop_duplicates(subset=["bvid"], keep="first").reset_index(drop=True)
    return working


def _write_state(session_dir: Path, payload: dict[str, Any]) -> Path:
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path = session_dir / MANUAL_MEDIA_STATE_FILENAME
    state_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=lambda value: getattr(value, "__dict__", str(value))) + "\n",
        encoding="utf-8",
    )
    return state_path


def _create_session_dir(root_dir: Path, prefix: str, started_at: datetime) -> Path:
    session_dir = root_dir / f"{prefix}_{_timestamp_token(started_at)}"
    suffix = 2
    while session_dir.exists():
        session_dir = root_dir / f"{prefix}_{_timestamp_token(started_at)}_{suffix}"
        suffix += 1
    session_dir.mkdir(parents=True, exist_ok=True)
    return session_dir


def _load_bvid_csv(csv_path: Path) -> pd.DataFrame:
    return _normalize_bvid_frame(pd.read_csv(csv_path))


def _extract_report_messages(report: Any) -> list[str]:
    messages: list[str] = []
    stop_reason = str(getattr(report, "stop_reason", "") or "").strip()
    if stop_reason:
        messages.append(stop_reason)
    for summary in getattr(report, "summaries", []) or []:
        for error in getattr(summary, "errors", []) or []:
            text = str(error).strip()
            if text:
                messages.append(text)
    return messages


def classify_manual_media_error_text(error_text: str) -> str:
    lowered = " ".join(str(error_text or "").split()).lower()
    if any(marker in lowered for marker in _WINERROR_MARKERS):
        return "winerror"
    if any(marker in lowered for marker in _RISK_MARKERS):
        return "risk"
    return "other"


def _classify_report(report: Any) -> str:
    if bool(getattr(report, "completed_all", False)) and int(getattr(report, "remaining_count", 0) or 0) == 0:
        return "completed"
    categories = {classify_manual_media_error_text(message) for message in _extract_report_messages(report)}
    if "winerror" in categories:
        return "winerror"
    if "risk" in categories:
        return "risk"
    return "other"


def _to_serializable_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "__dict__"):
        return dict(vars(value))
    return {"value": str(value)}


@dataclass(slots=True)
class ManualMediaWaitlistSyncResult:
    status: str
    waitlist_path: str
    pending_count: int
    dataset_name: str
    wrapper_log_path: str | None = None
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "waitlist_path": self.waitlist_path,
            "pending_count": self.pending_count,
            "dataset_name": self.dataset_name,
            "wrapper_log_path": self.wrapper_log_path,
            "error": self.error,
        }


@dataclass(slots=True)
class ManualMediaCrawlResult:
    status: str
    mode: str
    crawl_target: str
    session_dir: str
    state_path: str
    input_bvid_count: int
    submitted_bvid_count: int
    skipped_completed_count: int = 0
    deduplicated_bvid_count: int = 0
    filtered_csv_path: str | None = None
    deduplicated_csv_path: str | None = None
    waitlist_path: str | None = None
    task_count: int = 0
    sleep_count: int = 0
    cookie_refresh_count: int = 0
    stop_category: str = ""
    stop_reason: str = ""
    wrapper_log_paths: list[str] = field(default_factory=list)
    crawl_reports: list[dict[str, Any]] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "crawl_target": self.crawl_target,
            "session_dir": self.session_dir,
            "state_path": self.state_path,
            "input_bvid_count": self.input_bvid_count,
            "submitted_bvid_count": self.submitted_bvid_count,
            "skipped_completed_count": self.skipped_completed_count,
            "deduplicated_bvid_count": self.deduplicated_bvid_count,
            "filtered_csv_path": self.filtered_csv_path,
            "deduplicated_csv_path": self.deduplicated_csv_path,
            "waitlist_path": self.waitlist_path,
            "task_count": self.task_count,
            "sleep_count": self.sleep_count,
            "cookie_refresh_count": self.cookie_refresh_count,
            "stop_category": self.stop_category,
            "stop_reason": self.stop_reason,
            "wrapper_log_paths": self.wrapper_log_paths,
            "crawl_reports": self.crawl_reports,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


def _build_waitlist_path(
    crawl_target: str,
    dataset_name: str,
    *,
    manual_crawls_root_dir: Path | str | None = None,
) -> Path:
    normalized_target = str(crawl_target).strip().lower()
    if normalized_target == "meta":
        return build_manual_meta_waitlist_path(dataset_name, manual_crawls_root_dir=manual_crawls_root_dir)
    return build_manual_media_waitlist_path(dataset_name, manual_crawls_root_dir=manual_crawls_root_dir)


def _resolve_waitlist_export_rows(store: Any, crawl_target: str) -> list[dict[str, Any]]:
    normalized_target = str(crawl_target).strip().lower()
    if normalized_target == "meta":
        return list(store.export_manual_meta_waitlist_rows())
    return list(store.export_manual_media_waitlist_rows())


def _resolve_completed_bvids(store: Any, crawl_target: str, bvids: list[str]) -> set[str]:
    normalized_target = str(crawl_target).strip().lower()
    if normalized_target == "meta":
        return set(store.fetch_completed_metadata_bvids(bvids))
    return set(store.fetch_completed_media_bvids(bvids))


def _resolve_mode_settings(crawl_target: str) -> tuple[bool, CrawlTaskMode]:
    normalized_target = str(crawl_target).strip().lower()
    if normalized_target == "meta":
        return False, CrawlTaskMode.META_ONLY
    return True, CrawlTaskMode.MEDIA_ONLY


def _build_stopped_result(
    *,
    mode: str,
    crawl_target: str,
    session_dir: Path,
    overall_started_at: datetime,
    input_bvid_count: int,
    submitted_bvid_count: int,
    task_count: int,
    sleep_count: int,
    cookie_refresh_count: int,
    wrapper_log_paths: list[str],
    crawl_reports: list[dict[str, Any]],
    waitlist_path: str | None = None,
    filtered_csv_path: str | None = None,
    deduplicated_csv_path: str | None = None,
    skipped_completed_count: int = 0,
    deduplicated_bvid_count: int = 0,
    uploaded_names: list[str] | None = None,
    stop_reason: str = "用户请求停止任务。",
) -> ManualMediaCrawlResult:
    state_path = _write_state(
        session_dir,
        {
            "mode": mode,
            "crawl_target": crawl_target,
            "status": "stopped",
            "stop_category": "stopped",
            "stop_reason": stop_reason,
            "waitlist_path": waitlist_path,
            "input_bvid_count": int(input_bvid_count),
            "submitted_bvid_count": int(submitted_bvid_count),
            "skipped_completed_count": int(skipped_completed_count),
            "deduplicated_bvid_count": int(deduplicated_bvid_count),
            "task_count": int(task_count),
            "sleep_count": int(sleep_count),
            "cookie_refresh_count": int(cookie_refresh_count),
            "uploaded_names": list(uploaded_names or []),
            "started_at": overall_started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "crawl_reports": crawl_reports,
        },
    )
    return ManualMediaCrawlResult(
        status="stopped",
        mode=mode,
        crawl_target=crawl_target,
        session_dir=session_dir.as_posix(),
        state_path=state_path.as_posix(),
        input_bvid_count=int(input_bvid_count),
        submitted_bvid_count=int(submitted_bvid_count),
        skipped_completed_count=int(skipped_completed_count),
        deduplicated_bvid_count=int(deduplicated_bvid_count),
        filtered_csv_path=filtered_csv_path,
        deduplicated_csv_path=deduplicated_csv_path,
        waitlist_path=waitlist_path,
        task_count=int(task_count),
        sleep_count=int(sleep_count),
        cookie_refresh_count=int(cookie_refresh_count),
        stop_category="stopped",
        stop_reason=stop_reason,
        wrapper_log_paths=list(wrapper_log_paths),
        crawl_reports=list(crawl_reports),
        started_at=overall_started_at.isoformat(timespec="seconds"),
        finished_at=datetime.now().isoformat(timespec="seconds"),
    )


def sync_manual_media_waitlist(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
) -> ManualMediaWaitlistSyncResult:
    return _sync_manual_waitlist(
        store=store,
        gcp_config=gcp_config,
        crawl_target="media",
        manual_crawls_root_dir=manual_crawls_root_dir,
    )


def sync_manual_meta_waitlist(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
) -> ManualMediaWaitlistSyncResult:
    return _sync_manual_waitlist(
        store=store,
        gcp_config=gcp_config,
        crawl_target="meta",
        manual_crawls_root_dir=manual_crawls_root_dir,
    )


def _sync_manual_waitlist(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    crawl_target: str,
    manual_crawls_root_dir: Path | str | None = None,
) -> ManualMediaWaitlistSyncResult:
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    normalized_target = str(crawl_target).strip().lower()
    target_label = "元数据" if normalized_target == "meta" else "媒体数据"
    waitlist_path = _build_waitlist_path(
        normalized_target,
        gcp_config.bigquery_dataset,
        manual_crawls_root_dir=root_dir,
    )
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    try:
        rows = _resolve_waitlist_export_rows(store, normalized_target)
        frame = pd.DataFrame(rows)
        if frame.empty:
            frame = pd.DataFrame(columns=["bvid", "has_stat_snapshot", "has_comment_snapshot"])
        else:
            frame = _normalize_bvid_frame(frame)
            for column in ("has_stat_snapshot", "has_comment_snapshot"):
                if column in pd.DataFrame(rows).columns and column not in frame.columns:
                    frame[column] = pd.DataFrame(rows)[column]
        export_dataframe(frame, waitlist_path)
        _log(f"[INFO] 已同步待补{target_label}清单，共 {len(frame)} 条：{waitlist_path.as_posix()}")
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_waitlist_sync", logs, log_dir=root_dir / "logs")
        return ManualMediaWaitlistSyncResult(
            status="completed",
            waitlist_path=waitlist_path.as_posix(),
            pending_count=int(len(frame)),
            dataset_name=gcp_config.bigquery_dataset,
            wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"[ERROR] 同步待补{target_label}清单失败：{exc}")
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_waitlist_sync", logs, log_dir=root_dir / "logs")
        raise


def run_manual_media_mode_a(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    return _run_manual_target_mode_a(
        crawl_target="media",
        store=store,
        gcp_config=gcp_config,
        manual_crawls_root_dir=manual_crawls_root_dir,
        enable_sleep_resume=enable_sleep_resume,
        sleep_minutes=sleep_minutes,
        parallelism=parallelism,
        comment_limit=comment_limit,
        consecutive_failure_limit=consecutive_failure_limit,
        credential=credential,
        credential_provider=credential_provider,
        cookie_refresh_batch_size=cookie_refresh_batch_size,
        media_strategy=media_strategy,
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        started_at=started_at,
        should_stop=should_stop,
    )


def run_manual_meta_mode_a(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    return _run_manual_target_mode_a(
        crawl_target="meta",
        store=store,
        gcp_config=gcp_config,
        manual_crawls_root_dir=manual_crawls_root_dir,
        enable_sleep_resume=enable_sleep_resume,
        sleep_minutes=sleep_minutes,
        parallelism=parallelism,
        comment_limit=comment_limit,
        consecutive_failure_limit=consecutive_failure_limit,
        credential=credential,
        credential_provider=credential_provider,
        cookie_refresh_batch_size=cookie_refresh_batch_size,
        media_strategy=media_strategy,
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        started_at=started_at,
        should_stop=should_stop,
    )


def _run_manual_target_mode_a(
    *,
    crawl_target: str,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    if not gcp_config.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。")

    normalized_target = str(crawl_target).strip().lower()
    target_label = "元数据" if normalized_target == "meta" else "媒体数据"
    enable_media, task_mode = _resolve_mode_settings(normalized_target)
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    waitlist_path = _build_waitlist_path(normalized_target, gcp_config.bigquery_dataset, manual_crawls_root_dir=root_dir)
    if not waitlist_path.exists():
        raise ValueError(f"请先同步待补{target_label}视频清单。")

    overall_started_at = started_at or datetime.now()
    task_count = 0
    sleep_count = 0
    cookie_refresh_count = 0
    all_reports: list[dict[str, Any]] = []
    wrapper_log_paths: list[str] = []
    session_prefix = "manual_crawl_meta_mode_A" if normalized_target == "meta" else "manual_crawl_media_mode_A"
    session_dir = _create_session_dir(root_dir, session_prefix, overall_started_at)
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    _log(f"[INFO] 开始 Mode A（{target_label}）任务目录：{session_dir.as_posix()}")
    if should_stop is not None and should_stop():
        _log(f"[INFO] Mode A（{target_label}）在抓取前收到停止请求。")
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_A", logs, log_dir=session_dir / "logs")
        if wrapper_log_path is not None:
            wrapper_log_paths.append(wrapper_log_path.as_posix())
        return _build_stopped_result(
            mode="A",
            crawl_target=normalized_target,
            session_dir=session_dir,
            overall_started_at=overall_started_at,
            input_bvid_count=0,
            submitted_bvid_count=0,
            task_count=task_count,
            sleep_count=sleep_count,
            cookie_refresh_count=cookie_refresh_count,
            wrapper_log_paths=wrapper_log_paths,
            crawl_reports=all_reports,
            waitlist_path=waitlist_path.as_posix(),
        )

    input_df = _load_bvid_csv(waitlist_path)
    input_csv_path = session_dir / MANUAL_MEDIA_MODE_A_INPUT_FILENAME
    export_dataframe(input_df, input_csv_path)
    if input_df.empty:
        _log(f"[INFO] 当前待补{target_label}清单为空，已跳过抓取。")
        state_path = _write_state(
            session_dir,
            {
                "mode": "A",
                "crawl_target": normalized_target,
                "status": "skipped",
                "waitlist_path": waitlist_path.as_posix(),
                "input_bvid_count": 0,
                "submitted_bvid_count": 0,
                "started_at": overall_started_at.isoformat(timespec="seconds"),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_A", logs, log_dir=session_dir / "logs")
        if wrapper_log_path is not None:
            wrapper_log_paths.append(wrapper_log_path.as_posix())
        return ManualMediaCrawlResult(
            status="skipped",
            mode="A",
            crawl_target=normalized_target,
            session_dir=session_dir.as_posix(),
            state_path=state_path.as_posix(),
            input_bvid_count=0,
            submitted_bvid_count=0,
            waitlist_path=waitlist_path.as_posix(),
            task_count=task_count,
            sleep_count=sleep_count,
            cookie_refresh_count=cookie_refresh_count,
            wrapper_log_paths=wrapper_log_paths,
            crawl_reports=all_reports,
            started_at=overall_started_at.isoformat(timespec="seconds"),
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )

    active_media_strategy = media_strategy
    if active_media_strategy is not None and normalized_target == "media":
        active_media_strategy.stop_checker = should_stop

    if credential_provider is None:
        crawl_report = crawl_bvid_list_from_csv(
            input_csv_path,
            parallelism=int(parallelism),
            enable_media=enable_media,
            task_mode=task_mode,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=active_media_strategy,
            credential=credential,
            output_root_dir=root_dir,
            source_csv_name=input_csv_path.name,
            session_dir=session_dir,
            should_stop=should_stop,
        )
        batched_refresh_count = 0
        batched_reports = [crawl_report]
        stopped_by_request = False
    else:
        batched_outcome = run_batched_crawl_from_csv(
            input_csv_path,
            batch_size=int(cookie_refresh_batch_size),
            crawl_fn=crawl_bvid_list_from_csv,
            credential_provider=credential_provider,
            parallelism=int(parallelism),
            enable_media=enable_media,
            task_mode=task_mode,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=active_media_strategy,
            output_root_dir=root_dir,
            session_dir=session_dir,
            should_retry_remaining_fn=lambda report: _classify_report(report) == "risk",
            should_stop=should_stop,
        )
        batched_reports = batched_outcome.reports
        batched_refresh_count = batched_outcome.credential_refresh_count
        stopped_by_request = bool(getattr(batched_outcome, "stopped_by_request", False))
        crawl_report = batched_reports[-1] if batched_reports else None
    cookie_refresh_count += batched_refresh_count
    task_count += len(batched_reports)
    all_reports.extend(report.to_dict() for report in batched_reports)
    sync_result = _sync_manual_waitlist(
        store=store,
        gcp_config=gcp_config,
        crawl_target=normalized_target,
        manual_crawls_root_dir=root_dir,
    )
    waitlist_path = Path(sync_result.waitlist_path)

    if (should_stop is not None and should_stop()) or stopped_by_request:
        _log(f"[INFO] Mode A（{target_label}）在抓取过程中收到停止请求。")
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_A", logs, log_dir=session_dir / "logs")
        if wrapper_log_path is not None:
            wrapper_log_paths.append(wrapper_log_path.as_posix())
        return _build_stopped_result(
            mode="A",
            crawl_target=normalized_target,
            session_dir=session_dir,
            overall_started_at=overall_started_at,
            input_bvid_count=int(len(input_df)),
            submitted_bvid_count=int(len(input_df)),
            task_count=task_count,
            sleep_count=sleep_count,
            cookie_refresh_count=cookie_refresh_count,
            wrapper_log_paths=wrapper_log_paths,
            crawl_reports=all_reports,
            waitlist_path=waitlist_path.as_posix(),
            filtered_csv_path=input_csv_path.as_posix(),
        )

    last_stop_category = _classify_report(crawl_report) if crawl_report is not None else ""
    last_stop_reason = str(getattr(crawl_report, "stop_reason", "") or "").strip() if crawl_report is not None else ""
    _log(
        f"[INFO] Mode A（{target_label}）抓取完成：成功 {getattr(crawl_report, 'success_count', 0)} 条，"
        f"失败 {getattr(crawl_report, 'failed_count', 0)} 条，剩余 {getattr(crawl_report, 'remaining_count', 0)} 条。"
    )
    _log(f"[INFO] 本轮结束后已刷新待补{target_label}清单：{waitlist_path.as_posix()}。")
    if last_stop_category == "risk" and enable_sleep_resume:
        _log("[INFO] 命中风控后不再 sleep，直接结束当前任务并保留剩余清单。")

    status = "completed" if bool(getattr(crawl_report, "completed_all", False)) else "partial"
    if last_stop_category == "winerror":
        status = "failed"
    state_path = _write_state(
        session_dir,
        {
            "mode": "A",
            "crawl_target": normalized_target,
            "status": status,
            "stop_category": last_stop_category,
            "stop_reason": last_stop_reason,
            "waitlist_path": waitlist_path.as_posix(),
            "input_bvid_count": int(len(input_df)),
            "submitted_bvid_count": int(len(input_df)),
            "task_count": task_count,
            "sleep_count": sleep_count,
            "cookie_refresh_count": cookie_refresh_count,
            "started_at": overall_started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "crawl_report": crawl_report.to_dict() if crawl_report is not None else None,
            "sync_result": _to_serializable_dict(sync_result),
        },
    )
    wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_A", logs, log_dir=session_dir / "logs")
    if wrapper_log_path is not None:
        wrapper_log_paths.append(wrapper_log_path.as_posix())
    return ManualMediaCrawlResult(
        status=status,
        mode="A",
        crawl_target=normalized_target,
        session_dir=session_dir.as_posix(),
        state_path=state_path.as_posix(),
        input_bvid_count=int(len(input_df)),
        submitted_bvid_count=int(len(input_df)),
        waitlist_path=waitlist_path.as_posix(),
        task_count=task_count,
        sleep_count=sleep_count,
        cookie_refresh_count=cookie_refresh_count,
        stop_category=last_stop_category,
        stop_reason=last_stop_reason,
        wrapper_log_paths=wrapper_log_paths,
        crawl_reports=all_reports,
        filtered_csv_path=input_csv_path.as_posix(),
        started_at=overall_started_at.isoformat(timespec="seconds"),
        finished_at=datetime.now().isoformat(timespec="seconds"),
    )


def run_manual_media_mode_b(
    *,
    uploaded_frames: list[pd.DataFrame],
    uploaded_names: list[str],
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    return _run_manual_target_mode_b(
        crawl_target="media",
        uploaded_frames=uploaded_frames,
        uploaded_names=uploaded_names,
        store=store,
        gcp_config=gcp_config,
        manual_crawls_root_dir=manual_crawls_root_dir,
        enable_sleep_resume=enable_sleep_resume,
        sleep_minutes=sleep_minutes,
        parallelism=parallelism,
        comment_limit=comment_limit,
        consecutive_failure_limit=consecutive_failure_limit,
        credential=credential,
        credential_provider=credential_provider,
        cookie_refresh_batch_size=cookie_refresh_batch_size,
        media_strategy=media_strategy,
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        started_at=started_at,
        should_stop=should_stop,
    )


def run_manual_meta_mode_b(
    *,
    uploaded_frames: list[pd.DataFrame],
    uploaded_names: list[str],
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    return _run_manual_target_mode_b(
        crawl_target="meta",
        uploaded_frames=uploaded_frames,
        uploaded_names=uploaded_names,
        store=store,
        gcp_config=gcp_config,
        manual_crawls_root_dir=manual_crawls_root_dir,
        enable_sleep_resume=enable_sleep_resume,
        sleep_minutes=sleep_minutes,
        parallelism=parallelism,
        comment_limit=comment_limit,
        consecutive_failure_limit=consecutive_failure_limit,
        credential=credential,
        credential_provider=credential_provider,
        cookie_refresh_batch_size=cookie_refresh_batch_size,
        media_strategy=media_strategy,
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        started_at=started_at,
        should_stop=should_stop,
    )


def _run_manual_target_mode_b(
    *,
    crawl_target: str,
    uploaded_frames: list[pd.DataFrame],
    uploaded_names: list[str],
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
    enable_sleep_resume: bool = False,
    sleep_minutes: int = 5,
    parallelism: int = 1,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    credential_provider: Any | None = None,
    cookie_refresh_batch_size: int = 100,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ManualMediaCrawlResult:
    if not gcp_config.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。")
    if not uploaded_frames:
        raise ValueError("请至少上传一份包含 `bvid` 列的清单。")

    normalized_target = str(crawl_target).strip().lower()
    target_label = "元数据" if normalized_target == "meta" else "媒体数据"
    enable_media, task_mode = _resolve_mode_settings(normalized_target)
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    overall_started_at = started_at or datetime.now()
    session_prefix = "manual_crawl_meta_mode_B" if normalized_target == "meta" else "manual_crawl_media_mode_B"
    session_dir = _create_session_dir(root_dir, session_prefix, overall_started_at)
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    if should_stop is not None and should_stop():
        _log(f"[INFO] Mode B（{target_label}）在抓取前收到停止请求。")
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_B", logs, log_dir=session_dir / "logs")
        wrapper_log_paths = [wrapper_log_path.as_posix()] if wrapper_log_path is not None else []
        return _build_stopped_result(
            mode="B",
            crawl_target=normalized_target,
            session_dir=session_dir,
            overall_started_at=overall_started_at,
            input_bvid_count=0,
            submitted_bvid_count=0,
            task_count=0,
            sleep_count=0,
            cookie_refresh_count=0,
            wrapper_log_paths=wrapper_log_paths,
            crawl_reports=[],
            uploaded_names=uploaded_names,
        )

    merged_frame = pd.concat(uploaded_frames, ignore_index=True)
    deduplicated_df = _normalize_bvid_frame(merged_frame)
    input_bvid_count = len(dict.fromkeys(str(bvid).strip() for bvid in merged_frame.get("bvid", []) if str(bvid).strip()))
    deduplicated_csv_path = export_dataframe(deduplicated_df, session_dir / MANUAL_MEDIA_MODE_B_DEDUPED_FILENAME)
    completed_bvids = _resolve_completed_bvids(store, normalized_target, deduplicated_df["bvid"].tolist())
    filtered_df = deduplicated_df.loc[~deduplicated_df["bvid"].isin(completed_bvids)].reset_index(drop=True)
    filtered_csv_path = export_dataframe(filtered_df, session_dir / MANUAL_MEDIA_MODE_B_FILTERED_FILENAME)
    skipped_completed_count = int(len(deduplicated_df) - len(filtered_df))
    _log(
        f"[INFO] Mode B（{target_label}）已拼接 {len(uploaded_names)} 份清单，"
        f"去重后 {len(deduplicated_df)} 条，剔除已完成 {skipped_completed_count} 条，"
        f"待抓取 {len(filtered_df)} 条。"
    )

    if filtered_df.empty:
        state_path = _write_state(
            session_dir,
            {
                "mode": "B",
                "status": "skipped",
                "input_bvid_count": int(input_bvid_count),
                "deduplicated_bvid_count": int(len(deduplicated_df)),
                "skipped_completed_count": skipped_completed_count,
                "submitted_bvid_count": 0,
                "uploaded_names": list(uploaded_names),
                "started_at": overall_started_at.isoformat(timespec="seconds"),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        wrapper_log_path = save_timestamped_task_log("manual_media_mode_B", logs, log_dir=session_dir / "logs")
        return ManualMediaCrawlResult(
            status="skipped",
            mode="B",
            crawl_target=normalized_target,
            session_dir=session_dir.as_posix(),
            state_path=state_path.as_posix(),
            input_bvid_count=int(input_bvid_count),
            deduplicated_bvid_count=int(len(deduplicated_df)),
            skipped_completed_count=skipped_completed_count,
            submitted_bvid_count=0,
            deduplicated_csv_path=deduplicated_csv_path.as_posix(),
            filtered_csv_path=filtered_csv_path.as_posix(),
            wrapper_log_paths=[wrapper_log_path.as_posix()] if wrapper_log_path is not None else [],
            started_at=overall_started_at.isoformat(timespec="seconds"),
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )

    current_csv_path = Path(filtered_csv_path)
    task_count = 0
    sleep_count = 0
    cookie_refresh_count = 0
    stop_category = ""
    stop_reason = ""
    all_reports: list[dict[str, Any]] = []
    active_media_strategy = media_strategy
    if active_media_strategy is not None and normalized_target == "media":
        active_media_strategy.stop_checker = should_stop
    if credential_provider is None:
        crawl_report = crawl_bvid_list_from_csv(
            current_csv_path,
            parallelism=int(parallelism),
            enable_media=enable_media,
            task_mode=task_mode,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=active_media_strategy,
            credential=credential,
            output_root_dir=root_dir,
            source_csv_name=current_csv_path.name,
            session_dir=session_dir,
            should_stop=should_stop,
        )
        batched_reports = [crawl_report]
        stopped_by_request = False
    else:
        batched_outcome = run_batched_crawl_from_csv(
            current_csv_path,
            batch_size=int(cookie_refresh_batch_size),
            crawl_fn=crawl_bvid_list_from_csv,
            credential_provider=credential_provider,
            parallelism=int(parallelism),
            enable_media=enable_media,
            task_mode=task_mode,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=active_media_strategy,
            output_root_dir=root_dir,
            session_dir=session_dir,
            should_retry_remaining_fn=lambda report: _classify_report(report) == "risk",
            should_stop=should_stop,
        )
        batched_reports = batched_outcome.reports
        cookie_refresh_count += batched_outcome.credential_refresh_count
        stopped_by_request = bool(getattr(batched_outcome, "stopped_by_request", False))
        crawl_report = batched_reports[-1] if batched_reports else None
    task_count += len(batched_reports)
    stop_category = _classify_report(crawl_report) if crawl_report is not None else ""
    stop_reason = str(getattr(crawl_report, "stop_reason", "") or "").strip() if crawl_report is not None else ""
    all_reports.extend(report.to_dict() for report in batched_reports)
    _log(
        f"[INFO] Mode B（{target_label}）part_{task_count} 完成：成功 {getattr(crawl_report, 'success_count', 0)} 条，"
        f"失败 {getattr(crawl_report, 'failed_count', 0)} 条，剩余 {getattr(crawl_report, 'remaining_count', 0)} 条。"
    )
    if stop_category == "risk" and enable_sleep_resume:
        _log("[INFO] 命中风控后不再 sleep，直接结束当前任务并保留剩余清单。")

    if (should_stop is not None and should_stop()) or stopped_by_request:
        wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_B", logs, log_dir=session_dir / "logs")
        wrapper_log_paths = [wrapper_log_path.as_posix()] if wrapper_log_path is not None else []
        return _build_stopped_result(
            mode="B",
            crawl_target=normalized_target,
            session_dir=session_dir,
            overall_started_at=overall_started_at,
            input_bvid_count=int(input_bvid_count),
            submitted_bvid_count=int(len(filtered_df)),
            task_count=task_count,
            sleep_count=sleep_count,
            cookie_refresh_count=cookie_refresh_count,
            wrapper_log_paths=wrapper_log_paths,
            crawl_reports=all_reports,
            filtered_csv_path=filtered_csv_path.as_posix(),
            deduplicated_csv_path=deduplicated_csv_path.as_posix(),
            skipped_completed_count=skipped_completed_count,
            deduplicated_bvid_count=int(len(deduplicated_df)),
            uploaded_names=uploaded_names,
        )

    status = "completed" if bool(getattr(crawl_report, "completed_all", False)) else "partial"
    if stop_category == "winerror":
        status = "failed"
    state_path = _write_state(
        session_dir,
        {
            "mode": "B",
            "crawl_target": normalized_target,
            "status": status,
            "stop_category": stop_category,
            "stop_reason": stop_reason,
            "input_bvid_count": int(input_bvid_count),
            "deduplicated_bvid_count": int(len(deduplicated_df)),
            "skipped_completed_count": skipped_completed_count,
            "submitted_bvid_count": int(len(filtered_df)),
            "uploaded_names": list(uploaded_names),
            "task_count": task_count,
            "sleep_count": sleep_count,
            "cookie_refresh_count": cookie_refresh_count,
            "started_at": overall_started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "crawl_reports": all_reports,
        },
    )
    wrapper_log_path = save_timestamped_task_log(f"manual_{normalized_target}_mode_B", logs, log_dir=session_dir / "logs")
    return ManualMediaCrawlResult(
        status=status,
        mode="B",
        crawl_target=normalized_target,
        session_dir=session_dir.as_posix(),
        state_path=state_path.as_posix(),
        input_bvid_count=int(input_bvid_count),
        deduplicated_bvid_count=int(len(deduplicated_df)),
        skipped_completed_count=skipped_completed_count,
        submitted_bvid_count=int(len(filtered_df)),
        deduplicated_csv_path=deduplicated_csv_path.as_posix(),
        filtered_csv_path=filtered_csv_path.as_posix(),
        task_count=task_count,
        sleep_count=sleep_count,
        cookie_refresh_count=cookie_refresh_count,
        stop_category=stop_category,
        stop_reason=stop_reason,
        wrapper_log_paths=[wrapper_log_path.as_posix()] if wrapper_log_path is not None else [],
        crawl_reports=all_reports,
        started_at=overall_started_at.isoformat(timespec="seconds"),
        finished_at=datetime.now().isoformat(timespec="seconds"),
    )
