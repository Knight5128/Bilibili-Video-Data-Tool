from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR, crawl_bvid_list_from_csv
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy
from bili_pipeline.utils.file_merge import export_dataframe

from .shared import save_timestamped_task_log


MANUAL_MEDIA_CRAWLS_OUTPUT_DIR = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "manual_crawls"
MANUAL_MEDIA_STATE_FILENAME = "manual_crawl_media_state.json"
MANUAL_MEDIA_MODE_A_INPUT_FILENAME = "manual_crawl_media_mode_A_input.csv"
MANUAL_MEDIA_MODE_B_DEDUPED_FILENAME = "manual_crawl_media_mode_B_deduped.csv"
MANUAL_MEDIA_MODE_B_FILTERED_FILENAME = "manual_crawl_media_mode_B_filtered.csv"
MANUAL_MEDIA_WAITLIST_TEMPLATE = "manual_crawl_media_waitlist_{dataset_name}.csv"

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
            "stop_category": self.stop_category,
            "stop_reason": self.stop_reason,
            "wrapper_log_paths": self.wrapper_log_paths,
            "crawl_reports": self.crawl_reports,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


def sync_manual_media_waitlist(
    *,
    store: Any,
    gcp_config: GCPStorageConfig,
    manual_crawls_root_dir: Path | str | None = None,
) -> ManualMediaWaitlistSyncResult:
    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    waitlist_path = build_manual_media_waitlist_path(
        gcp_config.bigquery_dataset,
        manual_crawls_root_dir=root_dir,
    )
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    try:
        rows = list(store.export_manual_media_waitlist_rows())
        frame = pd.DataFrame(rows)
        if frame.empty:
            frame = pd.DataFrame(columns=["bvid", "has_stat_snapshot", "has_comment_snapshot"])
        else:
            frame = _normalize_bvid_frame(frame)
            for column in ("has_stat_snapshot", "has_comment_snapshot"):
                if column in pd.DataFrame(rows).columns and column not in frame.columns:
                    frame[column] = pd.DataFrame(rows)[column]
        export_dataframe(frame, waitlist_path)
        _log(f"[INFO] 已同步待补媒体/元数据清单，共 {len(frame)} 条：{waitlist_path.as_posix()}")
        wrapper_log_path = save_timestamped_task_log("manual_media_waitlist_sync", logs, log_dir=root_dir / "logs")
        return ManualMediaWaitlistSyncResult(
            status="completed",
            waitlist_path=waitlist_path.as_posix(),
            pending_count=int(len(frame)),
            dataset_name=gcp_config.bigquery_dataset,
            wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"[ERROR] 同步待补媒体/元数据清单失败：{exc}")
        wrapper_log_path = save_timestamped_task_log("manual_media_waitlist_sync", logs, log_dir=root_dir / "logs")
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
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
) -> ManualMediaCrawlResult:
    if not gcp_config.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。")

    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    waitlist_path = build_manual_media_waitlist_path(gcp_config.bigquery_dataset, manual_crawls_root_dir=root_dir)
    if not waitlist_path.exists():
        raise ValueError("请先同步待补媒体/元数据视频清单。")

    overall_started_at = started_at or datetime.now()
    task_count = 0
    sleep_count = 0
    all_reports: list[dict[str, Any]] = []
    wrapper_log_paths: list[str] = []
    last_session_dir: Path | None = None
    last_state_path: Path | None = None
    last_stop_category = ""
    last_stop_reason = ""

    current_started_at = overall_started_at
    while True:
        session_dir = _create_session_dir(root_dir, "manual_crawl_media_mode_A", current_started_at)
        last_session_dir = session_dir
        logs: list[str] = []

        def _log(message: str) -> None:
            logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

        _log(f"[INFO] 开始 Mode A 任务目录：{session_dir.as_posix()}")
        input_df = _load_bvid_csv(waitlist_path)
        input_csv_path = session_dir / MANUAL_MEDIA_MODE_A_INPUT_FILENAME
        export_dataframe(input_df, input_csv_path)
        if input_df.empty:
            _log("[INFO] 当前待补清单为空，已跳过抓取。")
            last_state_path = _write_state(
                session_dir,
                {
                    "mode": "A",
                    "status": "skipped",
                    "waitlist_path": waitlist_path.as_posix(),
                    "input_bvid_count": 0,
                    "submitted_bvid_count": 0,
                    "started_at": overall_started_at.isoformat(timespec="seconds"),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
            wrapper_log_path = save_timestamped_task_log("manual_media_mode_A", logs, log_dir=session_dir / "logs")
            if wrapper_log_path is not None:
                wrapper_log_paths.append(wrapper_log_path.as_posix())
            return ManualMediaCrawlResult(
                status="skipped",
                mode="A",
                session_dir=session_dir.as_posix(),
                state_path=last_state_path.as_posix(),
                input_bvid_count=0,
                submitted_bvid_count=0,
                waitlist_path=waitlist_path.as_posix(),
                task_count=task_count,
                sleep_count=sleep_count,
                wrapper_log_paths=wrapper_log_paths,
                crawl_reports=all_reports,
                started_at=overall_started_at.isoformat(timespec="seconds"),
                finished_at=datetime.now().isoformat(timespec="seconds"),
            )

        crawl_report = crawl_bvid_list_from_csv(
            input_csv_path,
            parallelism=int(parallelism),
            enable_media=True,
            task_mode=CrawlTaskMode.ONCE_ONLY,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=media_strategy,
            credential=credential,
            output_root_dir=root_dir,
            source_csv_name=input_csv_path.name,
            session_dir=session_dir,
        )
        task_count += 1
        last_stop_category = _classify_report(crawl_report)
        last_stop_reason = str(getattr(crawl_report, "stop_reason", "") or "").strip()
        all_reports.append(crawl_report.to_dict())
        sync_result = sync_manual_media_waitlist(
            store=store,
            gcp_config=gcp_config,
            manual_crawls_root_dir=root_dir,
        )
        waitlist_path = Path(sync_result.waitlist_path)
        _log(
            f"[INFO] Mode A 抓取完成：成功 {getattr(crawl_report, 'success_count', 0)} 条，"
            f"失败 {getattr(crawl_report, 'failed_count', 0)} 条，剩余 {getattr(crawl_report, 'remaining_count', 0)} 条。"
        )
        _log(f"[INFO] 本轮结束后已刷新待补清单：{waitlist_path.as_posix()}。")
        if last_stop_category == "risk" and enable_sleep_resume:
            _log(f"[INFO] 命中风控，已开启睡眠机制；将在 {int(sleep_minutes)} 分钟后启动新的 Mode A 任务。")
            time.sleep(int(sleep_minutes) * 60)
            sleep_count += 1
            current_started_at = datetime.now()
            status = "partial"
            if bool(getattr(crawl_report, "completed_all", False)):
                status = "completed"
            last_state_path = _write_state(
                session_dir,
                {
                    "mode": "A",
                    "status": status,
                    "stop_category": last_stop_category,
                    "stop_reason": last_stop_reason,
                    "waitlist_path": waitlist_path.as_posix(),
                    "input_bvid_count": int(len(input_df)),
                    "submitted_bvid_count": int(len(input_df)),
                    "task_count": task_count,
                    "sleep_count": sleep_count,
                    "started_at": overall_started_at.isoformat(timespec="seconds"),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "crawl_report": crawl_report.to_dict(),
                        "sync_result": _to_serializable_dict(sync_result),
                },
            )
            wrapper_log_path = save_timestamped_task_log("manual_media_mode_A", logs, log_dir=session_dir / "logs")
            if wrapper_log_path is not None:
                wrapper_log_paths.append(wrapper_log_path.as_posix())
            continue

        status = "completed" if bool(getattr(crawl_report, "completed_all", False)) else "partial"
        if last_stop_category == "winerror":
            status = "failed"
        last_state_path = _write_state(
            session_dir,
            {
                "mode": "A",
                "status": status,
                "stop_category": last_stop_category,
                "stop_reason": last_stop_reason,
                "waitlist_path": waitlist_path.as_posix(),
                "input_bvid_count": int(len(input_df)),
                "submitted_bvid_count": int(len(input_df)),
                "task_count": task_count,
                "sleep_count": sleep_count,
                "started_at": overall_started_at.isoformat(timespec="seconds"),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "crawl_report": crawl_report.to_dict(),
                "sync_result": _to_serializable_dict(sync_result),
            },
        )
        wrapper_log_path = save_timestamped_task_log("manual_media_mode_A", logs, log_dir=session_dir / "logs")
        if wrapper_log_path is not None:
            wrapper_log_paths.append(wrapper_log_path.as_posix())
        return ManualMediaCrawlResult(
            status=status,
            mode="A",
            session_dir=session_dir.as_posix(),
            state_path=last_state_path.as_posix(),
            input_bvid_count=int(len(input_df)),
            submitted_bvid_count=int(len(input_df)),
            waitlist_path=waitlist_path.as_posix(),
            task_count=task_count,
            sleep_count=sleep_count,
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
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    started_at: datetime | None = None,
) -> ManualMediaCrawlResult:
    if not gcp_config.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。")
    if not uploaded_frames:
        raise ValueError("请至少上传一份包含 `bvid` 列的清单。")

    root_dir = Path(manual_crawls_root_dir or MANUAL_MEDIA_CRAWLS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    overall_started_at = started_at or datetime.now()
    session_dir = _create_session_dir(root_dir, "manual_crawl_media_mode_B", overall_started_at)
    logs: list[str] = []

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    merged_frame = pd.concat(uploaded_frames, ignore_index=True)
    deduplicated_df = _normalize_bvid_frame(merged_frame)
    input_bvid_count = len(dict.fromkeys(str(bvid).strip() for bvid in merged_frame.get("bvid", []) if str(bvid).strip()))
    deduplicated_csv_path = export_dataframe(deduplicated_df, session_dir / MANUAL_MEDIA_MODE_B_DEDUPED_FILENAME)
    completed_bvids = store.fetch_completed_media_metadata_bvids(deduplicated_df["bvid"].tolist())
    filtered_df = deduplicated_df.loc[~deduplicated_df["bvid"].isin(completed_bvids)].reset_index(drop=True)
    filtered_csv_path = export_dataframe(filtered_df, session_dir / MANUAL_MEDIA_MODE_B_FILTERED_FILENAME)
    skipped_completed_count = int(len(deduplicated_df) - len(filtered_df))
    _log(
        f"[INFO] Mode B 已拼接 {len(uploaded_names)} 份清单，"
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
    stop_category = ""
    stop_reason = ""
    all_reports: list[dict[str, Any]] = []
    while True:
        crawl_report = crawl_bvid_list_from_csv(
            current_csv_path,
            parallelism=int(parallelism),
            enable_media=True,
            task_mode=CrawlTaskMode.ONCE_ONLY,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=media_strategy,
            credential=credential,
            output_root_dir=root_dir,
            source_csv_name=current_csv_path.name,
            session_dir=session_dir,
        )
        task_count += 1
        stop_category = _classify_report(crawl_report)
        stop_reason = str(getattr(crawl_report, "stop_reason", "") or "").strip()
        all_reports.append(crawl_report.to_dict())
        _log(
            f"[INFO] Mode B part_{task_count} 完成：成功 {getattr(crawl_report, 'success_count', 0)} 条，"
            f"失败 {getattr(crawl_report, 'failed_count', 0)} 条，剩余 {getattr(crawl_report, 'remaining_count', 0)} 条。"
        )
        remaining_csv_path = str(getattr(crawl_report, "remaining_csv_path", "") or "").strip()
        if stop_category == "risk" and enable_sleep_resume and remaining_csv_path:
            _log(f"[INFO] 命中风控，当前任务目录内将在 {int(sleep_minutes)} 分钟后继续下一 part。")
            time.sleep(int(sleep_minutes) * 60)
            sleep_count += 1
            current_csv_path = Path(remaining_csv_path)
            continue
        break

    status = "completed" if bool(getattr(crawl_report, "completed_all", False)) else "partial"
    if stop_category == "winerror":
        status = "failed"
    state_path = _write_state(
        session_dir,
        {
            "mode": "B",
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
            "started_at": overall_started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "crawl_reports": all_reports,
        },
    )
    wrapper_log_path = save_timestamped_task_log("manual_media_mode_B", logs, log_dir=session_dir / "logs")
    return ManualMediaCrawlResult(
        status=status,
        mode="B",
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
        stop_category=stop_category,
        stop_reason=stop_reason,
        wrapper_log_paths=[wrapper_log_path.as_posix()] if wrapper_log_path is not None else [],
        crawl_reports=all_reports,
        started_at=overall_started_at.isoformat(timespec="seconds"),
        finished_at=datetime.now().isoformat(timespec="seconds"),
    )
