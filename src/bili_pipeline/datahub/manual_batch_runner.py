from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR, crawl_bvid_list_from_csv
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy

from .discover_ops import DEFAULT_VIDEO_POOL_OUTPUT_DIR, UID_EXPANSION_DIRNAME
from .shared import save_timestamped_task_log


DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS = 14 * 24
MANUAL_CRAWLS_OUTPUT_DIR = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "manual_crawls"
MANUAL_CRAWL_STATE_FILENAME = "manual_crawl_state.json"
MANUAL_CRAWL_FILTERED_FILENAME = "filtered_video_list.csv"
MANUAL_CRAWL_STAT_COMMENT_PREFIX = "manual_crawl_stat_comment_"


def _timestamp_token(value: datetime) -> str:
    return value.strftime("%Y%m%d_%H%M%S")


def _to_utc_timestamp(raw_value: Any) -> pd.Timestamp:
    if raw_value in (None, ""):
        return pd.NaT
    text = str(raw_value).strip()
    if not text:
        return pd.NaT
    normalized = text.replace("Z", "+00:00")
    parsed = pd.to_datetime(normalized, errors="coerce", utc=True)
    return parsed if not pd.isna(parsed) else pd.NaT


def _ensure_bvid_first(columns: list[str]) -> list[str]:
    if "bvid" not in columns:
        return columns
    return ["bvid"] + [column for column in columns if column != "bvid"]


def _write_manual_state(session_dir: Path, payload: dict[str, Any]) -> Path:
    state_path = session_dir / MANUAL_CRAWL_STATE_FILENAME
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state_path


def _pick_latest_flooring_csv(directory: Path, prefix: str) -> Path | None:
    candidates = [
        path
        for path in directory.glob(f"{prefix}-*.csv")
        if path.is_file() and not path.name.endswith("_authors.csv")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _normalize_selected_uid_task_dirs(
    uid_expansions_dir: Path,
    selected_uid_task_dirs: Sequence[str] | None,
) -> list[Path]:
    selected_names = [str(item).strip() for item in (selected_uid_task_dirs or []) if str(item).strip()]
    if not selected_names:
        return []

    selected_dirs: list[Path] = []
    for selected_name in list(dict.fromkeys(selected_names)):
        candidate = (uid_expansions_dir / selected_name).resolve()
        if candidate.parent != uid_expansions_dir.resolve():
            raise ValueError(f"uid_expansion 任务目录不合法：{selected_name}")
        if not candidate.is_dir():
            raise ValueError(f"未找到 uid_expansion 任务目录：{selected_name}")
        selected_dirs.append(candidate)
    return selected_dirs


def discover_manual_batch_source_csvs(
    video_pool_root: Path | str | None = None,
    *,
    selected_uid_task_dirs: Sequence[str] | None = None,
) -> list[Path]:
    root_dir = Path(video_pool_root or DEFAULT_VIDEO_POOL_OUTPUT_DIR)
    full_site_floorings_dir = root_dir / "full_site_floorings"
    uid_expansions_dir = root_dir / UID_EXPANSION_DIRNAME

    source_paths: list[Path] = []
    if full_site_floorings_dir.exists():
        latest_daily_hot = _pick_latest_flooring_csv(full_site_floorings_dir, "daily_hot")
        latest_rankboard = _pick_latest_flooring_csv(full_site_floorings_dir, "rankboard")
        if latest_daily_hot is not None:
            source_paths.append(latest_daily_hot)
        if latest_rankboard is not None:
            source_paths.append(latest_rankboard)
    if uid_expansions_dir.exists() and selected_uid_task_dirs is not None:
        selected_dirs = _normalize_selected_uid_task_dirs(uid_expansions_dir, selected_uid_task_dirs)
        source_paths.extend(
            sorted(
                (
                    path
                    for session_dir in selected_dirs
                    for path in session_dir.glob("videolist_part_*.csv")
                    if path.is_file()
                ),
                key=lambda path: str(path.relative_to(root_dir)).replace("\\", "/"),
            )
        )
    return source_paths


def _load_candidate_dataframe(source_paths: list[Path], *, video_pool_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for source_path in source_paths:
        frame = pd.read_csv(source_path)
        if "bvid" not in frame.columns:
            continue
        enriched = frame.copy()
        enriched["bvid"] = enriched["bvid"].astype("string").fillna("").str.strip()
        enriched = enriched[enriched["bvid"].ne("")].copy()
        if enriched.empty:
            continue
        relative_path = source_path.relative_to(video_pool_root).as_posix()
        enriched["source_csv_path"] = relative_path
        enriched["source_group"] = "uid_expansions" if f"/{UID_EXPANSION_DIRNAME}/" in f"/{relative_path}/" else "full_site_floorings"
        if enriched["source_group"].eq("uid_expansions").all():
            source_priority = 0
        elif source_path.name.startswith("daily_hot-"):
            source_priority = 1
        else:
            source_priority = 2
        enriched["_source_priority"] = source_priority
        frames.append(enriched)
    if not frames:
        return pd.DataFrame(columns=["bvid", "pubdate", "source_csv_path", "source_group"])
    combined = pd.concat(frames, ignore_index=True)
    combined["pubdate"] = combined.get("pubdate", pd.Series(dtype="string")).astype("string").fillna("").str.strip()
    combined["_pubdate_ts"] = combined["pubdate"].apply(_to_utc_timestamp)
    combined["_pubdate_sort"] = combined["_pubdate_ts"].apply(
        lambda value: value.timestamp() if not pd.isna(value) else float("-inf")
    )
    combined = combined.sort_values(
        by=["_pubdate_sort", "_source_priority", "source_csv_path"],
        ascending=[False, True, True],
        kind="stable",
    )
    deduplicated = combined.drop_duplicates(subset=["bvid"], keep="first").reset_index(drop=True)
    return deduplicated


@dataclass(slots=True)
class ManualBatchCrawlResult:
    status: str
    session_dir: str
    state_path: str
    source_csv_count: int
    candidate_bvid_count: int
    filtered_bvid_count: int
    skipped_missing_pubdate_count: int
    skipped_outside_window_count: int
    stream_data_time_window_hours: int
    filtered_csv_path: str | None = None
    wrapper_log_path: str | None = None
    crawl_report: BatchCrawlReport | None = None
    error: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "session_dir": self.session_dir,
            "state_path": self.state_path,
            "source_csv_count": self.source_csv_count,
            "candidate_bvid_count": self.candidate_bvid_count,
            "filtered_bvid_count": self.filtered_bvid_count,
            "skipped_missing_pubdate_count": self.skipped_missing_pubdate_count,
            "skipped_outside_window_count": self.skipped_outside_window_count,
            "stream_data_time_window_hours": self.stream_data_time_window_hours,
            "filtered_csv_path": self.filtered_csv_path,
            "wrapper_log_path": self.wrapper_log_path,
            "crawl_report": self.crawl_report.to_dict() if self.crawl_report is not None else None,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at is not None else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at is not None else None,
        }


def run_manual_realtime_batch_crawl(
    *,
    gcp_config: GCPStorageConfig,
    stream_data_time_window_hours: int = DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS,
    parallelism: int = 2,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    credential: Any | None = None,
    media_strategy: MediaDownloadStrategy | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    video_pool_root: Path | str | None = None,
    manual_crawls_root_dir: Path | str | None = None,
    selected_uid_task_dirs: Sequence[str] | None = None,
    started_at: datetime | None = None,
) -> ManualBatchCrawlResult:
    if not gcp_config.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。请先完成 DataHub 中的 BigQuery / GCS 设置。")
    if int(stream_data_time_window_hours) <= 0:
        raise ValueError("STREAM_DATA_TIME_WINDOW 必须是大于 0 的小时数。")
    if not [str(item).strip() for item in (selected_uid_task_dirs or []) if str(item).strip()]:
        raise ValueError("请至少选择一个 uid_expansion 任务目录。")

    task_started_at = started_at or datetime.now()
    video_pool_root_path = Path(video_pool_root or DEFAULT_VIDEO_POOL_OUTPUT_DIR)
    manual_crawls_root = Path(manual_crawls_root_dir or MANUAL_CRAWLS_OUTPUT_DIR)
    session_dir = manual_crawls_root / f"{MANUAL_CRAWL_STAT_COMMENT_PREFIX}{_timestamp_token(task_started_at)}"
    session_dir.mkdir(parents=True, exist_ok=True)

    logs: list[str] = []
    state_path = _write_manual_state(
        session_dir,
        {
            "status": "preparing",
            "session_dir": session_dir.as_posix(),
            "started_at": task_started_at.isoformat(timespec="seconds"),
            "stream_data_time_window_hours": int(stream_data_time_window_hours),
        },
    )

    def _log(message: str) -> None:
        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    try:
        source_paths = discover_manual_batch_source_csvs(
            video_pool_root_path,
            selected_uid_task_dirs=selected_uid_task_dirs,
        )
        _log(f"[INFO] 已发现 {len(source_paths)} 个可用于手动批量抓取的视频列表 CSV。")
        if len(source_paths) < 2:
            _log("[WARN] 未发现最新 daily_hot / rankboard 文件，无法启动手动批量抓取。")
            wrapper_log_path = save_timestamped_task_log("manual_crawl_prepare", logs, log_dir=session_dir / "logs")
            result = ManualBatchCrawlResult(
                status="failed",
                session_dir=session_dir.as_posix(),
                state_path=state_path.as_posix(),
                source_csv_count=len(source_paths),
                candidate_bvid_count=0,
                filtered_bvid_count=0,
                skipped_missing_pubdate_count=0,
                skipped_outside_window_count=0,
                stream_data_time_window_hours=int(stream_data_time_window_hours),
                wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
                error="缺少最新 daily_hot 或 rankboard 视频列表文件。",
                started_at=task_started_at,
                finished_at=datetime.now(),
            )
            _write_manual_state(session_dir, result.to_dict())
            raise ValueError(result.error)
        if len(source_paths) == 2:
            _log("[WARN] 选中的 uid_expansion 任务目录下没有有效的 videolist_part_*.csv。")
            wrapper_log_path = save_timestamped_task_log("manual_crawl_prepare", logs, log_dir=session_dir / "logs")
            result = ManualBatchCrawlResult(
                status="failed",
                session_dir=session_dir.as_posix(),
                state_path=state_path.as_posix(),
                source_csv_count=len(source_paths),
                candidate_bvid_count=0,
                filtered_bvid_count=0,
                skipped_missing_pubdate_count=0,
                skipped_outside_window_count=0,
                stream_data_time_window_hours=int(stream_data_time_window_hours),
                wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
                error="选中的 uid_expansion 任务目录下没有有效的 videolist_part_*.csv。",
                started_at=task_started_at,
                finished_at=datetime.now(),
            )
            _write_manual_state(session_dir, result.to_dict())
            raise ValueError(result.error)

        candidate_df = _load_candidate_dataframe(source_paths, video_pool_root=video_pool_root_path)
        candidate_count = int(len(candidate_df))
        _log(f"[INFO] 去重后共得到 {candidate_count} 个唯一 bvid 候选。")

        now_utc = pd.Timestamp(task_started_at.replace(tzinfo=timezone.utc))
        window_start_utc = now_utc - timedelta(hours=int(stream_data_time_window_hours))
        missing_pubdate_mask = candidate_df["_pubdate_ts"].isna()
        within_window_mask = (
            candidate_df["_pubdate_ts"].notna()
            & (candidate_df["_pubdate_ts"] >= window_start_utc)
            & (candidate_df["_pubdate_ts"] <= now_utc)
        )
        filtered_df = candidate_df.loc[within_window_mask].copy()
        skipped_missing_pubdate_count = int(missing_pubdate_mask.sum())
        skipped_outside_window_count = int((candidate_df["_pubdate_ts"].notna() & ~within_window_mask).sum())

        _log(
            "[INFO] 发布时间窗口筛选完成："
            f"保留 {len(filtered_df)} 条，"
            f"缺少 pubdate 跳过 {skipped_missing_pubdate_count} 条，"
            f"超出窗口跳过 {skipped_outside_window_count} 条。"
        )

        filtered_csv_path = session_dir / MANUAL_CRAWL_FILTERED_FILENAME
        export_df = filtered_df.drop(columns=["_pubdate_ts", "_pubdate_sort"], errors="ignore")
        export_df = export_df.loc[:, _ensure_bvid_first(export_df.columns.tolist())]
        export_df.to_csv(filtered_csv_path, index=False, encoding="utf-8-sig")
        _log(f"[INFO] 已保存筛选后的视频列表：{filtered_csv_path.as_posix()}")

        if filtered_df.empty:
            wrapper_log_path = save_timestamped_task_log("manual_crawl_prepare", logs, log_dir=session_dir / "logs")
            result = ManualBatchCrawlResult(
                status="skipped",
                session_dir=session_dir.as_posix(),
                state_path=state_path.as_posix(),
                source_csv_count=len(source_paths),
                candidate_bvid_count=candidate_count,
                filtered_bvid_count=0,
                skipped_missing_pubdate_count=skipped_missing_pubdate_count,
                skipped_outside_window_count=skipped_outside_window_count,
                stream_data_time_window_hours=int(stream_data_time_window_hours),
                filtered_csv_path=filtered_csv_path.as_posix(),
                wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
                started_at=task_started_at,
                finished_at=datetime.now(),
            )
            _write_manual_state(session_dir, result.to_dict())
            return result

        _write_manual_state(
            session_dir,
            {
                "status": "crawling",
                "session_dir": session_dir.as_posix(),
                "started_at": task_started_at.isoformat(timespec="seconds"),
                "stream_data_time_window_hours": int(stream_data_time_window_hours),
                "source_csv_count": len(source_paths),
                "candidate_bvid_count": candidate_count,
                "filtered_bvid_count": int(len(filtered_df)),
                "filtered_csv_path": filtered_csv_path.as_posix(),
            },
        )
        _log("[INFO] 开始执行手动批量抓取（仅实时互动量 / 评论数据）。")

        crawl_report = crawl_bvid_list_from_csv(
            filtered_csv_path,
            parallelism=int(parallelism),
            enable_media=False,
            task_mode=CrawlTaskMode.REALTIME_ONLY,
            comment_limit=int(comment_limit),
            consecutive_failure_limit=int(consecutive_failure_limit),
            gcp_config=gcp_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=media_strategy,
            credential=credential,
            output_root_dir=manual_crawls_root,
            source_csv_name=filtered_csv_path.name,
            session_dir=session_dir,
        )
        _log(
            "[INFO] 手动批量抓取结束："
            f"成功 {crawl_report.success_count} 条，失败 {crawl_report.failed_count} 条，剩余 {crawl_report.remaining_count} 条。"
        )
        wrapper_log_path = save_timestamped_task_log("manual_crawl_prepare", logs, log_dir=session_dir / "logs")
        result = ManualBatchCrawlResult(
            status="completed" if crawl_report.completed_all else "partial",
            session_dir=session_dir.as_posix(),
            state_path=state_path.as_posix(),
            source_csv_count=len(source_paths),
            candidate_bvid_count=candidate_count,
            filtered_bvid_count=int(len(filtered_df)),
            skipped_missing_pubdate_count=skipped_missing_pubdate_count,
            skipped_outside_window_count=skipped_outside_window_count,
            stream_data_time_window_hours=int(stream_data_time_window_hours),
            filtered_csv_path=filtered_csv_path.as_posix(),
            wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
            crawl_report=crawl_report,
            started_at=task_started_at,
            finished_at=datetime.now(),
        )
        _write_manual_state(session_dir, result.to_dict())
        return result
    except Exception as exc:  # noqa: BLE001
        _log(f"[ERROR] 手动批量抓取失败：{exc}")
        wrapper_log_path = save_timestamped_task_log("manual_crawl_prepare", logs, log_dir=session_dir / "logs")
        result = ManualBatchCrawlResult(
            status="failed",
            session_dir=session_dir.as_posix(),
            state_path=state_path.as_posix(),
            source_csv_count=0,
            candidate_bvid_count=0,
            filtered_bvid_count=0,
            skipped_missing_pubdate_count=0,
            skipped_outside_window_count=0,
            stream_data_time_window_hours=int(stream_data_time_window_hours),
            wrapper_log_path=wrapper_log_path.as_posix() if wrapper_log_path is not None else None,
            error=str(exc),
            started_at=task_started_at,
            finished_at=datetime.now(),
        )
        _write_manual_state(session_dir, result.to_dict())
        raise
