from __future__ import annotations

import csv
import json
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bili_pipeline.collect import crawl_latest_comments as _crawl_latest_comments
from bili_pipeline.collect import crawl_stat_snapshot as _crawl_stat_snapshot
from bili_pipeline.collect import crawl_video_meta as _crawl_video_meta
from bili_pipeline.media import stream_media_to_store as _stream_media_to_store
from bili_pipeline.models import (
    BatchCrawlReport,
    CommentSnapshot,
    CrawlTaskMode,
    FullCrawlSummary,
    GCPStorageConfig,
    MediaDownloadStrategy,
    MediaResult,
    MetaResult,
    StatSnapshot,
)
from bili_pipeline.storage import BigQueryCrawlerStore
from bili_pipeline.utils.log_files import wrap_log_lines, wrap_log_text


DEFAULT_VIDEO_DATA_OUTPUT_DIR = Path("outputs") / "video_data"
BATCH_CRAWLS_OUTPUT_DIR = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "batch_crawls"
TEST_CRAWLS_OUTPUT_DIR = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "test_crawls"
BATCH_CRAWL_STATE_FILENAME = "batch_crawl_state.json"
BATCH_CRAWL_SUMMARY_LOG_BASENAME = "batch_crawl_summary"
ORIGINAL_BVIDS_FILENAME = "original_bvids.csv"
REMAINING_BVIDS_PART_PATTERN = re.compile(r"remaining_bvids_part_(\d+)\.csv$", re.IGNORECASE)


@dataclass(slots=True)
class BatchCrawlSessionContext:
    root_dir: Path
    session_dir: Path
    logs_dir: Path
    part_number: int
    is_new_session: bool


def _resolve_gcp_config(
    gcp_config: GCPStorageConfig | None = None,
    strategy: MediaDownloadStrategy | None = None,
) -> GCPStorageConfig:
    resolved = gcp_config or (strategy.gcp_config if strategy is not None else None)
    if resolved is None or not resolved.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。请至少提供 BigQuery Dataset 与 GCS Bucket。")
    return resolved


def _store(gcp_config: GCPStorageConfig) -> BigQueryCrawlerStore:
    return BigQueryCrawlerStore(gcp_config)


def _timestamp_for_dir(value: datetime) -> str:
    return value.strftime("%Y%m%d_%H%M%S")


def _timestamp_for_log(value: datetime) -> str:
    return value.strftime("%Y%m%d_%H%M%S")


def _summary_succeeded(summary: FullCrawlSummary) -> bool:
    return summary.meta_ok and summary.stat_ok and summary.comment_ok and summary.media_ok


def _coerce_task_mode(task_mode: CrawlTaskMode | str | None) -> CrawlTaskMode:
    if isinstance(task_mode, CrawlTaskMode):
        return task_mode
    if isinstance(task_mode, str) and task_mode.strip():
        return CrawlTaskMode(task_mode.strip())
    return CrawlTaskMode.FULL_BUNDLE


def _resolve_stage_flags(
    *,
    task_mode: CrawlTaskMode | str | None,
    enable_media: bool,
) -> tuple[CrawlTaskMode, bool, bool, bool]:
    resolved_mode = _coerce_task_mode(task_mode)
    include_meta = resolved_mode in {CrawlTaskMode.FULL_BUNDLE, CrawlTaskMode.ONCE_ONLY}
    include_realtime = resolved_mode in {CrawlTaskMode.FULL_BUNDLE, CrawlTaskMode.REALTIME_ONLY}
    include_media = resolved_mode in {CrawlTaskMode.FULL_BUNDLE, CrawlTaskMode.ONCE_ONLY} and bool(enable_media)
    return resolved_mode, include_meta, include_realtime, include_media


def _append_task_log(logs: list[str], message: str) -> None:
    logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def _read_csv_rows(csv_path: Path | str) -> tuple[list[str], list[dict[str, str]], list[str]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "bvid" not in (reader.fieldnames or []):
            raise ValueError("CSV must contain a 'bvid' column.")
        fieldnames = list(reader.fieldnames or [])
        rows: list[dict[str, str]] = []
        bvids: list[str] = []
        seen: set[str] = set()
        for row in reader:
            normalized_row = {key: (value if value is not None else "") for key, value in row.items()}
            rows.append(normalized_row)
            bvid = (normalized_row.get("bvid") or "").strip()
            if bvid and bvid not in seen:
                bvids.append(bvid)
                seen.add(bvid)
    return fieldnames, rows, bvids


def _write_csv_rows(fieldnames: list[str], rows: list[dict[str, str]], path: Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    return output_path


def _read_bvids_from_csv(csv_path: Path | str) -> list[str]:
    _, _, bvids = _read_csv_rows(csv_path)
    return bvids


def _load_batch_crawl_state(session_dir: Path) -> dict[str, Any]:
    state_path = session_dir / BATCH_CRAWL_STATE_FILENAME
    if not state_path.exists():
        return {"session_dir": str(session_dir), "parts": []}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"session_dir": str(session_dir), "parts": []}
    if not isinstance(payload, dict):
        return {"session_dir": str(session_dir), "parts": []}
    payload.setdefault("session_dir", str(session_dir))
    payload.setdefault("parts", [])
    return payload


def _save_batch_crawl_state(session_dir: Path, state: dict[str, Any]) -> Path:
    state_path = session_dir / BATCH_CRAWL_STATE_FILENAME
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state_path


def _infer_next_batch_part(session_dir: Path) -> int:
    max_part = 0
    for candidate in session_dir.glob("remaining_bvids_part_*.csv"):
        match = REMAINING_BVIDS_PART_PATTERN.fullmatch(candidate.name)
        if match is not None:
            max_part = max(max_part, int(match.group(1)))
    state = _load_batch_crawl_state(session_dir)
    for part in state.get("parts", []):
        try:
            max_part = max(max_part, int(part.get("part_number", 0)))
        except Exception:  # noqa: BLE001
            continue
    return max_part + 1 if max_part > 0 else 1


def _find_matching_batch_crawl_session(
    root_dir: Path,
    source_csv_name: str,
    input_bvids: list[str],
) -> tuple[Path | None, int | None]:
    match = REMAINING_BVIDS_PART_PATTERN.fullmatch(Path(source_csv_name).name)
    if match is None:
        return None, None

    uploaded_part_number = int(match.group(1))
    batch_root = root_dir / "batch_crawls"
    if not batch_root.exists():
        return None, uploaded_part_number

    candidate_paths = sorted(
        batch_root.glob(f"batch_crawl_*/remaining_bvids_part_{uploaded_part_number}.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    fallback_dirs: list[Path] = []
    exact_match_dirs: list[Path] = []
    for candidate_path in candidate_paths:
        fallback_dirs.append(candidate_path.parent)
        try:
            _, _, candidate_bvids = _read_csv_rows(candidate_path)
        except Exception:  # noqa: BLE001
            continue
        if candidate_bvids == input_bvids:
            exact_match_dirs.append(candidate_path.parent)

    if exact_match_dirs:
        return exact_match_dirs[0], uploaded_part_number
    if fallback_dirs:
        return fallback_dirs[0], uploaded_part_number
    return None, uploaded_part_number


def _prepare_batch_crawl_session(
    root_dir: Path | str | None,
    source_csv_name: str,
    input_bvids: list[str],
) -> BatchCrawlSessionContext:
    normalized_root_dir = Path(root_dir or DEFAULT_VIDEO_DATA_OUTPUT_DIR)
    session_dir, uploaded_part_number = _find_matching_batch_crawl_session(
        normalized_root_dir,
        source_csv_name,
        input_bvids,
    )
    if session_dir is not None:
        part_number = max(_infer_next_batch_part(session_dir), (uploaded_part_number or 0) + 1)
        return BatchCrawlSessionContext(
            root_dir=normalized_root_dir,
            session_dir=session_dir,
            logs_dir=session_dir / "logs",
            part_number=part_number,
            is_new_session=False,
        )

    started_at = datetime.now()
    new_session_dir = (
        normalized_root_dir
        / "batch_crawls"
        / f"batch_crawl_{_timestamp_for_dir(started_at)}"
    )
    return BatchCrawlSessionContext(
        root_dir=normalized_root_dir,
        session_dir=new_session_dir,
        logs_dir=new_session_dir / "logs",
        part_number=1,
        is_new_session=True,
    )


def _prepare_explicit_batch_crawl_session(
    session_dir: Path | str,
    *,
    root_dir: Path | str | None = None,
) -> BatchCrawlSessionContext:
    resolved_session_dir = Path(session_dir)
    return BatchCrawlSessionContext(
        root_dir=Path(root_dir or resolved_session_dir.parent),
        session_dir=resolved_session_dir,
        logs_dir=resolved_session_dir / "logs",
        part_number=_infer_next_batch_part(resolved_session_dir) if resolved_session_dir.exists() else 1,
        is_new_session=not resolved_session_dir.exists(),
    )


def _build_batch_crawl_summary_text(state: dict[str, Any]) -> str:
    parts = sorted(state.get("parts", []), key=lambda item: int(item.get("part_number", 0)))
    interruption_count = sum(1 for part in parts if part.get("stopped_due_to_consecutive_failures"))
    lines = [
        "Bilibili Video Data Crawler Batch Crawl Summary",
        f"session_dir: {state.get('session_dir', '')}",
        f"session_started_at: {state.get('session_started_at', '')}",
        f"session_completed_at: {state.get('session_completed_at', '')}",
        f"original_csv_name: {state.get('original_csv_name', '')}",
        f"task_mode: {state.get('task_mode', '')}",
        f"original_bvid_count: {state.get('original_bvid_count', 0)}",
        f"part_count: {len(parts)}",
        f"interruption_count: {interruption_count}",
        f"completed_all: {bool(state.get('completed_all'))}",
        "",
    ]
    for part in parts:
        lines.extend(
            [
                f"part_{part.get('part_number', 0)}:",
                f"  started_at: {part.get('subtask_started_at', '')}",
                f"  finished_at: {part.get('subtask_finished_at', '')}",
                f"  source_csv_name: {part.get('source_csv_name', '')}",
                f"  task_mode: {part.get('task_mode', '')}",
                f"  input_bvid_count: {part.get('input_bvid_count', 0)}",
                f"  processed_count: {part.get('processed_count', 0)}",
                f"  success_count: {part.get('success_count', 0)}",
                f"  failed_count: {part.get('failed_count', 0)}",
                f"  remaining_count: {part.get('remaining_count', 0)}",
                f"  consecutive_failure_limit: {part.get('consecutive_failure_limit', 0)}",
                f"  stopped_due_to_consecutive_failures: {bool(part.get('stopped_due_to_consecutive_failures'))}",
                f"  stop_reason: {part.get('stop_reason', '')}",
                f"  log_file: {part.get('log_file', '')}",
                f"  remaining_file: {part.get('remaining_file', '')}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _write_batch_crawl_summary_log(session_dir: Path, state: dict[str, Any], finished_at: datetime) -> Path:
    summary_path = session_dir / f"{BATCH_CRAWL_SUMMARY_LOG_BASENAME}_{_timestamp_for_log(finished_at)}.log"
    summary_started_at = state.get("session_started_at") or finished_at
    summary_path.write_text(
        wrap_log_text(
            _build_batch_crawl_summary_text(state),
            started_at=summary_started_at,
            finished_at=finished_at,
        ),
        encoding="utf-8",
    )
    return summary_path


def _record_batch_crawl_part(
    *,
    session: BatchCrawlSessionContext,
    source_csv_name: str,
    task_mode: str,
    input_bvid_count: int,
    processed_count: int,
    success_count: int,
    failed_count: int,
    remaining_count: int,
    subtask_started_at: datetime,
    subtask_finished_at: datetime,
    consecutive_failure_limit: int,
    stopped_due_to_consecutive_failures: bool,
    stop_reason: str,
    remaining_csv_path: Path | None,
    task_log_path: Path | None,
    original_bvid_count: int,
) -> tuple[dict[str, Any], Path]:
    state = _load_batch_crawl_state(session.session_dir)
    parts = [part for part in state.get("parts", []) if int(part.get("part_number", 0)) != session.part_number]
    parts.append(
        {
            "part_number": session.part_number,
            "source_csv_name": source_csv_name,
            "task_mode": task_mode,
            "subtask_started_at": subtask_started_at.isoformat(),
            "subtask_finished_at": subtask_finished_at.isoformat(),
            "input_bvid_count": input_bvid_count,
            "processed_count": processed_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "remaining_count": remaining_count,
            "consecutive_failure_limit": consecutive_failure_limit,
            "stopped_due_to_consecutive_failures": stopped_due_to_consecutive_failures,
            "stop_reason": stop_reason,
            "log_file": task_log_path.relative_to(session.session_dir).as_posix() if task_log_path is not None else "",
            "remaining_file": remaining_csv_path.name if remaining_csv_path is not None else "",
        }
    )
    parts.sort(key=lambda item: int(item.get("part_number", 0)))
    state.update(
        {
            "session_dir": str(session.session_dir),
            "session_started_at": state.get("session_started_at") or subtask_started_at.isoformat(),
            "session_completed_at": subtask_finished_at.isoformat() if remaining_count == 0 else "",
            "original_csv_name": state.get("original_csv_name") or source_csv_name,
            "task_mode": state.get("task_mode") or task_mode,
            "original_bvid_count": int(state.get("original_bvid_count") or original_bvid_count),
            "completed_all": remaining_count == 0,
            "parts": parts,
        }
    )
    state_path = _save_batch_crawl_state(session.session_dir, state)
    return state, state_path


def crawl_video_meta(
    bvid: str,
    gcp_config: GCPStorageConfig,
    credential: Any | None = None,
) -> MetaResult:
    result = _crawl_video_meta(bvid, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_video_meta(result)
    return result


def crawl_stat_snapshot(
    bvid: str,
    gcp_config: GCPStorageConfig,
    credential: Any | None = None,
) -> StatSnapshot:
    snapshot = _crawl_stat_snapshot(bvid, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_stat_snapshot(snapshot)
    return snapshot


def crawl_latest_comments(
    bvid: str,
    limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    credential: Any | None = None,
) -> CommentSnapshot:
    snapshot = _crawl_latest_comments(bvid, limit=limit, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_comment_snapshot(snapshot)
    return snapshot


def stream_media_to_store(
    bvid: str,
    strategy: MediaDownloadStrategy,
    credential: Any | None = None,
) -> MediaResult:
    gcp_config = _resolve_gcp_config(strategy=strategy)
    return _stream_media_to_store(bvid=bvid, strategy=strategy, store=_store(gcp_config), credential=credential)


def crawl_media_assets(
    bvid: str,
    strategy: MediaDownloadStrategy | None = None,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    credential: Any | None = None,
) -> MediaResult:
    resolved_config = _resolve_gcp_config(gcp_config, strategy)
    active_strategy = strategy or MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        storage_backend="gcs",
        gcp_config=resolved_config,
    )
    return stream_media_to_store(bvid=bvid, strategy=active_strategy, credential=credential)


def crawl_full_video_bundle(
    bvid: str,
    *,
    enable_media: bool = True,
    task_mode: CrawlTaskMode | str | None = None,
    comment_limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    media_strategy: MediaDownloadStrategy | None = None,
    credential: Any | None = None,
) -> FullCrawlSummary:
    resolved_config = _resolve_gcp_config(gcp_config, media_strategy)
    resolved_mode, include_meta, include_realtime, include_media = _resolve_stage_flags(
        task_mode=task_mode,
        enable_media=enable_media,
    )
    errors: list[str] = []
    meta_result = None
    stat_snapshot = None
    comment_snapshot = None
    media_result = None

    meta_ok = not include_meta
    if include_meta:
        try:
            meta_result = crawl_video_meta(bvid, gcp_config=resolved_config, credential=credential)
            meta_ok = True
        except Exception as exc:  # noqa: BLE001
            meta_ok = False
            errors.append(f"meta: {exc}")

    stat_ok = not include_realtime
    comment_ok = not include_realtime
    if include_realtime:
        try:
            stat_snapshot = crawl_stat_snapshot(bvid, gcp_config=resolved_config, credential=credential)
            stat_ok = True
        except Exception as exc:  # noqa: BLE001
            stat_ok = False
            errors.append(f"stat: {exc}")
        try:
            comment_snapshot = crawl_latest_comments(
                bvid,
                gcp_config=resolved_config,
                limit=comment_limit,
                credential=credential,
            )
            comment_ok = True
        except Exception as exc:  # noqa: BLE001
            comment_ok = False
            errors.append(f"comment: {exc}")

    media_ok = not include_media
    if include_media:
        try:
            media_result = crawl_media_assets(
                bvid,
                strategy=media_strategy,
                gcp_config=resolved_config,
                max_height=max_height,
                chunk_size_mb=chunk_size_mb,
                credential=credential,
            )
            media_ok = True
        except Exception as exc:  # noqa: BLE001
            media_ok = False
            errors.append(f"media: {exc}")

    return FullCrawlSummary(
        bvid=bvid,
        meta_ok=meta_ok,
        stat_ok=stat_ok,
        comment_ok=comment_ok,
        media_ok=media_ok,
        snapshot_time=datetime.now(),
        task_mode=resolved_mode.value,
        errors=errors,
        meta_result=meta_result,
        stat_snapshot=stat_snapshot,
        comment_snapshot=comment_snapshot,
        media_result=media_result,
    )


def crawl_bvid_list_from_csv(
    csv_path: Path | str,
    *,
    parallelism: int = 4,
    enable_media: bool = True,
    task_mode: CrawlTaskMode | str | None = None,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    media_strategy: MediaDownloadStrategy | None = None,
    credential: Any | None = None,
    output_root_dir: Path | str | None = None,
    source_csv_name: str | None = None,
    session_dir: Path | str | None = None,
) -> BatchCrawlReport:
    resolved_config = _resolve_gcp_config(gcp_config, media_strategy)
    resolved_mode, include_meta, include_realtime, include_media = _resolve_stage_flags(
        task_mode=task_mode,
        enable_media=enable_media,
    )
    fieldnames, original_rows, bvids = _read_csv_rows(csv_path)
    source_name = (source_csv_name or Path(csv_path).name).strip() or Path(csv_path).name
    session = (
        _prepare_explicit_batch_crawl_session(session_dir, root_dir=output_root_dir)
        if session_dir is not None
        else _prepare_batch_crawl_session(output_root_dir, source_name, bvids)
    )
    session.session_dir.mkdir(parents=True, exist_ok=True)
    session.logs_dir.mkdir(parents=True, exist_ok=True)
    if session.is_new_session:
        original_csv_path = session.session_dir / ORIGINAL_BVIDS_FILENAME
        if not original_csv_path.exists():
            _write_csv_rows(fieldnames, original_rows, original_csv_path)

    run_id = uuid.uuid4().hex
    started_at = datetime.now()
    store = _store(resolved_config)
    effective_parallelism = max(1, int(parallelism))
    if consecutive_failure_limit > 0 and effective_parallelism > 1:
        effective_parallelism = 1
    store.save_run_start(
        run_id,
        mode="batch_csv",
        notes={
            "csv_path": str(csv_path),
            "source_csv_name": source_name,
            "session_dir": str(session.session_dir),
            "part_number": session.part_number,
            "parallelism": parallelism,
            "effective_parallelism": effective_parallelism,
            "task_mode": resolved_mode.value,
            "include_meta": include_meta,
            "include_realtime": include_realtime,
            "include_media": include_media,
            "enable_media": enable_media,
            "comment_limit": comment_limit,
            "consecutive_failure_limit": consecutive_failure_limit,
            "bigquery_dataset": resolved_config.bigquery_dataset,
            "gcs_bucket_name": resolved_config.gcs_bucket_name,
            "max_height": max_height,
            "storage_backend": (media_strategy.storage_backend if media_strategy is not None else "gcs"),
        },
    )

    summaries: list[FullCrawlSummary] = []
    successful_bvids: list[str] = []
    failed_bvids: list[str] = []
    task_logs: list[str] = []
    _append_task_log(task_logs, f"开始执行 CSV 批量抓取子任务 part_{session.part_number}。")
    _append_task_log(task_logs, f"子任务开始时间：{started_at.isoformat()}")
    _append_task_log(task_logs, f"输入文件：{source_name}")
    _append_task_log(task_logs, f"输入唯一 bvid 数量：{len(bvids)}")
    if effective_parallelism != parallelism:
        _append_task_log(
            task_logs,
            f"为严格按 CSV 顺序判断连续失败阈值，本次任务自动将并发度从 {parallelism} 调整为 {effective_parallelism}。",
        )

    def _worker(item_bvid: str) -> FullCrawlSummary:
        return crawl_full_video_bundle(
            item_bvid,
            enable_media=enable_media,
            task_mode=resolved_mode,
            comment_limit=comment_limit,
            gcp_config=resolved_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=media_strategy,
            credential=credential,
        )

    consecutive_failures = 0
    stopped_due_to_consecutive_failures = False
    stop_reason = ""

    if effective_parallelism <= 1:
        for bvid in bvids:
            summary = _worker(bvid)
            summaries.append(summary)
            store.save_run_item(run_id, summary)
            if _summary_succeeded(summary):
                successful_bvids.append(summary.bvid)
                consecutive_failures = 0
                _append_task_log(task_logs, f"[SUCCESS] bvid={summary.bvid} 已成功抓取全部所需数据。")
            else:
                failed_bvids.append(summary.bvid)
                consecutive_failures += 1
                error_text = " | ".join(summary.errors) if summary.errors else "unknown error"
                _append_task_log(task_logs, f"[ERROR] bvid={summary.bvid} 抓取失败：{error_text}")
                if consecutive_failure_limit > 0 and consecutive_failures >= consecutive_failure_limit:
                    stopped_due_to_consecutive_failures = True
                    stop_reason = (
                        f"检测到连续 {consecutive_failures} 条视频抓取失败，"
                        f"已按阈值 {consecutive_failure_limit} 暂停当前 CSV 批量抓取流程。"
                    )
                    _append_task_log(task_logs, f"[STOP] {stop_reason}")
                    break
    else:
        with ThreadPoolExecutor(max_workers=effective_parallelism) as executor:
            futures = {executor.submit(_worker, bvid): bvid for bvid in bvids}
            for future in as_completed(futures):
                summary = future.result()
                summaries.append(summary)
                store.save_run_item(run_id, summary)
                if _summary_succeeded(summary):
                    successful_bvids.append(summary.bvid)
                else:
                    failed_bvids.append(summary.bvid)

        summaries.sort(key=lambda item: bvids.index(item.bvid))
        for summary in summaries:
            if _summary_succeeded(summary):
                _append_task_log(task_logs, f"[SUCCESS] bvid={summary.bvid} 已成功抓取全部所需数据。")
            else:
                error_text = " | ".join(summary.errors) if summary.errors else "unknown error"
                _append_task_log(task_logs, f"[ERROR] bvid={summary.bvid} 抓取失败：{error_text}")

    processed_count = len(summaries)
    success_count = len(successful_bvids)
    failed_count = len(failed_bvids)
    successful_bvid_set = set(successful_bvids)
    remaining_rows = [
        row
        for row in original_rows
        if ((row.get("bvid") or "").strip() and (row.get("bvid") or "").strip() not in successful_bvid_set)
    ]
    remaining_bvids: list[str] = []
    remaining_seen: set[str] = set()
    for row in remaining_rows:
        bvid = (row.get("bvid") or "").strip()
        if bvid and bvid not in remaining_seen:
            remaining_bvids.append(bvid)
            remaining_seen.add(bvid)

    finished_at = datetime.now()
    if not stop_reason:
        if remaining_bvids:
            stop_reason = "当前子任务已结束，但仍有未成功抓取的视频，已导出 remaining CSV 供继续执行。"
            _append_task_log(task_logs, f"[WARN] {stop_reason}")
        else:
            stop_reason = "当前 batch_crawl 已完成，所有视频均已成功抓取。"
            _append_task_log(task_logs, f"[INFO] {stop_reason}")

    remaining_csv_path = None
    if remaining_rows:
        remaining_csv_path = _write_csv_rows(
            fieldnames,
            remaining_rows,
            session.session_dir / f"remaining_bvids_part_{session.part_number}.csv",
        )
        _append_task_log(task_logs, f"[INFO] 已导出剔除成功条目后的剩余 CSV：{remaining_csv_path}")

    _append_task_log(task_logs, f"子任务结束时间：{finished_at.isoformat()}")
    task_log_path = session.logs_dir / f"batch_crawl_part_{session.part_number}_{_timestamp_for_log(finished_at)}.log"
    task_log_path.write_text(
        wrap_log_lines(task_logs, started_at=started_at, finished_at=finished_at),
        encoding="utf-8",
    )

    state, state_path = _record_batch_crawl_part(
        session=session,
        source_csv_name=source_name,
        task_mode=resolved_mode.value,
        input_bvid_count=len(bvids),
        processed_count=processed_count,
        success_count=success_count,
        failed_count=failed_count,
        remaining_count=len(remaining_bvids),
        subtask_started_at=started_at,
        subtask_finished_at=finished_at,
        consecutive_failure_limit=consecutive_failure_limit,
        stopped_due_to_consecutive_failures=stopped_due_to_consecutive_failures,
        stop_reason=stop_reason,
        remaining_csv_path=remaining_csv_path,
        task_log_path=task_log_path,
        original_bvid_count=len(bvids),
    )

    session_summary_log_path = None
    if len(remaining_bvids) == 0:
        session_summary_log_path = _write_batch_crawl_summary_log(session.session_dir, state, finished_at)
        state["session_completed_at"] = finished_at.isoformat()
        state["session_summary_log"] = session_summary_log_path.name
        state["completed_all"] = True
        state_path = _save_batch_crawl_state(session.session_dir, state)

    store.finalize_run(run_id, total_bvids=len(bvids), success_count=success_count, failed_count=failed_count)

    return BatchCrawlReport(
        run_id=run_id,
        total_bvids=len(bvids),
        processed_count=processed_count,
        success_count=success_count,
        failed_count=failed_count,
        remaining_count=len(remaining_bvids),
        started_at=started_at,
        finished_at=finished_at,
        task_mode=resolved_mode.value,
        summaries=summaries,
        part_number=session.part_number,
        effective_parallelism=effective_parallelism,
        consecutive_failure_limit=consecutive_failure_limit,
        stopped_due_to_consecutive_failures=stopped_due_to_consecutive_failures,
        completed_all=len(remaining_bvids) == 0,
        stop_reason=stop_reason,
        session_dir=str(session.session_dir),
        logs_dir=str(session.logs_dir),
        remaining_csv_path=str(remaining_csv_path) if remaining_csv_path is not None else None,
        task_log_path=str(task_log_path),
        session_state_path=str(state_path),
        session_summary_log_path=str(session_summary_log_path) if session_summary_log_path is not None else None,
        successful_bvids=successful_bvids,
        failed_bvids=failed_bvids,
    )
