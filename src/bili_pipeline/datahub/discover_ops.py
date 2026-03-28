from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path

import pandas as pd

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import (
    BilibiliHotSource,
    BilibiliRankboardSource,
    BilibiliUserRecentVideoSource,
    BilibiliWeeklyHotSource,
    BilibiliZoneTop10Source,
    VideoPoolBuilder,
    load_valid_partition_tids,
)
from bili_pipeline.models import DiscoverResult, RankboardResult
from bili_pipeline.utils.log_files import wrap_log_text
from bili_pipeline.utils.file_merge import export_dataframe

from .shared import display_path


APP_DIR = Path(__file__).resolve().parents[3]
VALID_TAGS_PATH = APP_DIR / "all_valid_tags.csv"
RANKBOARD_VALID_RIDS_PATH = APP_DIR / "rankboard_valid_rids.csv"
DEFAULT_VIDEO_POOL_OUTPUT_DIR = Path("outputs") / "video_pool"
FULL_SITE_FLOORINGS_OUTPUT_DIR = DEFAULT_VIDEO_POOL_OUTPUT_DIR / "full_site_floorings"
FULL_SITE_FLOORINGS_LOGS_DIR = FULL_SITE_FLOORINGS_OUTPUT_DIR / "logs"
BVID_TO_UIDS_OUTPUT_DIR = DEFAULT_VIDEO_POOL_OUTPUT_DIR / "bvid_to_uids"
UID_EXPANSION_DIRNAME = "uid_expansions"
UID_EXPANSION_STATE_FILENAME = "uid_expansion_state.json"
UID_EXPANSION_SUMMARY_FILENAME = "uid_expansion_summary.log"
UID_EXPANSION_ORIGINAL_UIDS_FILENAME = "original_uids.csv"
UID_EXPANSION_PART_FILE_PATTERN = re.compile(r"(?:videolist|remaining_uids)_part_(\d+)\.csv$", re.IGNORECASE)
UID_EXPANSION_SESSION_TIMESTAMP_PATTERN = re.compile(
    r"uid_expansion_(?:\d+_days_)?(?:\d{8}_)?(\d{8}_\d{6})(?:_\d+)?$",
    re.IGNORECASE,
)
REMAINING_UIDS_PART_PATTERN = re.compile(r"remaining_uids_part_(\d+)\.csv$", re.IGNORECASE)
FAILED_OWNER_MID_LOG_PATTERN = re.compile(r"\[WARN\]:\s*作者\s+(\d+)\s+抓取失败")
FULL_EXPORT_REQUEST_INTERVAL_SECONDS = 1.5
FULL_EXPORT_REQUEST_JITTER_SECONDS = 1.0
FULL_EXPORT_MAX_RETRIES = 4
FULL_EXPORT_RETRY_BACKOFF_SECONDS = 5.0
FULL_EXPORT_PARTITION_BATCH_SIZE = 10
FULL_EXPORT_PARTITION_BATCH_PAUSE_SECONDS = 10.0
CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS = 1.2
CUSTOM_EXPORT_REQUEST_JITTER_SECONDS = 0.8
CUSTOM_EXPORT_MAX_RETRIES = 4
CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS = 5.0
CUSTOM_EXPORT_BATCH_SIZE = 20
CUSTOM_EXPORT_BATCH_PAUSE_SECONDS = 8.0
DEFAULT_UID_EXPANSION_START_DATE = date(2025, 9, 23)
HOT_400_PAGE_SIZE = 20
HOT_400_MAX_PAGES = 20


@dataclass(slots=True)
class OwnerBatchExportOutcome:
    result: DiscoverResult
    failed_owner_mids: list[int]
    remaining_owner_mids: list[int]
    successful_owner_count: int
    processed_batches: int
    total_batches: int
    stopped_due_to_full_failed_batch: bool
    stopped_batch_index: int | None


@dataclass(slots=True)
class UidExpansionSessionContext:
    root_dir: Path
    session_dir: Path
    logs_dir: Path
    part_number: int
    is_new_session: bool


@dataclass(slots=True)
class RankboardBoard:
    rid: int
    name: str
    slug: str
    url: str


@dataclass(slots=True)
class CustomSeedSelection:
    include_daily_hot: bool
    weekly_weeks: int
    include_column_top: bool
    include_rankboard: bool

    def active_tokens(self) -> list[str]:
        tokens: list[str] = []
        if self.include_daily_hot:
            tokens.append("daily_hot")
        if self.weekly_weeks > 0:
            tokens.append(f"weeklymustsee_{self.weekly_weeks}")
        if self.include_column_top:
            tokens.append("column_top")
        if self.include_rankboard:
            tokens.append("rankboard")
        return tokens


def extract_owner_mids(df: pd.DataFrame, column_name: str) -> tuple[list[int], int]:
    parsed = pd.to_numeric(df[column_name], errors="coerce")
    non_empty_mask = df[column_name].notna() & df[column_name].astype("string").str.strip().ne("")
    invalid_count = int((parsed.isna() & non_empty_mask).sum())
    owner_mids = [int(value) for value in parsed.dropna().tolist()]
    return list(dict.fromkeys(owner_mids)), invalid_count


def extract_bvids(df: pd.DataFrame, column_name: str) -> list[str]:
    values = df[column_name].astype("string").fillna("").str.strip()
    bvids = [value for value in values.tolist() if value]
    return list(dict.fromkeys(bvids))


def decode_uploaded_text(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("无法识别文本编码。")


def extract_failed_owner_mids_from_text(text: str) -> list[int]:
    return [int(match.group(1)) for match in FAILED_OWNER_MID_LOG_PATTERN.finditer(text)]


def chunk_list(values: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        return [values]
    return [values[start : start + chunk_size] for start in range(0, len(values), chunk_size)]


def summarize_exception(exc: Exception, limit: int = 160) -> str:
    summary = " ".join(str(exc).split())
    if not summary:
        return exc.__class__.__name__
    if len(summary) <= limit:
        return summary
    return f"{summary[: limit - 3]}..."


def normalize_output_root(raw_path: str) -> Path:
    stripped = raw_path.strip() if raw_path else ""
    if not stripped:
        return DEFAULT_VIDEO_POOL_OUTPUT_DIR
    path = Path(stripped)
    return path if not path.suffix else path.parent


def owner_mid_dataframe(owner_mids: list[int]) -> pd.DataFrame:
    return pd.DataFrame({"owner_mid": owner_mids})


def save_owner_mid_csv(owner_mids: list[int], path: Path) -> Path:
    return export_dataframe(owner_mid_dataframe(owner_mids), path)


def format_date_token(value: datetime | date) -> str:
    if isinstance(value, datetime):
        value = value.date()
    return value.strftime("%Y%m%d")


def format_timestamp_token(value: datetime) -> str:
    return value.strftime("%Y%m%d_%H%M%S")


def format_datetime_iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def parse_datetime_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_compact_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def coerce_start_datetime(selected_date: date) -> datetime:
    return datetime.combine(selected_date, dt_time.min)


def coerce_end_datetime(selected_date: date, reference_now: datetime | None = None) -> datetime:
    current_time = reference_now or datetime.now()
    if selected_date >= current_time.date():
        return current_time
    return datetime.combine(selected_date, dt_time.max.replace(microsecond=0))


def build_uid_expansion_session_name(requested_start_at: datetime, task_started_at: datetime) -> str:
    return f"uid_expansion_{format_date_token(requested_start_at)}_{format_timestamp_token(task_started_at)}"


def extract_owner_mids_from_rows(rows: list[dict]) -> tuple[list[int], list[str]]:
    owner_mids: list[int] = []
    missing_bvids: list[str] = []
    for row in rows:
        owner_mid = row.get("owner_mid")
        if owner_mid in (None, ""):
            bvid = row.get("bvid")
            if bvid:
                missing_bvids.append(str(bvid))
            continue
        coerced = int(owner_mid)
        if coerced not in owner_mids:
            owner_mids.append(coerced)
    return owner_mids, missing_bvids


def derive_result_pubdate_window(result: DiscoverResult) -> tuple[datetime | None, datetime | None]:
    pubdates = sorted(entry.pubdate for entry in result.entries if entry.pubdate is not None)
    if not pubdates:
        return None, None
    return pubdates[0], pubdates[-1]


def read_owner_mid_csv(path: Path) -> list[int]:
    try:
        df = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return []
    if "owner_mid" not in df.columns:
        return []
    owner_mids, _ = extract_owner_mids(df, "owner_mid")
    return owner_mids


def read_bvid_csv(path: Path) -> list[str]:
    try:
        df = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return []
    if "bvid" not in df.columns:
        return []
    return extract_bvids(df, "bvid")


def path_key(path: Path) -> str:
    return str(path.resolve(strict=False))


def load_uid_expansion_state(session_dir: Path) -> dict:
    state_path = session_dir / UID_EXPANSION_STATE_FILENAME
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_uid_expansion_state(session_dir: Path, state: dict) -> Path:
    state_path = session_dir / UID_EXPANSION_STATE_FILENAME
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state_path


def infer_next_uid_expansion_part(session_dir: Path) -> int:
    highest_part = 0
    state = load_uid_expansion_state(session_dir)
    for part in state.get("parts", []):
        try:
            highest_part = max(highest_part, int(part.get("part_number", 0)))
        except (TypeError, ValueError):
            continue
    for path in session_dir.glob("*_part_*.csv"):
        match = UID_EXPANSION_PART_FILE_PATTERN.search(path.name)
        if match is None:
            continue
        highest_part = max(highest_part, int(match.group(1)))
    return highest_part + 1


def build_uid_expansion_summary_text(state: dict) -> str:
    parts = sorted(state.get("parts", []), key=lambda item: int(item.get("part_number", 0)))
    interruption_count = sum(1 for part in parts if int(part.get("remaining_owner_count", 0)) > 0)
    total_videos = sum(int(part.get("video_count", 0)) for part in parts)
    total_successful_owners = sum(int(part.get("successful_owner_count", 0)) for part in parts)
    lines = [
        "uid_expansion 任务总结",
        f"session_dir: {state.get('session_dir', '')}",
        f"requested_window_start_at: {state.get('requested_window_start_at', '')}",
        f"requested_window_end_at: {state.get('requested_window_end_at', '')}",
        f"effective_window_start_at: {state.get('effective_window_start_at', '')}",
        f"effective_window_end_at: {state.get('effective_window_end_at', '')}",
        f"actual_window_start_at: {state.get('actual_window_start_at', '')}",
        f"actual_window_end_at: {state.get('actual_window_end_at', '')}",
        f"original_uid_count: {state.get('original_uid_count', '')}",
        f"history_reused_owner_count: {state.get('history_reused_owner_count', 0)}",
        f"part_count: {len(parts)}",
        f"interruption_count: {interruption_count}",
        f"total_video_count: {total_videos}",
        f"total_successful_owner_count: {total_successful_owners}",
        "",
        "parts:",
    ]
    for part in parts:
        stop_reason = "full_failed_batch_stop" if part.get("stopped_due_to_full_failed_batch") else "completed_all_batches"
        lines.extend(
            [
                f"- part_{part.get('part_number')}:",
                f"  started_at={part.get('run_started_at', '')}",
                f"  finished_at={part.get('run_finished_at', '')}",
                f"  input_owner_count={part.get('input_owner_count', 0)}",
                f"  successful_owner_count={part.get('successful_owner_count', 0)}",
                f"  failed_owner_count={part.get('failed_owner_count', 0)}",
                f"  remaining_owner_count={part.get('remaining_owner_count', 0)}",
                f"  video_count={part.get('video_count', 0)}",
                f"  effective_window_start_at={part.get('effective_window_start_at', '')}",
                f"  effective_window_end_at={part.get('effective_window_end_at', '')}",
                f"  actual_window_start_at={part.get('actual_window_start_at', '')}",
                f"  actual_window_end_at={part.get('actual_window_end_at', '')}",
                f"  processed_batches={part.get('processed_batches', 0)}/{part.get('total_batches', 0)}",
                f"  stop_reason={stop_reason}",
                f"  stopped_batch_index={part.get('stopped_batch_index', '')}",
                f"  video_file={part.get('video_file', '')}",
                f"  remaining_file={part.get('remaining_file', '')}",
                f"  log_file={part.get('log_file', '')}",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def resolve_uid_expansion_summary_window(state: dict) -> tuple[str, str]:
    parts = sorted(state.get("parts", []), key=lambda item: int(item.get("part_number", 0)))
    started_at = (
        (parts[0].get("run_started_at") if parts else None)
        or state.get("requested_window_start_at")
        or state.get("updated_at")
        or datetime.now().isoformat(timespec="seconds")
    )
    finished_at = state.get("updated_at") or (parts[-1].get("run_finished_at") if parts else None) or started_at
    return str(started_at), str(finished_at)


def write_uid_expansion_summary(session_dir: Path, state: dict) -> Path:
    summary_path = session_dir / UID_EXPANSION_SUMMARY_FILENAME
    started_at, finished_at = resolve_uid_expansion_summary_window(state)
    summary_path.write_text(
        wrap_log_text(build_uid_expansion_summary_text(state), started_at=started_at, finished_at=finished_at),
        encoding="utf-8",
    )
    return summary_path


def find_matching_uid_expansion_session(
    root_dir: Path,
    uploaded_file_names: list[str],
    owner_mids: list[int],
) -> tuple[Path | None, int | None]:
    matched_parts = []
    for file_name in uploaded_file_names:
        match = REMAINING_UIDS_PART_PATTERN.fullmatch(Path(file_name).name)
        if match is not None:
            matched_parts.append(int(match.group(1)))
    if not matched_parts:
        return None, None

    uploaded_part_number = max(matched_parts)
    uid_expansions_root = root_dir / UID_EXPANSION_DIRNAME
    if not uid_expansions_root.exists():
        return None, uploaded_part_number

    candidate_paths = sorted(
        uid_expansions_root.glob(f"*/remaining_uids_part_{uploaded_part_number}.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    fallback_dirs: list[Path] = []
    exact_match_dirs: list[Path] = []
    for candidate_path in candidate_paths:
        fallback_dirs.append(candidate_path.parent)
        candidate_owner_mids = read_owner_mid_csv(candidate_path)
        if candidate_owner_mids == owner_mids:
            exact_match_dirs.append(candidate_path.parent)

    if exact_match_dirs:
        return exact_match_dirs[0], uploaded_part_number
    if fallback_dirs:
        return fallback_dirs[0], uploaded_part_number
    return None, uploaded_part_number


def prepare_uid_expansion_session(
    root_dir: Path,
    uploaded_file_names: list[str],
    owner_mids: list[int],
    requested_start_at: datetime,
    requested_end_at: datetime,
    task_started_at: datetime,
    logger=None,
) -> UidExpansionSessionContext:
    normalized_root_dir = normalize_output_root(str(root_dir))
    session_dir, uploaded_part_number = find_matching_uid_expansion_session(
        normalized_root_dir,
        uploaded_file_names,
        owner_mids,
    )
    if session_dir is not None:
        part_number = max(infer_next_uid_expansion_part(session_dir), (uploaded_part_number or 0) + 1)
        if logger is not None:
            logger(f"[INFO]: 识别到已有 uid_expansion 会话：{display_path(session_dir, APP_DIR)}，本次将保存为 part_{part_number}。")
        return UidExpansionSessionContext(
            root_dir=normalized_root_dir,
            session_dir=session_dir,
            logs_dir=session_dir / "logs",
            part_number=part_number,
            is_new_session=False,
        )

    base_name = build_uid_expansion_session_name(requested_start_at, task_started_at)
    parent_dir = normalized_root_dir / UID_EXPANSION_DIRNAME
    session_dir = parent_dir / base_name
    suffix = 2
    while session_dir.exists():
        session_dir = parent_dir / f"{base_name}_{suffix}"
        suffix += 1
    if logger is not None:
        if uploaded_part_number is not None:
            logger("[WARN]: 上传文件名像历史 remaining_uids_part_n.csv，但未找到匹配会话；已按新任务创建目录。")
        logger(
            "[INFO]: 已创建新的 uid_expansion 会话目录："
            f"{display_path(session_dir, APP_DIR)}（start_date={requested_start_at.date().isoformat()}，"
            f"task_started_at={task_started_at.isoformat(timespec='seconds')}）。"
        )
    return UidExpansionSessionContext(
        root_dir=normalized_root_dir,
        session_dir=session_dir,
        logs_dir=session_dir / "logs",
        part_number=1,
        is_new_session=True,
    )


def resolve_uid_history_task_started_at_from_parts(state: dict) -> datetime | None:
    part_started_candidates = [
        parsed_started_at
        for part in state.get("parts", [])
        if isinstance(part, dict)
        for parsed_started_at in [parse_datetime_iso(part.get("run_started_at")) or parse_compact_datetime(part.get("run_started_at"))]
        if parsed_started_at is not None
    ]
    if not part_started_candidates:
        return None
    return min(part_started_candidates)


def resolve_uid_history_task_started_at_from_session_dir(session_dir: Path | None) -> datetime | None:
    if session_dir is None:
        return None
    match = UID_EXPANSION_SESSION_TIMESTAMP_PATTERN.search(session_dir.name)
    if match is None:
        return None
    return parse_compact_datetime(match.group(1))


def resolve_uid_history_task_started_at(state: dict, session_dir: Path | None = None) -> datetime | None:
    primary_candidates = [
        candidate
        for candidate in (
            parse_datetime_iso(state.get("task_started_at")),
            resolve_uid_history_task_started_at_from_session_dir(session_dir),
            resolve_uid_history_task_started_at_from_parts(state),
        )
        if candidate is not None
    ]
    if primary_candidates:
        return min(primary_candidates)

    fallback_candidates = [
        candidate
        for candidate in (
            parse_datetime_iso(state.get("updated_at")),
            parse_datetime_iso(state.get("requested_window_end_at")),
            parse_datetime_iso(state.get("effective_window_end_at")),
        )
        if candidate is not None
    ]
    return min(fallback_candidates, default=None)


def load_owner_history_task_starts(
    root_dir: Path,
    excluded_session_dirs: list[Path] | None = None,
) -> dict[int, datetime]:
    uid_expansions_root = normalize_output_root(str(root_dir)) / UID_EXPANSION_DIRNAME
    if not uid_expansions_root.exists():
        return {}

    excluded_keys = {path_key(path) for path in (excluded_session_dirs or [])}
    task_starts: dict[int, datetime] = {}
    for session_dir in sorted(uid_expansions_root.iterdir(), key=lambda path: path.stat().st_mtime):
        if not session_dir.is_dir():
            continue
        if path_key(session_dir) in excluded_keys:
            continue
        owner_mids = read_owner_mid_csv(session_dir / UID_EXPANSION_ORIGINAL_UIDS_FILENAME)
        if not owner_mids:
            continue
        task_started_at = resolve_uid_history_task_started_at(load_uid_expansion_state(session_dir), session_dir)
        if task_started_at is None:
            continue
        for owner_mid in owner_mids:
            previous = task_starts.get(owner_mid)
            if previous is None or task_started_at > previous:
                task_starts[owner_mid] = task_started_at
    return task_starts


def build_owner_since_overrides(
    owner_mids: list[int],
    root_dir: Path,
    requested_start_at: datetime,
    requested_end_at: datetime,
    excluded_session_dirs: list[Path] | None = None,
    logger=None,
) -> tuple[dict[int, datetime], int]:
    task_starts = load_owner_history_task_starts(root_dir, excluded_session_dirs=excluded_session_dirs)
    overrides: dict[int, datetime] = {}
    reused_owner_count = 0
    for owner_mid in owner_mids:
        task_started_at = task_starts.get(owner_mid)
        if task_started_at is None:
            continue
        reused_owner_count += 1
        history_start_at = coerce_start_datetime(task_started_at.date())
        overrides[owner_mid] = min(max(requested_start_at, history_start_at), requested_end_at)
    if logger is not None:
        logger(
            f"[INFO]: 历史任务增量检查完成：{reused_owner_count} 个作者命中过往 original_uids，"
            f"{len(owner_mids) - reused_owner_count} 个作者按全局 start_date 抓取；"
            "命中历史的作者将从其最近一次任务开始日（含当天）继续抓取。"
        )
    return overrides, reused_owner_count


def load_existing_uid_expansion_bvids(root_dir: Path) -> set[str]:
    uid_expansions_root = normalize_output_root(str(root_dir)) / UID_EXPANSION_DIRNAME
    if not uid_expansions_root.exists():
        return set()
    existing_bvids: set[str] = set()
    for videolist_path in uid_expansions_root.glob("*/videolist_part_*.csv"):
        existing_bvids.update(read_bvid_csv(videolist_path))
    return existing_bvids


def drop_existing_uid_expansion_duplicates(
    result: DiscoverResult,
    root_dir: Path,
    logger=None,
) -> tuple[DiscoverResult, int]:
    existing_bvids = load_existing_uid_expansion_bvids(root_dir)
    if not existing_bvids:
        return result, 0
    filtered_entries = [entry for entry in result.entries if entry.bvid not in existing_bvids]
    removed_count = len(result.entries) - len(filtered_entries)
    if logger is not None:
        logger(
            f"[INFO]: 全局 videolist 去重检查完成：历史任务中已有 {len(existing_bvids)} 个唯一 bvid，"
            f"本次过滤掉 {removed_count} 条重复视频。"
        )
    return DiscoverResult(entries=filtered_entries, owner_mids=result.owner_mids), removed_count


def record_uid_expansion_part(
    session: UidExpansionSessionContext,
    requested_start_at: datetime,
    requested_end_at: datetime,
    owner_since_overrides: dict[int, datetime],
    reused_owner_count: int,
    input_owner_count: int,
    outcome: OwnerBatchExportOutcome,
    video_path: Path,
    remaining_path: Path | None,
    log_path: Path | None,
    run_started_at: str,
    run_finished_at: str,
) -> dict:
    state = load_uid_expansion_state(session.session_dir)
    actual_start_at, actual_end_at = derive_result_pubdate_window(outcome.result)
    effective_start_candidates = list(owner_since_overrides.values()) or [requested_start_at]
    effective_start_at = min(effective_start_candidates)
    parts = [part for part in state.get("parts", []) if int(part.get("part_number", 0)) != session.part_number]
    parts.append(
        {
            "part_number": session.part_number,
            "run_started_at": run_started_at,
            "run_finished_at": run_finished_at,
            "input_owner_count": input_owner_count,
            "successful_owner_count": outcome.successful_owner_count,
            "failed_owner_count": len(outcome.failed_owner_mids),
            "remaining_owner_count": len(outcome.remaining_owner_mids),
            "video_count": len(outcome.result.entries),
            "processed_batches": outcome.processed_batches,
            "total_batches": outcome.total_batches,
            "stopped_due_to_full_failed_batch": outcome.stopped_due_to_full_failed_batch,
            "stopped_batch_index": outcome.stopped_batch_index,
            "effective_window_start_at": format_datetime_iso(effective_start_at),
            "effective_window_end_at": format_datetime_iso(requested_end_at),
            "actual_window_start_at": format_datetime_iso(actual_start_at),
            "actual_window_end_at": format_datetime_iso(actual_end_at),
            "video_file": video_path.name,
            "remaining_file": remaining_path.name if remaining_path is not None else "",
            "log_file": log_path.relative_to(session.session_dir).as_posix() if log_path is not None else "",
        }
    )
    parts.sort(key=lambda item: int(item.get("part_number", 0)))
    original_uid_count = int(state.get("original_uid_count") or input_owner_count)
    previous_actual_start = parse_datetime_iso(state.get("actual_window_start_at"))
    previous_actual_end = parse_datetime_iso(state.get("actual_window_end_at"))
    task_started_at = state.get("task_started_at") or run_started_at
    state.update(
        {
            "session_dir": display_path(session.session_dir, APP_DIR),
            "task_started_at": task_started_at,
            "requested_window_start_at": format_datetime_iso(requested_start_at),
            "requested_window_end_at": format_datetime_iso(requested_end_at),
            "effective_window_start_at": format_datetime_iso(
                min(effective_start_at, parse_datetime_iso(state.get("effective_window_start_at")) or effective_start_at)
            ),
            "effective_window_end_at": format_datetime_iso(requested_end_at),
            "actual_window_start_at": format_datetime_iso(
                min([value for value in (previous_actual_start, actual_start_at) if value is not None], default=actual_start_at)
            ),
            "actual_window_end_at": format_datetime_iso(
                max([value for value in (previous_actual_end, actual_end_at) if value is not None], default=actual_end_at)
            ),
            "original_uid_count": original_uid_count,
            "history_reused_owner_count": max(int(state.get("history_reused_owner_count") or 0), reused_owner_count),
            "parts": parts,
            "updated_at": run_finished_at,
        }
    )
    save_uid_expansion_state(session.session_dir, state)
    return state


def build_result_from_owner_mids_with_guardrails(
    owner_mids: list[int],
    requested_start_at: datetime,
    requested_end_at: datetime,
    owner_since_overrides: dict[int, datetime] | None = None,
    logger=None,
) -> OwnerBatchExportOutcome:
    normalized_owner_mids = list(dict.fromkeys(int(owner_mid) for owner_mid in owner_mids))
    if not normalized_owner_mids:
        return OwnerBatchExportOutcome(
            result=DiscoverResult(entries=[], owner_mids=[]),
            failed_owner_mids=[],
            remaining_owner_mids=[],
            successful_owner_count=0,
            processed_batches=0,
            total_batches=0,
            stopped_due_to_full_failed_batch=False,
            stopped_batch_index=None,
        )

    builder = VideoPoolBuilder(
        config=DiscoverConfig(start_date=requested_start_at, end_date=requested_end_at),
        hot_sources=[],
        partition_sources=[],
        author_source=BilibiliUserRecentVideoSource(
            page_size=30,
            max_pages=20,
            request_interval_seconds=CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
            request_jitter_seconds=CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
            max_retries=CUSTOM_EXPORT_MAX_RETRIES,
            retry_backoff_seconds=CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
        ),
    )
    owner_since_overrides = owner_since_overrides or {}
    batches = chunk_list(normalized_owner_mids, CUSTOM_EXPORT_BATCH_SIZE)
    merged_entries = {}
    failed_owner_mids: list[int] = []
    successful_owner_count = 0
    processed_batches = 0
    stopped_due_to_full_failed_batch = False
    stopped_batch_index: int | None = None

    for batch_index, batch_owner_mids in enumerate(batches, start=1):
        batch_failed_owner_mids: list[int] = []
        if logger is not None:
            logger(f"[INFO]: 开始处理第 {batch_index}/{len(batches)} 批作者，共 {len(batch_owner_mids)} 个。")

        def _on_batch_error(owner_mid: int, _index: int, _total: int, exc: Exception) -> None:
            batch_failed_owner_mids.append(owner_mid)
            if logger is not None:
                logger(f"[WARN]: 作者 {owner_mid} 抓取失败，已跳过。原因：{summarize_exception(exc)}")

        batch_result = builder.build_from_owner_mids(
            batch_owner_mids,
            owner_since_overrides=owner_since_overrides,
            error_callback=_on_batch_error,
        )
        before_count = len(merged_entries)
        for entry in batch_result.entries:
            merged_entries.setdefault(entry.bvid, entry)

        batch_failed_owner_mid_set = set(batch_failed_owner_mids)
        failed_owner_mids.extend([owner_mid for owner_mid in batch_owner_mids if owner_mid in batch_failed_owner_mid_set])
        successful_owner_count += len(batch_owner_mids) - len(batch_failed_owner_mid_set)
        processed_batches = batch_index
        if logger is not None:
            logger(f"[INFO]: 第 {batch_index}/{len(batches)} 批完成，新增 {len(merged_entries) - before_count} 条视频，累计 {len(merged_entries)} 条。")
            if batch_failed_owner_mids:
                logger(f"[WARN]: 本批有 {len(batch_failed_owner_mids)} 个作者抓取失败。")
        if batch_failed_owner_mid_set and len(batch_failed_owner_mid_set) == len(batch_owner_mids):
            stopped_due_to_full_failed_batch = True
            stopped_batch_index = batch_index
            if logger is not None:
                logger("[WARN]: 检测到当前批次全部作者抓取失败，已提前停止后续批次，并导出剩余作者 UID。")
            break
        if batch_index < len(batches):
            if logger is not None:
                logger(f"[INFO]: 批次间暂停 {int(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)} 秒，降低请求频率。")
            time.sleep(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)

    result = DiscoverResult(entries=sorted(merged_entries.values(), key=lambda item: item.discovered_at), owner_mids=normalized_owner_mids)
    remaining_owner_mids = list(dict.fromkeys(failed_owner_mids))
    if stopped_due_to_full_failed_batch and stopped_batch_index is not None:
        remaining_owner_mids.extend(owner_mid for batch_owner_mids in batches[stopped_batch_index:] for owner_mid in batch_owner_mids)
        remaining_owner_mids = list(dict.fromkeys(remaining_owner_mids))
    return OwnerBatchExportOutcome(
        result=result,
        failed_owner_mids=list(dict.fromkeys(failed_owner_mids)),
        remaining_owner_mids=remaining_owner_mids,
        successful_owner_count=successful_owner_count,
        processed_batches=processed_batches,
        total_batches=len(batches),
        stopped_due_to_full_failed_batch=stopped_due_to_full_failed_batch,
        stopped_batch_index=stopped_batch_index,
    )


def load_full_export_tids() -> list[int]:
    return load_valid_partition_tids(VALID_TAGS_PATH)


def load_rankboard_boards() -> list[RankboardBoard]:
    boards: list[RankboardBoard] = []
    with RANKBOARD_VALID_RIDS_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            boards.append(
                RankboardBoard(
                    rid=int(row["rid"]),
                    name=row["board_name"].strip(),
                    slug=row["board_slug"].strip(),
                    url=row["board_url"].strip(),
                )
            )
    return boards


def build_custom_seed_result(
    selection: CustomSeedSelection,
    valid_tids: list[int],
    logger=None,
) -> DiscoverResult:
    candidates = []
    if selection.include_daily_hot:
        hot_source = BilibiliHotSource(
            ps=HOT_400_PAGE_SIZE,
            fetch_all_pages=True,
            max_pages=HOT_400_MAX_PAGES,
            request_interval_seconds=FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
            request_jitter_seconds=FULL_EXPORT_REQUEST_JITTER_SECONDS,
            max_retries=FULL_EXPORT_MAX_RETRIES,
            retry_backoff_seconds=FULL_EXPORT_RETRY_BACKOFF_SECONDS,
        )
        if logger is not None:
            logger("[INFO]: 正在抓取当天全站热门视频（默认400条）。")
        hot_candidates = hot_source.fetch()
        candidates.extend(hot_candidates)
        if logger is not None:
            logger(f"[INFO]: 热门 400 条抓取完成，新增 {len(hot_candidates)} 条候选视频。")

    if selection.weekly_weeks > 0:
        for week in range(1, selection.weekly_weeks + 1):
            if logger is not None:
                logger(f"[INFO]: 正在抓取第 {week}/{selection.weekly_weeks} 期每周必看。")
            weekly_candidates = BilibiliWeeklyHotSource(
                week=week,
                request_interval_seconds=FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
                request_jitter_seconds=FULL_EXPORT_REQUEST_JITTER_SECONDS,
                max_retries=FULL_EXPORT_MAX_RETRIES,
                retry_backoff_seconds=FULL_EXPORT_RETRY_BACKOFF_SECONDS,
            ).fetch()
            candidates.extend(weekly_candidates)
            if logger is not None:
                logger(f"[INFO]: 第 {week} 期每周必看抓取完成，新增 {len(weekly_candidates)} 条候选视频。")

    if selection.include_column_top:
        tid_batches = chunk_list(valid_tids, FULL_EXPORT_PARTITION_BATCH_SIZE)
        for batch_index, tid_batch in enumerate(tid_batches, start=1):
            if logger is not None and len(tid_batches) > 1:
                logger(f"[INFO]: 开始抓取第 {batch_index}/{len(tid_batches)} 批分区主流视频，共 {len(tid_batch)} 个 tid。")
            for tid in tid_batch:
                zone_candidates = BilibiliZoneTop10Source(tid=tid, day=1).fetch()
                candidates.extend(zone_candidates)
                if logger is not None:
                    logger(f"[INFO]: 分区 tid={tid} 当天主流视频抓取完成，新增 {len(zone_candidates)} 条候选视频。")
            if batch_index < len(tid_batches):
                if logger is not None:
                    logger(f"[INFO]: 分区批次间暂停 {int(FULL_EXPORT_PARTITION_BATCH_PAUSE_SECONDS)} 秒。")
                time.sleep(FULL_EXPORT_PARTITION_BATCH_PAUSE_SECONDS)

    builder = VideoPoolBuilder(
        config=DiscoverConfig(start_date=datetime(2000, 1, 1), end_date=datetime.now(), enable_author_backfill=False),
        hot_sources=[],
        partition_sources=[],
        author_source=BilibiliUserRecentVideoSource(page_size=1, max_pages=1),
    )
    return builder.build_from_seed_candidates(candidates, now=datetime.now())


def build_rankboard_result(boards: list[RankboardBoard], logger=None) -> RankboardResult:
    entries = []
    total = len(boards)
    for index, board in enumerate(boards, start=1):
        if logger is not None:
            logger(f"[INFO]: 正在抓取第 {index}/{total} 个实时排行榜分区：{board.name}（rid={board.rid}）。")
        board_entries = BilibiliRankboardSource(
            board_rid=board.rid,
            board_name=board.name,
            board_url=board.url,
            request_interval_seconds=FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
            request_jitter_seconds=FULL_EXPORT_REQUEST_JITTER_SECONDS,
            max_retries=FULL_EXPORT_MAX_RETRIES,
            retry_backoff_seconds=FULL_EXPORT_RETRY_BACKOFF_SECONDS,
        ).fetch()
        entries.extend(board_entries)
        if logger is not None:
            logger(f"[INFO]: 排行榜 {board.name} 抓取完成，新增 {len(board_entries)} 条榜单记录。")
    return RankboardResult(entries=entries)
