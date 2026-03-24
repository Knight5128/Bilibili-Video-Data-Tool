from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from html import escape
import importlib
from pathlib import Path
import re
import time

import pandas as pd
import streamlit as st

from bili_pipeline.bilibili_zones import find_by_tid, list_zones, unique_mains
from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import (
    BilibiliUserRecentVideoSource,
    VideoPoolBuilder,
    load_valid_partition_tids,
    resolve_owner_mids_from_bvids,
)
from bili_pipeline.discover.export_csv import discover_entries_to_rows, export_discover_result_csv
from bili_pipeline.discover.real_demo import build_real_result
from bili_pipeline.models import DiscoverResult
from bili_pipeline.utils.file_merge import (
    build_deduplicated_output_path,
    deduplicate_dataframe,
    export_dataframe,
    merge_dataframes,
    parse_comma_separated_keys,
    read_uploaded_dataframe,
    resolve_output_path,
    validate_columns,
)
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvp-builder.png"
VALID_TAGS_PATH = APP_DIR / "all_valid_tags.csv"
LOGS_DIR = APP_DIR / "logs"
DEFAULT_VIDEO_POOL_OUTPUT_DIR = Path("outputs") / "video_pool"
BVID_TO_UIDS_OUTPUT_DIR = DEFAULT_VIDEO_POOL_OUTPUT_DIR / "bvid_to_uids"
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
FAILED_OWNER_MID_LOG_PATTERN = re.compile(r"\[WARN\]:\s*作者\s+(\d+)\s+抓取失败")
REMAINING_UIDS_PART_PATTERN = re.compile(r"remaining_uids_part_(\d+)\.csv$", re.IGNORECASE)
UID_EXPANSION_PART_FILE_PATTERN = re.compile(
    r"(?:videolist|remaining_uids)_part_(\d+)\.csv$",
    re.IGNORECASE,
)
UID_EXPANSION_DIRNAME = "uid_expansions"
UID_EXPANSION_STATE_FILENAME = "uid_expansion_state.json"
UID_EXPANSION_SUMMARY_FILENAME = "uid_expansion_summary.log"
UID_EXPANSION_ORIGINAL_UIDS_FILENAME = "original_uids.csv"


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


def _build_logo_data_uri(logo_path: Path) -> str | None:
    if not logo_path.exists():
        return None
    mime_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }
    mime_type = mime_types.get(logo_path.suffix.lower())
    if mime_type is None:
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _render_centered_header(title: str, logo_path: Path) -> None:
    safe_title = escape(title)
    logo_uri = _build_logo_data_uri(logo_path)
    if logo_uri is None:
        st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.25rem;'>{safe_title}</h1>", unsafe_allow_html=True)
        return
    st.markdown(
        f"""
        <div style="display: flex; justify-content: center; align-items: center; gap: 0.75rem; margin-bottom: 0.25rem;">
            <img src="{logo_uri}" alt="{safe_title} logo" style="height: 3.5rem; width: 3.5rem; object-fit: contain;" />
            <h1 style="margin: 0;">{safe_title}</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _preview_discover_result(result, limit: int = 200) -> None:
    preview = pd.DataFrame(discover_entries_to_rows(result.entries)).head(limit)
    if preview.empty:
        st.info("本次结果为空。")
        return
    st.dataframe(preview, width="stretch", hide_index=True)


def _read_uploaded_files(uploaded_files) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    for uploaded_file in uploaded_files:
        try:
            dfs.append(read_uploaded_dataframe(uploaded_file))
        except Exception as e:  # noqa: BLE001
            st.error(f"读取文件失败：{uploaded_file.name}（{e}）")
            return []
    return dfs


def _merge_and_deduplicate_by_column(
    uploaded_files,
    dfs: list[pd.DataFrame],
    column_name: str,
    label: str,
) -> pd.DataFrame | None:
    normalized_column = column_name.strip()
    if not normalized_column:
        st.error(f"请填写{label}所在的列名。")
        return None

    for idx, df in enumerate(dfs):
        try:
            validate_columns(df, [normalized_column], label)
        except ValueError as e:
            st.error(f"第 {idx + 1} 个文件（{uploaded_files[idx].name}）校验失败：{e}")
            return None

    merged = pd.concat(dfs, ignore_index=True)
    deduplicated, _ = deduplicate_dataframe(merged, dedupe_keys=[normalized_column])
    return deduplicated.reset_index(drop=True)


def _extract_owner_mids(df: pd.DataFrame, column_name: str) -> tuple[list[int], int]:
    parsed = pd.to_numeric(df[column_name], errors="coerce")
    non_empty_mask = df[column_name].notna() & df[column_name].astype("string").str.strip().ne("")
    invalid_count = int((parsed.isna() & non_empty_mask).sum())
    owner_mids = [int(value) for value in parsed.dropna().tolist()]
    return list(dict.fromkeys(owner_mids)), invalid_count


def _extract_bvids(df: pd.DataFrame, column_name: str) -> list[str]:
    values = df[column_name].astype("string").fillna("").str.strip()
    bvids = [value for value in values.tolist() if value]
    return list(dict.fromkeys(bvids))


def _decode_uploaded_text_file(uploaded_file) -> str:
    raw = uploaded_file.getvalue()
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"无法识别文本编码：{uploaded_file.name}")


def _read_uploaded_text_files(uploaded_files) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for uploaded_file in uploaded_files:
        try:
            texts.append((uploaded_file.name, _decode_uploaded_text_file(uploaded_file)))
        except Exception as e:  # noqa: BLE001
            st.error(f"读取日志文件失败：{uploaded_file.name}（{e}）")
            return []
    return texts


def _extract_failed_owner_mids_from_text(text: str) -> list[int]:
    return [int(match.group(1)) for match in FAILED_OWNER_MID_LOG_PATTERN.finditer(text)]


def _chunk_list(values: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        return [values]
    return [values[start : start + chunk_size] for start in range(0, len(values), chunk_size)]


def _summarize_exception(exc: Exception, limit: int = 160) -> str:
    summary = " ".join(str(exc).split())
    if not summary:
        return exc.__class__.__name__
    if len(summary) <= limit:
        return summary
    return f"{summary[: limit - 3]}..."


def _default_output_path(filename: str) -> Path:
    return DEFAULT_VIDEO_POOL_OUTPUT_DIR / filename


def _normalize_output_root(raw_path: str) -> Path:
    stripped = raw_path.strip() if raw_path else ""
    if not stripped:
        return DEFAULT_VIDEO_POOL_OUTPUT_DIR
    path = Path(stripped)
    return path if not path.suffix else path.parent


def _owner_mid_dataframe(owner_mids: list[int]) -> pd.DataFrame:
    return pd.DataFrame({"owner_mid": owner_mids})


def _save_owner_mid_csv(owner_mids: list[int], path: Path) -> Path:
    return export_dataframe(_owner_mid_dataframe(owner_mids), path)


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(APP_DIR).as_posix()
    except ValueError:
        return path.as_posix()


def _load_uid_expansion_state(session_dir: Path) -> dict:
    state_path = session_dir / UID_EXPANSION_STATE_FILENAME
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_uid_expansion_state(session_dir: Path, state: dict) -> Path:
    state_path = session_dir / UID_EXPANSION_STATE_FILENAME
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return state_path


def _infer_next_uid_expansion_part(session_dir: Path) -> int:
    highest_part = 0
    state = _load_uid_expansion_state(session_dir)
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


def _build_uid_expansion_summary_text(state: dict) -> str:
    parts = sorted(
        state.get("parts", []),
        key=lambda item: int(item.get("part_number", 0)),
    )
    interruption_count = sum(1 for part in parts if int(part.get("remaining_owner_count", 0)) > 0)
    total_videos = sum(int(part.get("video_count", 0)) for part in parts)
    total_successful_owners = sum(int(part.get("successful_owner_count", 0)) for part in parts)
    lines = [
        "uid_expansion 任务总结",
        f"session_dir: {state.get('session_dir', '')}",
        f"lookback_days: {state.get('lookback_days', '')}",
        f"original_uid_count: {state.get('original_uid_count', '')}",
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
                f"  processed_batches={part.get('processed_batches', 0)}/{part.get('total_batches', 0)}",
                f"  stop_reason={stop_reason}",
                f"  stopped_batch_index={part.get('stopped_batch_index', '')}",
                f"  video_file={part.get('video_file', '')}",
                f"  remaining_file={part.get('remaining_file', '')}",
                f"  log_file={part.get('log_file', '')}",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _write_uid_expansion_summary(session_dir: Path, state: dict) -> Path:
    summary_path = session_dir / UID_EXPANSION_SUMMARY_FILENAME
    summary_path.write_text(_build_uid_expansion_summary_text(state), encoding="utf-8")
    return summary_path


def _find_matching_uid_expansion_session(
    root_dir: Path,
    uploaded_file_names: list[str],
    owner_mids: list[int],
    lookback_days: int,
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
        uid_expansions_root.glob(
            f"uid_expansion_{lookback_days}_days_*/remaining_uids_part_{uploaded_part_number}.csv"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    fallback_dirs: list[Path] = []
    exact_match_dirs: list[Path] = []
    for candidate_path in candidate_paths:
        fallback_dirs.append(candidate_path.parent)
        try:
            candidate_df = pd.read_csv(candidate_path)
        except Exception:  # noqa: BLE001
            continue
        if "owner_mid" not in candidate_df.columns:
            continue
        candidate_owner_mids, _ = _extract_owner_mids(candidate_df, "owner_mid")
        if candidate_owner_mids == owner_mids:
            exact_match_dirs.append(candidate_path.parent)

    if exact_match_dirs:
        return exact_match_dirs[0], uploaded_part_number
    if fallback_dirs:
        return fallback_dirs[0], uploaded_part_number
    return None, uploaded_part_number


def _prepare_uid_expansion_session(
    root_dir: Path,
    uploaded_file_names: list[str],
    owner_mids: list[int],
    lookback_days: int,
    logger=None,
) -> UidExpansionSessionContext:
    normalized_root_dir = _normalize_output_root(str(root_dir))
    session_dir, uploaded_part_number = _find_matching_uid_expansion_session(
        normalized_root_dir,
        uploaded_file_names,
        owner_mids,
        lookback_days,
    )
    if session_dir is not None:
        part_number = max(_infer_next_uid_expansion_part(session_dir), (uploaded_part_number or 0) + 1)
        if logger is not None:
            logger(
                f"[INFO]: 识别到已有 uid_expansion 会话：{_display_path(session_dir)}，本次将保存为 part_{part_number}。"
            )
        return UidExpansionSessionContext(
            root_dir=normalized_root_dir,
            session_dir=session_dir,
            logs_dir=session_dir / "logs",
            part_number=part_number,
            is_new_session=False,
        )

    session_dir = (
        normalized_root_dir
        / UID_EXPANSION_DIRNAME
        / f"uid_expansion_{lookback_days}_days_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    if logger is not None:
        if uploaded_part_number is not None:
            logger(
                "[WARN]: 上传文件名看起来像历史 remaining_uids_part_n.csv，但未找到匹配会话；"
                "已按新的 uid_expansion 任务创建目录。"
            )
        logger(f"[INFO]: 已创建新的 uid_expansion 会话目录：{_display_path(session_dir)}。")
    return UidExpansionSessionContext(
        root_dir=normalized_root_dir,
        session_dir=session_dir,
        logs_dir=session_dir / "logs",
        part_number=1,
        is_new_session=True,
    )


def _record_uid_expansion_part(
    session: UidExpansionSessionContext,
    lookback_days: int,
    input_owner_count: int,
    outcome: OwnerBatchExportOutcome,
    video_path: Path,
    remaining_path: Path | None,
    log_path: Path | None,
    run_started_at: str,
    run_finished_at: str,
) -> dict:
    state = _load_uid_expansion_state(session.session_dir)
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
            "video_file": video_path.name,
            "remaining_file": remaining_path.name if remaining_path is not None else "",
            "log_file": (
                log_path.relative_to(session.session_dir).as_posix()
                if log_path is not None
                else ""
            ),
        }
    )
    parts.sort(key=lambda item: int(item.get("part_number", 0)))
    original_uid_count = int(state.get("original_uid_count") or input_owner_count)
    state.update(
        {
            "session_dir": _display_path(session.session_dir),
            "lookback_days": int(lookback_days),
            "original_uid_count": original_uid_count,
            "parts": parts,
            "updated_at": run_finished_at,
        }
    )
    _save_uid_expansion_state(session.session_dir, state)
    return state


def _build_result_from_owner_mids_with_guardrails(
    owner_mids: list[int],
    lookback_days: int,
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
        config=DiscoverConfig(lookback_days=lookback_days),
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
    batches = _chunk_list(normalized_owner_mids, CUSTOM_EXPORT_BATCH_SIZE)
    merged_entries = {}
    failed_owner_mids: list[int] = []
    successful_owner_count = 0
    processed_batches = 0
    stopped_due_to_full_failed_batch = False
    stopped_batch_index: int | None = None

    for batch_index, batch_owner_mids in enumerate(batches, start=1):
        batch_failed_owner_mids: list[int] = []
        if logger is not None:
            logger(
                f"[INFO]: 开始处理第 {batch_index}/{len(batches)} 批作者，共 {len(batch_owner_mids)} 个。"
            )

        def _on_batch_error(owner_mid: int, _index: int, _total: int, exc: Exception) -> None:
            batch_failed_owner_mids.append(owner_mid)
            if logger is not None:
                logger(f"[WARN]: 作者 {owner_mid} 抓取失败，已跳过。原因：{_summarize_exception(exc)}")

        batch_result = builder.build_from_owner_mids(
            batch_owner_mids,
            error_callback=_on_batch_error,
        )
        before_count = len(merged_entries)
        for entry in batch_result.entries:
            merged_entries.setdefault(entry.bvid, entry)

        batch_failed_owner_mid_set = set(batch_failed_owner_mids)
        failed_owner_mids.extend(
            [owner_mid for owner_mid in batch_owner_mids if owner_mid in batch_failed_owner_mid_set]
        )
        successful_owner_count += len(batch_owner_mids) - len(batch_failed_owner_mid_set)
        processed_batches = batch_index
        if logger is not None:
            logger(
                f"[INFO]: 第 {batch_index}/{len(batches)} 批完成，新增 {len(merged_entries) - before_count} 条视频，累计 {len(merged_entries)} 条。"
            )
            if batch_failed_owner_mids:
                logger(f"[WARN]: 本批有 {len(batch_failed_owner_mids)} 个作者抓取失败。")
        if batch_failed_owner_mid_set and len(batch_failed_owner_mid_set) == len(batch_owner_mids):
            stopped_due_to_full_failed_batch = True
            stopped_batch_index = batch_index
            if logger is not None:
                logger(
                    "[WARN]: 检测到当前批次全部作者抓取失败，已提前停止后续批次，并将输出本次已抓取视频列表及剩余作者 UID。"
                )
            break
        if batch_index < len(batches):
            if logger is not None:
                logger(f"[INFO]: 批次间暂停 {int(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)} 秒，降低请求频率。")
            time.sleep(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)

    result = DiscoverResult(
        entries=sorted(merged_entries.values(), key=lambda item: item.discovered_at),
        owner_mids=normalized_owner_mids,
    )
    remaining_owner_mids = list(dict.fromkeys(failed_owner_mids))
    if stopped_due_to_full_failed_batch and stopped_batch_index is not None:
        remaining_owner_mids.extend(
            owner_mid
            for batch_owner_mids in batches[stopped_batch_index:]
            for owner_mid in batch_owner_mids
        )
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


def _append_log(logs: list[str], placeholder, message: str) -> None:
    logs.append(message)
    placeholder.code("\n".join(logs), language=None)


def _save_task_logs(task_name: str, logs: list[str], *, log_dir: Path | None = None) -> Path | None:
    if not logs:
        return None
    target_log_dir = log_dir or LOGS_DIR
    target_log_dir.mkdir(parents=True, exist_ok=True)
    safe_task_name = re.sub(r"[^A-Za-z0-9._-]+", "_", task_name).strip("_") or "task"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_path = target_log_dir / f"{timestamp}_{safe_task_name}.log"
    content = "\n".join(logs).strip()
    log_path.write_text(content + "\n", encoding="utf-8")
    return log_path


def _show_saved_log_path(log_path: Path | None) -> None:
    if log_path is None:
        return
    try:
        display_path = log_path.relative_to(APP_DIR)
    except ValueError:
        display_path = log_path
    st.caption(f"运行日志已保存：`{display_path.as_posix()}`")


def _load_full_export_tids() -> list[int]:
    return load_valid_partition_tids(VALID_TAGS_PATH)


def _build_full_site_result_with_latest_impl(**kwargs) -> DiscoverResult:
    # Streamlit reruns may keep imported package modules cached; reload here so
    # the full-export path always picks up the latest throttling implementation.
    bilibili_sources_module = importlib.import_module("bili_pipeline.discover.bilibili_sources")
    importlib.reload(bilibili_sources_module)
    full_site_module = importlib.import_module("bili_pipeline.discover.full_site")
    importlib.reload(full_site_module)
    return full_site_module.build_full_site_result(**kwargs)


page_config = {"page_title": "Bilibili Video Pool Builder", "layout": "centered"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
render_night_sky_background()
_render_centered_header("Bilibili Video Pool Builder", LOGO_PATH)

tab_full_export, tab_export, tab_tid, tab_custom_export, tab_merge, tab_quick_jump = st.tabs(
    ["全量导出视频列表", "按分区导出视频列表", "tid与分区名称对应", "自定义导出视频列表", "文件拼接及去重", "快捷跳转"]
)

with tab_full_export:
    st.subheader("全量导出视频列表")
    st.caption(
        "一键汇总全站热门榜单、过去若干周的每周必看，以及 `all_valid_tags.csv` 中全部有效分区在 "
        "`lookback_days` 内的投稿视频，并自动按 BVID 去重导出。当前已启用更保守的请求间隔、"
        "重试退避与分批冷却，以降低长时间全量抓取时的 404 / 风控触发概率。"
    )

    valid_tid_count = None
    try:
        valid_tid_count = len(_load_full_export_tids())
    except Exception as e:  # noqa: BLE001
        st.error(f"读取有效分区列表失败：{e}")

    with st.form("full_export_params"):
        full_lookback_days = st.number_input("lookback_days", min_value=1, max_value=3650, value=90, step=1)
        default_full_name = f"full_site_{int(full_lookback_days)}_days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_out_path = st.text_input("输出 CSV 文件路径", value=str(_default_output_path(default_full_name)))
        full_submitted = st.form_submit_button("开始全量抓取并导出")

    if valid_tid_count is not None:
        weeks_to_fetch = int(full_lookback_days) // 7 + 1
        st.caption(
            f"将抓取：1 份全站热门榜单 + 过去 {weeks_to_fetch} 周每周必看 + {valid_tid_count} 个有效分区的近期投稿。"
        )

    full_log_placeholder = st.empty()
    if full_submitted:
        full_logs: list[str] = []
        try:
            valid_tids = _load_full_export_tids()
        except Exception as e:  # noqa: BLE001
            st.error(f"读取有效分区列表失败：{e}")
        else:
            def _log(message: str) -> None:
                _append_log(full_logs, full_log_placeholder, message)

            _log(f"[INFO]: 开始执行全量抓取任务，lookback_days={int(full_lookback_days)}。")
            try:
                with st.spinner("正在抓取全站视频并构建 video_pool..."):
                    result = _build_full_site_result_with_latest_impl(
                        lookback_days=int(full_lookback_days),
                        valid_tids=valid_tids,
                        logger=_log,
                        request_interval_seconds=FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
                        request_jitter_seconds=FULL_EXPORT_REQUEST_JITTER_SECONDS,
                        max_retries=FULL_EXPORT_MAX_RETRIES,
                        retry_backoff_seconds=FULL_EXPORT_RETRY_BACKOFF_SECONDS,
                        partition_batch_size=FULL_EXPORT_PARTITION_BATCH_SIZE,
                        partition_batch_pause_seconds=FULL_EXPORT_PARTITION_BATCH_PAUSE_SECONDS,
                    )
                    saved = export_discover_result_csv(result, full_out_path)
                _log(f"[INFO]: 导出完成：{saved}")
            except Exception as e:  # noqa: BLE001
                _log(f"[ERROR]: 全量抓取任务失败：{_summarize_exception(e)}")
                st.error(f"全量抓取失败：{e}")
            else:
                st.success(f"已导出：{saved}")
                st.caption(f"共 {len(result.entries)} 条 entries（展示前 200 条预览）")
                _preview_discover_result(result)
            finally:
                _show_saved_log_path(_save_task_logs("full_export", full_logs))

with tab_export:
    with st.form("params"):
        tid = st.number_input("tid", min_value=1, max_value=999999, value=17, step=1)
        lookback_days = st.number_input("lookback_days", min_value=1, max_value=3650, value=90, step=1)
        default_name = f"tid{int(tid)}_{int(lookback_days)}_days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        out_path = st.text_input("输出 CSV 文件路径", value=str(_default_output_path(default_name)))
        submitted = st.form_submit_button("开始构建并导出")

    export_log_placeholder = st.empty()
    if submitted:
        export_logs: list[str] = []

        def _export_log(message: str) -> None:
            _append_log(export_logs, export_log_placeholder, message)

        _export_log(f"[INFO]: 开始执行分区抓取任务，tid={int(tid)}，lookback_days={int(lookback_days)}。")
        try:
            with st.spinner("正在拉取数据并构建 video_pool..."):
                result = build_real_result(
                    tid=int(tid),
                    lookback_days=int(lookback_days),
                    logger=_export_log,
                )
                saved = export_discover_result_csv(result, out_path)
            _export_log(f"[INFO]: 导出完成：{saved}")
        except Exception as e:  # noqa: BLE001
            _export_log(f"[ERROR]: 分区抓取任务失败：{_summarize_exception(e)}")
            st.error(f"分区抓取失败：{e}")
        else:
            st.success(f"已导出：{saved}")
            st.caption(f"共 {len(result.entries)} 条 entries（展示前 200 条预览）")
            _preview_discover_result(result)
        finally:
            _show_saved_log_path(_save_task_logs(f"tid_{int(tid)}_export", export_logs))

with tab_tid:
    st.subheader("tid 与分区名称对应")
    st.caption("数据来源：bilibili-API-collect 的「视频分区一览」页面（已固化到本项目，便于离线查阅）。")

    col_a, col_b = st.columns([1, 2])
    with col_a:
        q_tid = st.number_input("按 tid 精确查询", min_value=1, max_value=999999, value=17, step=1)
    with col_b:
        q_text = st.text_input("关键字搜索（主分区/名称/代号）", value="")

    matches = find_by_tid(int(q_tid))
    if matches:
        st.write("精确查询结果")
        st.dataframe(
            [
                {"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route}
                for z in matches
            ],
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("未找到该 tid 的记录（可能是该页面未覆盖或已变更）。")

    zones = list(list_zones())
    mains = ["全部"] + unique_mains(zones)
    sel_main = st.selectbox("按主分区筛选", options=mains, index=0)

    def _hit(z) -> bool:
        if sel_main != "全部" and z.main != sel_main:
            return False
        if not q_text.strip():
            return True
        t = q_text.strip().lower()
        return t in z.main.lower() or t in z.name.lower() or t in z.code.lower()

    filtered = [z for z in zones if _hit(z)]
    st.write(f"共 {len(filtered)} 条（可在表格右上角下载）")
    st.dataframe(
        [{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in filtered],
        width="stretch",
        hide_index=True,
    )


with tab_custom_export:
    st.subheader("自定义导出视频列表")
    st.caption(
        "支持按 BVID 列表回查作者 ID、导出作者一段时间内上传视频列表，或直接从抓取日志中提取失败作者 UID。"
        " 当前已内置保守限速、指数退避重试与分批冷却，以降低长批量导出时的风控触发概率。"
    )

    tab_custom_bvid, tab_custom_owner, tab_failed_uid_log = st.tabs(
        ["BVID回查作者ID➡️", "导出作者一段时间内上传视频列表🔁", "从日志提取失败作者UID"]
    )

    with tab_custom_owner:
        owner_files = st.file_uploader(
            "上传保存作者 ID 的 CSV/XLSX 文件（可多选）",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="custom_owner_files",
        )
        owner_column = st.text_input(
            "作者 ID 所在列名",
            value="owner_mid",
            key="custom_owner_column",
        )
        owner_lookback_days = st.number_input(
            "lookback_days",
            min_value=1,
            max_value=3650,
            value=90,
            step=1,
            key="custom_owner_lookback_days",
        )
        owner_out_path = st.text_input(
            "输出根目录（将自动在其下创建 uid_expansions/...）",
            value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR),
            key="custom_owner_out_path",
        )
        owner_log_placeholder = st.empty()

        if st.button("开始导出作者一段时间内上传视频列表", key="custom_owner_submit"):
            owner_logs: list[str] = []

            def _owner_log(message: str) -> None:
                _append_log(owner_logs, owner_log_placeholder, message)

            if not owner_files:
                st.warning("请先上传至少一个 CSV/XLSX 文件。")
            else:
                owner_dfs = _read_uploaded_files(owner_files)
                prepared_owner_df = None
                if owner_dfs:
                    prepared_owner_df = _merge_and_deduplicate_by_column(
                        owner_files,
                        owner_dfs,
                        owner_column,
                        "作者 ID 列",
                    )

                if prepared_owner_df is not None:
                    owner_mids, invalid_count = _extract_owner_mids(prepared_owner_df, owner_column.strip())
                    if invalid_count:
                        st.warning(f"检测到 {invalid_count} 条无法解析为整数的作者 ID，已自动忽略。")

                    if not owner_mids:
                        st.warning("未从上传文件中解析出有效的作者 ID。")
                    else:
                        output_root = _normalize_output_root(owner_out_path)
                        session = _prepare_uid_expansion_session(
                            output_root,
                            [uploaded_file.name for uploaded_file in owner_files],
                            owner_mids,
                            int(owner_lookback_days),
                            logger=_owner_log,
                        )
                        run_started_at = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved: Path | None = None
                        remaining_saved: Path | None = None
                        summary_saved: Path | None = None
                        outcome: OwnerBatchExportOutcome | None = None

                        _owner_log(f"[INFO]: 去重后共有 {len(owner_mids)} 个作者待抓取。")
                        _owner_log(
                            f"[INFO]: 开始执行作者批量抓取任务，lookback_days={int(owner_lookback_days)}。"
                        )
                        _owner_log(
                            f"[INFO]: 本次 uid_expansion 会话目录：{_display_path(session.session_dir)}，当前 part_{session.part_number}。"
                        )
                        if session.is_new_session:
                            original_uids_path = session.session_dir / UID_EXPANSION_ORIGINAL_UIDS_FILENAME
                            _save_owner_mid_csv(owner_mids, original_uids_path)
                            _owner_log(f"[INFO]: 已保存首轮原始作者 UID 列表：{original_uids_path}")
                        try:
                            with st.spinner("正在导出作者一段时间内上传视频列表..."):
                                outcome = _build_result_from_owner_mids_with_guardrails(
                                    owner_mids,
                                    int(owner_lookback_days),
                                    logger=_owner_log,
                                )
                                video_output_path = session.session_dir / f"videolist_part_{session.part_number}.csv"
                                saved = export_discover_result_csv(outcome.result, video_output_path)
                                _owner_log(f"[INFO]: 视频列表已导出：{saved}")
                                if outcome.remaining_owner_mids:
                                    remaining_output_path = (
                                        session.session_dir / f"remaining_uids_part_{session.part_number}.csv"
                                    )
                                    remaining_saved = _save_owner_mid_csv(
                                        outcome.remaining_owner_mids,
                                        remaining_output_path,
                                    )
                                    _owner_log(f"[INFO]: 剩余作者 UID 已导出：{remaining_saved}")
                            _owner_log(f"[INFO]: 导出完成：{saved}")
                        except Exception as e:  # noqa: BLE001
                            _owner_log(f"[ERROR]: 作者批量抓取任务失败：{_summarize_exception(e)}")
                            st.error(f"导出作者一段时间内上传视频列表失败：{e}")
                        else:
                            if outcome is not None and outcome.failed_owner_mids:
                                preview_failed_owner_mids = ", ".join(
                                    str(owner_mid) for owner_mid in outcome.failed_owner_mids[:10]
                                )
                                if len(outcome.failed_owner_mids) > 10:
                                    preview_failed_owner_mids += " ..."
                                st.warning(
                                    f"本次共有 {len(outcome.failed_owner_mids)} 个作者抓取失败：{preview_failed_owner_mids}"
                                )
                            if outcome is not None and outcome.remaining_owner_mids:
                                if outcome.stopped_due_to_full_failed_batch:
                                    st.warning(
                                        "检测到某一批作者全部抓取失败，程序已提前停止，并导出了当前已抓取视频列表与剩余作者 UID。"
                                    )
                                else:
                                    st.warning("本次已跑完全部批次，但仍有失败作者，已导出 remaining_uids_part 文件供继续重试。")
                                if remaining_saved is not None:
                                    st.caption(f"剩余作者 UID：`{_display_path(remaining_saved)}`")
                            else:
                                st.success(f"已导出：{saved}")
                            st.caption(
                                f"输入作者数 {len(owner_mids)}，导出视频数 {len(outcome.result.entries) if outcome is not None else 0}"
                                "（展示前 200 条预览）"
                            )
                            if outcome is not None:
                                _preview_discover_result(outcome.result)
                        finally:
                            log_path = _save_task_logs(
                                f"uid_expansion_part_{session.part_number}",
                                owner_logs,
                                log_dir=session.logs_dir,
                            )
                            _show_saved_log_path(log_path)
                            if outcome is not None and saved is not None:
                                state = _record_uid_expansion_part(
                                    session,
                                    int(owner_lookback_days),
                                    len(owner_mids),
                                    outcome,
                                    saved,
                                    remaining_saved,
                                    log_path,
                                    run_started_at,
                                    datetime.now().strftime("%Y%m%d_%H%M%S"),
                                )
                                if not outcome.remaining_owner_mids:
                                    summary_saved = _write_uid_expansion_summary(session.session_dir, state)
                                    st.caption(f"任务总结已保存：`{_display_path(summary_saved)}`")

    with tab_custom_bvid:
        bvid_files = st.file_uploader(
            "上传保存 BVID 的 CSV/XLSX 文件（可多选）",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="custom_bvid_files",
        )
        bvid_column = st.text_input(
            "BVID 所在列名",
            value="bvid",
            key="custom_bvid_column",
        )
        default_bvid_name = (
            f"bvid_to_uids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        bvid_out_path = st.text_input(
            "输出 CSV 文件路径",
            value=str(BVID_TO_UIDS_OUTPUT_DIR / default_bvid_name),
            key="custom_bvid_out_path",
        )
        bvid_log_placeholder = st.empty()

        if st.button("开始 BVID 回查作者 ID 并导出 owner_mid 列表", key="custom_bvid_submit"):
            bvid_logs: list[str] = []

            def _bvid_log(message: str) -> None:
                _append_log(bvid_logs, bvid_log_placeholder, message)

            if not bvid_files:
                st.warning("请先上传至少一个 CSV/XLSX 文件。")
            else:
                bvid_dfs = _read_uploaded_files(bvid_files)
                prepared_bvid_df = None
                if bvid_dfs:
                    prepared_bvid_df = _merge_and_deduplicate_by_column(
                        bvid_files,
                        bvid_dfs,
                        bvid_column,
                        "BVID 列",
                    )

                if prepared_bvid_df is not None:
                    bvids = _extract_bvids(prepared_bvid_df, bvid_column.strip())
                    if not bvids:
                        st.warning("未从上传文件中解析出有效的 BVID。")
                    else:
                        _bvid_log(f"[INFO]: 去重后共有 {len(bvids)} 个 BVID 待反查作者。")
                        _bvid_log("[INFO]: 开始执行 BVID 回查作者 ID 任务。")

                        def _on_bvid_progress(_bvid: str, index: int, total: int, _owner_mid: int | None) -> None:
                            if index == 1 or index == total or index % 20 == 0:
                                _bvid_log(f"[INFO]: BVID 反查进度 {index}/{total}。")

                        def _on_bvid_error(bvid: str, index: int, total: int, exc: Exception) -> None:
                            _bvid_log(
                                f"[WARN]: BVID 反查失败 {index}/{total}：{bvid}。原因：{_summarize_exception(exc)}"
                            )

                        try:
                            with st.spinner("正在根据 BVID 回查作者 ID 并导出 owner_mid 列表..."):
                                owner_mids, failed_bvids = resolve_owner_mids_from_bvids(
                                    bvids,
                                    request_interval_seconds=CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
                                    request_jitter_seconds=CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
                                    max_retries=CUSTOM_EXPORT_MAX_RETRIES,
                                    retry_backoff_seconds=CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
                                    progress_callback=_on_bvid_progress,
                                    error_callback=_on_bvid_error,
                                )

                                if owner_mids:
                                    _bvid_log(f"[INFO]: 已解析出 {len(owner_mids)} 个唯一作者，准备导出 owner_mid 列表。")
                                    saved = _save_owner_mid_csv(owner_mids, Path(bvid_out_path))
                                else:
                                    saved = None
                            if saved is not None:
                                _bvid_log(f"[INFO]: 导出完成：{saved}")
                            else:
                                _bvid_log("[WARN]: 未解析出有效作者，因此未生成导出文件。")
                        except Exception as e:  # noqa: BLE001
                            _bvid_log(f"[ERROR]: BVID 反查作者抓取任务失败：{_summarize_exception(e)}")
                            st.error(f"BVID 回查作者 ID 失败：{e}")
                        else:
                            if failed_bvids:
                                preview_failed = ", ".join(failed_bvids[:10])
                                if len(failed_bvids) > 10:
                                    preview_failed += " ..."
                                st.warning(f"有 {len(failed_bvids)} 个 BVID 未能解析出作者：{preview_failed}")
                            if not owner_mids or saved is None:
                                st.warning("未能根据上传的 BVID 解析出有效作者，因此没有导出结果。")
                            else:
                                st.success(f"已导出：{saved}")
                                st.caption(
                                    f"输入 BVID 数 {len(bvids)}，解析出唯一作者数 {len(owner_mids)}（展示前 200 条预览）"
                                )
                                st.dataframe(_owner_mid_dataframe(owner_mids).head(200), width="stretch", hide_index=True)
                        finally:
                            _show_saved_log_path(_save_task_logs("bvid_to_uids_export", bvid_logs))

    with tab_failed_uid_log:
        log_files = st.file_uploader(
            "上传抓取日志文件（支持 .log / .txt，可多选）",
            type=["log", "txt"],
            accept_multiple_files=True,
            key="failed_owner_uid_log_files",
        )
        log_text = st.text_area(
            "或直接粘贴日志文本",
            value="",
            height=180,
            key="failed_owner_uid_log_text",
        )
        default_failed_owner_uid_name = (
            f"failed_owner_mid_from_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        failed_owner_uid_out_path = st.text_input(
            "输出 CSV 文件路径",
            value=str(_default_output_path(default_failed_owner_uid_name)),
            key="failed_owner_uid_out_path",
        )
        failed_owner_uid_log_placeholder = st.empty()

        if st.button("开始解析失败作者 UID 并导出", key="failed_owner_uid_submit"):
            failed_owner_uid_logs: list[str] = []

            def _failed_owner_uid_log(message: str) -> None:
                _append_log(failed_owner_uid_logs, failed_owner_uid_log_placeholder, message)

            try:
                if not log_files and not log_text.strip():
                    st.warning("请至少上传一个日志文件，或直接粘贴日志文本。")
                else:
                    matched_owner_mids: list[int] = []
                    uploaded_texts = _read_uploaded_text_files(log_files) if log_files else []

                    if uploaded_texts:
                        _failed_owner_uid_log(f"[INFO]: 已读取 {len(uploaded_texts)} 个日志文件。")
                    for file_name, text in uploaded_texts:
                        file_owner_mids = _extract_failed_owner_mids_from_text(text)
                        matched_owner_mids.extend(file_owner_mids)
                        _failed_owner_uid_log(
                            f"[INFO]: 文件 {file_name} 中解析到 {len(file_owner_mids)} 条失败作者记录。"
                        )

                    if log_text.strip():
                        pasted_owner_mids = _extract_failed_owner_mids_from_text(log_text)
                        matched_owner_mids.extend(pasted_owner_mids)
                        _failed_owner_uid_log(
                            f"[INFO]: 粘贴文本中解析到 {len(pasted_owner_mids)} 条失败作者记录。"
                        )

                    unique_owner_mids = sorted(set(matched_owner_mids))
                    if not unique_owner_mids:
                        _failed_owner_uid_log("[WARN]: 未匹配到失败作者 UID，请检查日志格式。")
                        st.warning("未从日志中匹配到失败作者 UID。")
                    else:
                        output_path = resolve_output_path(failed_owner_uid_out_path)
                        result_df = pd.DataFrame({"owner_mid": unique_owner_mids})
                        saved = export_dataframe(result_df, output_path)
                        _failed_owner_uid_log(
                            f"[INFO]: 已去重并升序整理为 {len(unique_owner_mids)} 个作者 UID。"
                        )
                        _failed_owner_uid_log(f"[INFO]: 导出完成：{saved}")
                        st.success(f"已导出：{saved}")
                        st.caption(f"共提取 {len(unique_owner_mids)} 个唯一失败作者 UID（展示前 200 条预览）")
                        st.dataframe(result_df.head(200), width="stretch", hide_index=True)
            finally:
                _show_saved_log_path(_save_task_logs("failed_owner_uid_extract", failed_owner_uid_logs))


with tab_merge:
    st.subheader("CSV/XLSX 文件拼接及去重")
    st.caption("上传多个本地 CSV/XLSX 文件，先拼接并导出；可选再基于指定键去重，并额外导出一份去重结果。")

    uploaded_files = st.file_uploader(
        "选择要拼接的文件（可多选）",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
    )

    sort_keys_raw = st.text_input(
        "排序键（逗号分隔，可留空）",
        value="",
        help="当留空时，默认按主键 bvid 降序排序；当填写时，所有排序键必须在每个表格中都存在。",
    )

    enable_dedup = st.checkbox("对输出文件进行去重", value=False)
    dedupe_keys_raw = ""
    keep_keys_raw = ""
    if enable_dedup:
        dedupe_keys_raw = st.text_input(
            "去重键（逗号分隔，可留空）",
            value="",
            help="留空时默认按整行内容去重；去重时优先保留获取时间最晚的记录，若时间相同则保留拼接结果中排在前面的一条。",
        )
        keep_keys_raw = st.text_input(
            "保留键（逗号分隔，留空则默认保留全部键）",
            value="",
            help="仅影响去重后导出的附加文件；留空时保留全部列。",
        )

    default_merge_name = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_merge_path = st.text_input(
        "输出文件路径",
        value=str(_default_output_path(default_merge_name)),
    )

    do_merge = st.button("开始拼接并导出")

    if do_merge:
        if not uploaded_files:
            st.warning("请先上传至少一个 CSV/XLSX 文件。")
        else:
            dfs: list[pd.DataFrame] = []
            for f in uploaded_files:
                try:
                    df = read_uploaded_dataframe(f)
                except Exception as e:  # noqa: BLE001
                    st.error(f"读取文件失败：{f.name}（{e}）")
                    dfs = []
                    break
                dfs.append(df)

            if dfs:
                sort_keys = parse_comma_separated_keys(sort_keys_raw)
                for idx, df in enumerate(dfs):
                    try:
                        validate_columns(df, sort_keys, "排序键")
                    except ValueError as e:
                        st.error(f"第 {idx + 1} 个文件（{uploaded_files[idx].name}）校验失败：{e}")
                        dfs = []
                        break

            if dfs:
                try:
                    merged = merge_dataframes(dfs, sort_keys)
                    merged_out_path = resolve_output_path(out_merge_path)
                    export_dataframe(merged, merged_out_path)

                    st.success(f"已导出拼接文件：{merged_out_path}")
                    st.caption(f"拼接结果共 {len(merged)} 条记录（展示前 200 条预览）")
                    st.dataframe(merged.head(200), width="stretch", hide_index=True)

                    if enable_dedup:
                        dedupe_keys = parse_comma_separated_keys(dedupe_keys_raw)
                        keep_keys = parse_comma_separated_keys(keep_keys_raw)
                        deduplicated, dedupe_time_column = deduplicate_dataframe(
                            merged,
                            dedupe_keys=dedupe_keys,
                            keep_keys=keep_keys or None,
                        )
                        dedup_out_path = build_deduplicated_output_path(merged_out_path)
                        export_dataframe(deduplicated, dedup_out_path)

                        if dedupe_time_column is not None:
                            st.success(f"已导出去重文件：{dedup_out_path}（按 {dedupe_time_column} 保留最新记录）")
                        else:
                            st.success(f"已导出去重文件：{dedup_out_path}（未识别到获取时间列，重复项保留拼接结果中靠前的一条）")

                        st.caption(f"去重结果共 {len(deduplicated)} 条记录（展示前 200 条预览）")
                        st.dataframe(deduplicated.head(200), width="stretch", hide_index=True)
                except Exception as e:  # noqa: BLE001
                    st.error(f"拼接或导出失败：{e}")


with tab_quick_jump:
    st.subheader("快捷跳转")
    st.caption("输入单个 BVID 或作者 ID，点击后自动使用系统默认浏览器打开对应页面。")

    video_col, owner_col = st.columns(2)

    with video_col:
        quick_jump_bvid = st.text_input(
            "视频 BVID",
            value="",
            key="quick_jump_bvid",
            help="支持直接输入 BVID，也支持粘贴视频链接后自动提取。",
        )
        if st.button("跳转到视频页", key="quick_jump_bvid_submit", width="stretch"):
            normalized_bvid = normalize_bvid(quick_jump_bvid)
            if normalized_bvid is None:
                st.warning("请输入有效的 BVID。")
            else:
                video_url = build_video_url(normalized_bvid)
                if open_in_default_browser(video_url):
                    st.success(f"已打开视频页：{video_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")

    with owner_col:
        quick_jump_owner_mid = st.text_input(
            "作者 ID",
            value="",
            key="quick_jump_owner_mid",
            help="支持直接输入作者 ID，也支持粘贴作者主页链接后自动提取。",
        )
        if st.button("跳转到作者主页", key="quick_jump_owner_mid_submit", width="stretch"):
            normalized_owner_mid = normalize_owner_mid(quick_jump_owner_mid)
            if normalized_owner_mid is None:
                st.warning("请输入有效的作者 ID。")
            else:
                owner_url = build_owner_space_url(normalized_owner_mid)
                if open_in_default_browser(owner_url):
                    st.success(f"已打开作者主页：{owner_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")

