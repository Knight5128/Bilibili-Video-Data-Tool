from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from bilibili_api.user import User

from bili_pipeline.utils import run_async
from bili_pipeline.utils.file_merge import export_dataframe


AUTHOR_REFINEMENTS_OUTPUT_DIR = Path("outputs") / "author_refinements"
AUTHOR_REFINEMENT_STATE_FILENAME = "author_refinement_state.json"
AUTHOR_REFINEMENT_SUMMARY_FILENAME = "author_refinement_summary.log"
AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME = "original_authors.csv"
AUTHOR_REFINEMENT_ACCUMULATED_FILENAME = "expanded_author_list_accumulated.csv"
AUTHOR_REFINEMENT_COMPLETE_FILENAME = "expanded_author_list_complete.csv"
AUTHOR_REFINEMENT_REFINED_FULL_FILENAME = "refined_full.csv"
AUTHOR_REFINEMENT_REFINED_SAMPLED_FILENAME = "refined_sampled.csv"
AUTHOR_REFINEMENT_BIN_SUMMARY_FILENAME = "refinement_bin_summary.csv"
AUTHOR_REFINEMENT_BASE_CHART_FILENAME = "author_follower_distribution.html"
AUTHOR_REFINEMENT_COMPARE_CHART_FILENAME = "author_follower_distribution_compare.html"
AUTHOR_REMAINING_FILE_PATTERN = re.compile(r"remaining_authors_part_(\d+)\.csv$", re.IGNORECASE)
AUTHOR_METADATA_PART_FILE_PATTERN = re.compile(r"author_metadata_part_(\d+)\.csv$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class AuthorCategoryOption:
    column: str
    label: str


@dataclass(frozen=True, slots=True)
class PriorityRetentionRule:
    min_follower_count: int
    retention_ratio: float


@dataclass(slots=True)
class AuthorRefinementSessionContext:
    root_dir: Path
    session_dir: Path
    logs_dir: Path
    part_number: int
    is_new_session: bool


@dataclass(slots=True)
class AuthorMetadataBatchOutcome:
    metadata_df: pd.DataFrame
    failures: list[dict[str, Any]]
    remaining_owner_mids: list[int]
    successful_owner_count: int
    processed_owner_count: int
    risk_failure_count: int
    stopped_due_to_risk: bool
    last_risk_error: str


AUTHOR_CATEGORY_OPTIONS: tuple[AuthorCategoryOption, ...] = (
    AuthorCategoryOption("owner_verified", "作者是否认证"),
    AuthorCategoryOption("owner_level", "作者账号等级"),
    AuthorCategoryOption("owner_gender", "作者性别"),
    AuthorCategoryOption("owner_vip_type", "作者会员类型"),
)

AUTHOR_METADATA_COLUMN_ORDER = [
    "owner_mid",
    "owner_name",
    "owner_face",
    "owner_sign",
    "owner_gender",
    "owner_level",
    "owner_verified",
    "owner_verified_title",
    "owner_vip_type",
    "owner_follower_count",
    "owner_following_count",
    "owner_video_count",
]

_RISK_MARKERS = (
    "352",
    "412",
    "429",
    "too many requests",
    "precondition failed",
    "风控",
)

_OVERLAY_COLORS = [
    "#E45756",
    "#54A24B",
    "#F58518",
    "#B279A2",
    "#72B7B2",
    "#EECA3B",
]


def parse_priority_retention_rules(text: str) -> list[PriorityRetentionRule]:
    raw_text = (text or "").strip()
    if not raw_text:
        return []

    rules: list[PriorityRetentionRule] = []
    seen_thresholds: set[int] = set()
    for line_number, raw_line in enumerate(raw_text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        parts = [item.strip() for item in re.split(r"[,，\t]+", line) if item.strip()]
        if len(parts) != 2:
            raise ValueError(f"第 {line_number} 行格式不正确；请按“最低粉丝数,保留比例”填写。")
        try:
            min_follower_count = int(float(parts[0]))
        except ValueError as exc:
            raise ValueError(f"第 {line_number} 行最低粉丝数无法解析：{parts[0]}") from exc
        try:
            retention_ratio = float(parts[1])
        except ValueError as exc:
            raise ValueError(f"第 {line_number} 行保留比例无法解析：{parts[1]}") from exc
        if min_follower_count < 0:
            raise ValueError(f"第 {line_number} 行最低粉丝数必须大于等于 0。")
        if not 0 <= retention_ratio <= 1:
            raise ValueError(f"第 {line_number} 行保留比例必须位于 0 到 1 之间。")
        if min_follower_count in seen_thresholds:
            raise ValueError(f"第 {line_number} 行与前文存在重复的最低粉丝数阈值：{min_follower_count}。")
        seen_thresholds.add(min_follower_count)
        rules.append(PriorityRetentionRule(min_follower_count=min_follower_count, retention_ratio=retention_ratio))

    return sorted(rules, key=lambda item: item.min_follower_count, reverse=True)


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_verified(official_info: dict[str, Any]) -> bool:
    verified_title = official_info.get("title") or official_info.get("desc")
    official_type = official_info.get("type")
    return bool(verified_title or official_info.get("role") or official_type not in (-1, None))


def _coerce_mid_frame(df: pd.DataFrame, owner_mid_column: str) -> pd.DataFrame:
    working = df.copy()
    working[owner_mid_column] = pd.to_numeric(working[owner_mid_column], errors="coerce").astype("Int64")
    return working


def _owner_mid_dataframe(owner_mids: list[int]) -> pd.DataFrame:
    return pd.DataFrame({"owner_mid": owner_mids})


def _extract_owner_mids(df: pd.DataFrame, column_name: str) -> list[int]:
    parsed = pd.to_numeric(df[column_name], errors="coerce")
    owner_mids = [int(value) for value in parsed.dropna().tolist()]
    return list(dict.fromkeys(owner_mids))


def _read_owner_mid_csv(path: Path) -> list[int]:
    try:
        df = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return []
    if "owner_mid" not in df.columns:
        return []
    return _extract_owner_mids(df, "owner_mid")


def _load_state(session_dir: Path) -> dict[str, Any]:
    state_path = session_dir / AUTHOR_REFINEMENT_STATE_FILENAME
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_state(session_dir: Path, state: dict[str, Any]) -> Path:
    session_dir.mkdir(parents=True, exist_ok=True)
    state_path = session_dir / AUTHOR_REFINEMENT_STATE_FILENAME
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state_path


def save_remaining_author_csv(owner_mids: list[int], path: Path) -> Path:
    return export_dataframe(_owner_mid_dataframe(owner_mids), path)


def infer_next_author_refinement_part(session_dir: Path) -> int:
    highest_part = 0
    state = _load_state(session_dir)
    for part in state.get("parts", []):
        try:
            highest_part = max(highest_part, int(part.get("part_number", 0)))
        except (TypeError, ValueError):
            continue
    for path in session_dir.glob("author_metadata_part_*.csv"):
        match = AUTHOR_METADATA_PART_FILE_PATTERN.search(path.name)
        if match is None:
            continue
        highest_part = max(highest_part, int(match.group(1)))
    for path in session_dir.glob("remaining_authors_part_*.csv"):
        match = AUTHOR_REMAINING_FILE_PATTERN.search(path.name)
        if match is None:
            continue
        highest_part = max(highest_part, int(match.group(1)))
    return highest_part + 1


def find_matching_author_refinement_session(
    output_root: Path,
    uploaded_file_name: str,
    owner_mids: list[int],
) -> tuple[Path | None, int | None]:
    match = AUTHOR_REMAINING_FILE_PATTERN.search(uploaded_file_name or "")
    if match is None:
        return None, None

    uploaded_part_number = int(match.group(1))
    candidate_paths = sorted(
        output_root.glob(f"*/remaining_authors_part_{uploaded_part_number}.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    fallback_dirs: list[Path] = []
    exact_match_dirs: list[Path] = []
    for candidate_path in candidate_paths:
        fallback_dirs.append(candidate_path.parent)
        candidate_owner_mids = _read_owner_mid_csv(candidate_path)
        if candidate_owner_mids == owner_mids:
            exact_match_dirs.append(candidate_path.parent)

    if exact_match_dirs:
        return exact_match_dirs[0], uploaded_part_number
    if fallback_dirs:
        return fallback_dirs[0], uploaded_part_number
    return None, uploaded_part_number


def prepare_author_refinement_session(
    output_root: Path,
    uploaded_file_name: str,
    owner_mids: list[int],
    task_started_at: datetime,
    logger=None,
) -> AuthorRefinementSessionContext:
    root_dir = Path(output_root or AUTHOR_REFINEMENTS_OUTPUT_DIR)
    root_dir.mkdir(parents=True, exist_ok=True)
    session_dir, uploaded_part_number = find_matching_author_refinement_session(root_dir, uploaded_file_name, owner_mids)
    if session_dir is not None:
        part_number = max(infer_next_author_refinement_part(session_dir), (uploaded_part_number or 0) + 1)
        if logger is not None:
            logger(f"[INFO] 识别到已有作者洞察任务：{session_dir.as_posix()}，本次将续跑为 part_{part_number}。")
        return AuthorRefinementSessionContext(
            root_dir=root_dir,
            session_dir=session_dir,
            logs_dir=session_dir / "logs",
            part_number=part_number,
            is_new_session=False,
        )

    session_dir = root_dir / f"author_refinement_{task_started_at.strftime('%Y%m%d_%H%M%S')}"
    suffix = 2
    while session_dir.exists():
        session_dir = root_dir / f"author_refinement_{task_started_at.strftime('%Y%m%d_%H%M%S')}_{suffix}"
        suffix += 1
    if logger is not None:
        if uploaded_part_number is not None:
            logger("[WARN] 上传文件名像历史 remaining_authors_part_n.csv，但未找到匹配会话；已按新任务创建目录。")
        logger(f"[INFO] 已创建新的作者洞察任务目录：{session_dir.as_posix()}。")
    return AuthorRefinementSessionContext(
        root_dir=root_dir,
        session_dir=session_dir,
        logs_dir=session_dir / "logs",
        part_number=1,
        is_new_session=True,
    )


def build_followup_author_refinement_session(session_dir: Path) -> AuthorRefinementSessionContext:
    return AuthorRefinementSessionContext(
        root_dir=session_dir.parent,
        session_dir=session_dir,
        logs_dir=session_dir / "logs",
        part_number=infer_next_author_refinement_part(session_dir),
        is_new_session=False,
    )


def classify_author_refinement_error(exc: Exception) -> tuple[bool, str]:
    message = " ".join(str(exc).split())
    lowered = message.lower()
    is_risk = any(marker in lowered for marker in _RISK_MARKERS)
    return is_risk, message or exc.__class__.__name__


def crawl_author_metadata(owner_mid: int, credential: Any | None = None) -> dict[str, Any]:
    user = User(owner_mid, credential=credential) if credential is not None else User(owner_mid)
    user_info = run_async(user.get_user_info()) or {}
    relation_info = run_async(user.get_relation_info()) or {}
    overview_stat = run_async(user.get_overview_stat()) or {}
    official_info = user_info.get("official") or {}
    vip_info = user_info.get("vip") or {}

    return {
        "owner_mid": owner_mid,
        "owner_name": user_info.get("name"),
        "owner_face": user_info.get("face"),
        "owner_sign": user_info.get("sign"),
        "owner_gender": user_info.get("sex"),
        "owner_level": _to_int(user_info.get("level")),
        "owner_verified": _is_verified(official_info),
        "owner_verified_title": official_info.get("title") or official_info.get("desc"),
        "owner_vip_type": _to_int(vip_info.get("type")),
        "owner_follower_count": _to_int(relation_info.get("follower")),
        "owner_following_count": _to_int(relation_info.get("following")),
        "owner_video_count": _to_int(overview_stat.get("video")),
    }


def crawl_author_metadata_with_guardrails(
    owner_mids: list[int],
    *,
    credential: Any | None = None,
    logger=None,
    request_pause_seconds: float = 0.0,
    consecutive_risk_limit: int = 5,
) -> AuthorMetadataBatchOutcome:
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    consecutive_risk_hits = 0
    last_risk_error = ""
    processed_owner_count = 0

    total = len(owner_mids)
    for index, owner_mid in enumerate(owner_mids, start=1):
        if request_pause_seconds > 0 and index > 1:
            time.sleep(request_pause_seconds)
        if logger is not None:
            logger(f"[{index}/{total}] 正在抓取作者 {owner_mid} 的 MetaResult 级作者属性。")
        try:
            row = crawl_author_metadata(owner_mid, credential=credential)
        except Exception as exc:  # noqa: BLE001
            processed_owner_count += 1
            is_risk, error_message = classify_author_refinement_error(exc)
            failures.append({"owner_mid": owner_mid, "error": error_message, "is_risk": int(is_risk)})
            if is_risk:
                consecutive_risk_hits += 1
                last_risk_error = error_message
                if logger is not None:
                    logger(
                        f"[WARN] 作者 {owner_mid} 触发风控相关报错（连续 {consecutive_risk_hits}/{consecutive_risk_limit} 次）："
                        f"{error_message}"
                    )
                if consecutive_risk_hits >= max(1, int(consecutive_risk_limit)):
                    remaining_owner_mids = owner_mids[index - 1 :]
                    frame = pd.DataFrame(rows).reindex(columns=AUTHOR_METADATA_COLUMN_ORDER) if rows else pd.DataFrame(columns=AUTHOR_METADATA_COLUMN_ORDER)
                    return AuthorMetadataBatchOutcome(
                        metadata_df=frame,
                        failures=failures,
                        remaining_owner_mids=remaining_owner_mids,
                        successful_owner_count=len(rows),
                        processed_owner_count=processed_owner_count,
                        risk_failure_count=consecutive_risk_hits,
                        stopped_due_to_risk=True,
                        last_risk_error=last_risk_error,
                    )
            else:
                consecutive_risk_hits = 0
                if logger is not None:
                    logger(f"[WARN] 作者 {owner_mid} 抓取失败，但未判定为风控：{error_message}")
            continue

        processed_owner_count += 1
        consecutive_risk_hits = 0
        rows.append(row)
        if logger is not None:
            logger(
                f"[OK] 作者 {owner_mid} 抓取完成：粉丝 {row.get('owner_follower_count')}, "
                f"关注 {row.get('owner_following_count')}, 公开视频 {row.get('owner_video_count')}。"
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=AUTHOR_METADATA_COLUMN_ORDER)
    else:
        frame = frame.reindex(columns=AUTHOR_METADATA_COLUMN_ORDER)
    return AuthorMetadataBatchOutcome(
        metadata_df=frame,
        failures=failures,
        remaining_owner_mids=[],
        successful_owner_count=len(rows),
        processed_owner_count=processed_owner_count,
        risk_failure_count=0,
        stopped_due_to_risk=False,
        last_risk_error=last_risk_error,
    )


def merge_author_metadata(source_df: pd.DataFrame, metadata_df: pd.DataFrame, owner_mid_column: str) -> pd.DataFrame:
    prepared_source = _coerce_mid_frame(source_df, owner_mid_column)
    prepared_meta = metadata_df.copy()
    if "owner_mid" in prepared_meta.columns:
        prepared_meta["owner_mid"] = pd.to_numeric(prepared_meta["owner_mid"], errors="coerce").astype("Int64")
    merged = prepared_source.merge(
        prepared_meta,
        how="left",
        left_on=owner_mid_column,
        right_on="owner_mid",
        suffixes=("", "_author_meta"),
    )
    return merged


def build_session_metadata_dataframe(session_dir: Path) -> pd.DataFrame:
    part_frames: list[pd.DataFrame] = []
    for path in sorted(session_dir.glob("author_metadata_part_*.csv")):
        try:
            frame = pd.read_csv(path)
        except Exception:  # noqa: BLE001
            continue
        if frame.empty:
            continue
        part_frames.append(frame)

    if not part_frames:
        return pd.DataFrame(columns=AUTHOR_METADATA_COLUMN_ORDER)

    merged = pd.concat(part_frames, ignore_index=True)
    merged["owner_mid"] = pd.to_numeric(merged["owner_mid"], errors="coerce").astype("Int64")
    merged = merged.drop_duplicates(subset=["owner_mid"], keep="last")
    return merged.reindex(columns=AUTHOR_METADATA_COLUMN_ORDER).reset_index(drop=True)


def build_session_expanded_author_list(session_dir: Path) -> pd.DataFrame:
    metadata_df = build_session_metadata_dataframe(session_dir)
    state = _load_state(session_dir)
    owner_mid_column = str(state.get("owner_mid_column") or "owner_mid")
    original_path = session_dir / AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME
    if not original_path.exists():
        return metadata_df.copy()
    try:
        original_df = pd.read_csv(original_path)
    except Exception:  # noqa: BLE001
        return metadata_df.copy()
    if original_df.empty:
        return metadata_df.copy()
    if owner_mid_column not in original_df.columns:
        owner_mid_column = "owner_mid" if "owner_mid" in original_df.columns else original_df.columns[0]
    return merge_author_metadata(original_df, metadata_df, owner_mid_column)


def record_author_refinement_part(
    session: AuthorRefinementSessionContext,
    *,
    owner_mid_column: str,
    source_name: str,
    input_owner_count: int,
    outcome: AuthorMetadataBatchOutcome,
    metadata_part_path: Path,
    accumulated_path: Path,
    remaining_path: Path | None,
    failures_path: Path | None,
    log_path: Path | None,
    run_started_at: str,
    run_finished_at: str,
) -> dict[str, Any]:
    state = _load_state(session.session_dir)
    parts = [part for part in state.get("parts", []) if int(part.get("part_number", 0)) != session.part_number]
    parts.append(
        {
            "part_number": session.part_number,
            "run_started_at": run_started_at,
            "run_finished_at": run_finished_at,
            "input_owner_count": input_owner_count,
            "processed_owner_count": outcome.processed_owner_count,
            "successful_owner_count": outcome.successful_owner_count,
            "failure_count": len(outcome.failures),
            "remaining_owner_count": len(outcome.remaining_owner_mids),
            "risk_failure_count": outcome.risk_failure_count,
            "stopped_due_to_risk": outcome.stopped_due_to_risk,
            "last_risk_error": outcome.last_risk_error,
            "metadata_part_file": metadata_part_path.name,
            "accumulated_file": accumulated_path.name,
            "remaining_file": remaining_path.name if remaining_path is not None else "",
            "failures_file": failures_path.name if failures_path is not None else "",
            "log_file": log_path.relative_to(session.session_dir).as_posix() if log_path is not None else "",
        }
    )
    parts.sort(key=lambda item: int(item.get("part_number", 0)))
    state.update(
        {
            "session_dir": session.session_dir.as_posix(),
            "task_started_at": state.get("task_started_at") or run_started_at,
            "owner_mid_column": owner_mid_column,
            "source_name": source_name,
            "original_author_count": int(state.get("original_author_count") or input_owner_count),
            "parts": parts,
            "completed_all": len(outcome.remaining_owner_mids) == 0,
            "latest_accumulated_file": accumulated_path.name,
            "last_remaining_file": remaining_path.name if remaining_path is not None else "",
            "last_risk_error": outcome.last_risk_error,
            "updated_at": run_finished_at,
        }
    )
    _save_state(session.session_dir, state)
    return state


def build_author_refinement_summary_text(state: dict[str, Any]) -> str:
    parts = sorted(state.get("parts", []), key=lambda item: int(item.get("part_number", 0)))
    lines = [
        "author_refinement 任务总结",
        f"session_dir: {state.get('session_dir', '')}",
        f"source_name: {state.get('source_name', '')}",
        f"owner_mid_column: {state.get('owner_mid_column', '')}",
        f"original_author_count: {state.get('original_author_count', 0)}",
        f"part_count: {len(parts)}",
        f"completed_all: {state.get('completed_all', False)}",
        f"task_started_at: {state.get('task_started_at', '')}",
        f"updated_at: {state.get('updated_at', '')}",
        f"last_risk_error: {state.get('last_risk_error', '')}",
        "",
    ]
    for part in parts:
        lines.extend(
            [
                f"[part_{part.get('part_number', 0)}]",
                f"run_started_at: {part.get('run_started_at', '')}",
                f"run_finished_at: {part.get('run_finished_at', '')}",
                f"input_owner_count: {part.get('input_owner_count', 0)}",
                f"processed_owner_count: {part.get('processed_owner_count', 0)}",
                f"successful_owner_count: {part.get('successful_owner_count', 0)}",
                f"failure_count: {part.get('failure_count', 0)}",
                f"remaining_owner_count: {part.get('remaining_owner_count', 0)}",
                f"risk_failure_count: {part.get('risk_failure_count', 0)}",
                f"stopped_due_to_risk: {part.get('stopped_due_to_risk', False)}",
                f"metadata_part_file: {part.get('metadata_part_file', '')}",
                f"remaining_file: {part.get('remaining_file', '')}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def write_author_refinement_summary(session_dir: Path, state: dict[str, Any]) -> Path:
    summary_path = session_dir / AUTHOR_REFINEMENT_SUMMARY_FILENAME
    summary_path.write_text(build_author_refinement_summary_text(state), encoding="utf-8")
    return summary_path


def _build_follower_bins(series: pd.Series, bin_count: int) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    result = pd.Series("missing_or_nonpositive", index=series.index, dtype="object")
    positive_mask = numeric > 0
    if not positive_mask.any():
        return result

    log_values = numeric.loc[positive_mask].astype(float).map(math.log10)
    active_bin_count = max(1, min(int(bin_count), int(log_values.nunique())))
    if active_bin_count == 1:
        result.loc[positive_mask] = "positive"
        return result

    result.loc[positive_mask] = pd.cut(log_values, bins=active_bin_count, include_lowest=True, duplicates="drop").astype(str)
    return result


def stratified_sample_by_followers(
    df: pd.DataFrame,
    *,
    sample_ratio: float,
    follower_column: str = "owner_follower_count",
    bin_count: int = 10,
    random_state: int = 42,
    sample_target_count: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty = df.copy()
        empty["refinement_keep"] = pd.Series(dtype="bool")
        empty["refinement_follower_bin"] = pd.Series(dtype="object")
        return empty, df.copy(), pd.DataFrame(columns=["follower_bin", "original_count", "target_float", "sampled_count"])

    working = df.copy()
    working["refinement_follower_bin"] = _build_follower_bins(working[follower_column], bin_count=bin_count)

    total_count = len(working)
    target_count = int(sample_target_count) if sample_target_count is not None else math.ceil(total_count * sample_ratio)
    if target_count <= 0:
        working["refinement_keep"] = False
        summary = (
            working.groupby("refinement_follower_bin", dropna=False)
            .size()
            .rename("original_count")
            .reset_index()
            .rename(columns={"refinement_follower_bin": "follower_bin"})
        )
        summary["target_float"] = summary["original_count"] * float(sample_ratio)
        summary["sampled_count"] = 0
        return working, working.iloc[0:0].copy(), summary

    if target_count >= total_count:
        working["refinement_keep"] = True
        summary = (
            working.groupby("refinement_follower_bin", dropna=False)
            .size()
            .rename("original_count")
            .reset_index()
            .rename(columns={"refinement_follower_bin": "follower_bin"})
        )
        summary["target_float"] = summary["original_count"] * float(sample_ratio)
        summary["sampled_count"] = summary["original_count"]
        return working, working.copy(), summary

    grouped = working.groupby("refinement_follower_bin", dropna=False, sort=True)
    original_counts = grouped.size()
    target_floats = original_counts * float(sample_ratio)
    sampled_counts = target_floats.map(math.floor).astype(int)
    remaining = target_count - int(sampled_counts.sum())
    residual = (target_floats - sampled_counts).sort_values(ascending=False)

    for bin_name in residual.index:
        if remaining <= 0:
            break
        available = int(original_counts.loc[bin_name] - sampled_counts.loc[bin_name])
        if available <= 0:
            continue
        sampled_counts.loc[bin_name] += 1
        remaining -= 1

    sampled_parts: list[pd.DataFrame] = []
    for offset, (bin_name, group) in enumerate(grouped):
        take_count = int(sampled_counts.loc[bin_name])
        if take_count <= 0:
            continue
        if take_count >= len(group):
            sampled_parts.append(group.copy())
            continue
        sampled_parts.append(group.sample(n=take_count, random_state=int(random_state) + offset))

    sampled_df = pd.concat(sampled_parts, axis=0).sort_index() if sampled_parts else working.iloc[0:0].copy()
    working["refinement_keep"] = working.index.isin(sampled_df.index)
    sampled_df = working.loc[working["refinement_keep"]].copy()
    summary = pd.DataFrame(
        {
            "follower_bin": original_counts.index.astype(str),
            "original_count": original_counts.values,
            "target_float": target_floats.values,
            "sampled_count": [int(sampled_counts.loc[bin_name]) for bin_name in original_counts.index],
        }
    )
    return working, sampled_df, summary


def sample_authors_with_priority_rules(
    df: pd.DataFrame,
    *,
    sample_ratio: float,
    priority_rules: list[PriorityRetentionRule] | None = None,
    follower_column: str = "owner_follower_count",
    bin_count: int = 10,
    random_state: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rules = priority_rules or []
    if not rules:
        refined_full_df, sampled_df, bin_summary_df = stratified_sample_by_followers(
            df,
            sample_ratio=sample_ratio,
            follower_column=follower_column,
            bin_count=bin_count,
            random_state=random_state,
        )
        empty_summary = pd.DataFrame(
            columns=[
                "区间",
                "最低粉丝数",
                "最高粉丝数",
                "原始作者数",
                "保留比例",
                "保留作者数",
            ]
        )
        return refined_full_df, sampled_df, bin_summary_df, empty_summary

    if df.empty:
        refined_full_df, sampled_df, bin_summary_df = stratified_sample_by_followers(
            df,
            sample_ratio=sample_ratio,
            follower_column=follower_column,
            bin_count=bin_count,
            random_state=random_state,
        )
        empty_summary = pd.DataFrame(
            columns=[
                "区间",
                "最低粉丝数",
                "最高粉丝数",
                "原始作者数",
                "保留比例",
                "保留作者数",
            ]
        )
        return refined_full_df, sampled_df, bin_summary_df, empty_summary

    working = df.copy()
    follower_series = pd.to_numeric(working[follower_column], errors="coerce")
    covered_mask = pd.Series(False, index=working.index)
    priority_selected_parts: list[pd.DataFrame] = []
    priority_summary_rows: list[dict[str, object]] = []
    previous_upper_bound: int | None = None

    for offset, rule in enumerate(rules):
        interval_mask = follower_series >= rule.min_follower_count
        if previous_upper_bound is not None:
            interval_mask &= follower_series < previous_upper_bound
        interval_df = working.loc[interval_mask].copy()
        covered_mask |= interval_mask.fillna(False)
        original_count = len(interval_df)
        keep_count = min(original_count, math.ceil(original_count * float(rule.retention_ratio)))
        if keep_count <= 0:
            selected_df = interval_df.iloc[0:0].copy()
        elif keep_count >= original_count:
            selected_df = interval_df.copy()
        else:
            selected_df = interval_df.sample(n=keep_count, random_state=int(random_state) + offset)
        priority_selected_parts.append(selected_df)
        priority_summary_rows.append(
            {
                "区间": f"[{rule.min_follower_count}, {previous_upper_bound if previous_upper_bound is not None else 'inf'})",
                "最低粉丝数": rule.min_follower_count,
                "最高粉丝数": previous_upper_bound,
                "原始作者数": original_count,
                "保留比例": float(rule.retention_ratio),
                "保留作者数": len(selected_df),
            }
        )
        previous_upper_bound = rule.min_follower_count

    priority_selected_df = (
        pd.concat(priority_selected_parts, axis=0).sort_index().drop_duplicates()
        if priority_selected_parts
        else working.iloc[0:0].copy()
    )
    total_target_count = math.ceil(len(working) * float(sample_ratio))
    remaining_pool = working.loc[~covered_mask].copy()
    remaining_target_count = max(0, total_target_count - len(priority_selected_df))
    remaining_refined_df, remaining_sampled_df, _remaining_bin_summary = stratified_sample_by_followers(
        remaining_pool,
        sample_ratio=sample_ratio,
        follower_column=follower_column,
        bin_count=bin_count,
        random_state=int(random_state) + len(rules) + 1,
        sample_target_count=remaining_target_count,
    )
    sampled_df = (
        pd.concat([priority_selected_df, remaining_sampled_df], axis=0).sort_index().drop_duplicates()
        if not priority_selected_df.empty or not remaining_sampled_df.empty
        else working.iloc[0:0].copy()
    )
    full_refined_df = working.copy()
    full_refined_df["refinement_follower_bin"] = _build_follower_bins(full_refined_df[follower_column], bin_count=bin_count)
    full_refined_df["refinement_keep"] = full_refined_df.index.isin(sampled_df.index)
    bin_summary_df = (
        full_refined_df.groupby("refinement_follower_bin", dropna=False)
        .agg(
            original_count=(follower_column, "size"),
            sampled_count=("refinement_keep", "sum"),
        )
        .reset_index()
        .rename(columns={"refinement_follower_bin": "follower_bin"})
    )
    bin_summary_df["target_float"] = bin_summary_df["original_count"] * float(sample_ratio)
    return full_refined_df, sampled_df, bin_summary_df, pd.DataFrame(priority_summary_rows)


def _encode_category_values(df: pd.DataFrame, column: str) -> tuple[pd.Series, dict[int, str]]:
    series = df[column] if column in df.columns else pd.Series(index=df.index, dtype="object")
    if column == "owner_verified":
        encoded = series.fillna(False).astype(bool).astype(int)
        return encoded, {0: "未认证", 1: "已认证"}

    numeric = pd.to_numeric(series, errors="coerce")
    if column in {"owner_level", "owner_vip_type"} and numeric.notna().any():
        encoded = numeric.fillna(-1).astype(int)
        unique_values = sorted(encoded.unique().tolist())
        return encoded, {int(value): str(int(value)) for value in unique_values}

    filled = series.fillna("未知").astype(str)
    categories = list(dict.fromkeys(filled.tolist()))
    mapping = {name: code for code, name in enumerate(categories)}
    encoded = filled.map(mapping).astype(int)
    return encoded, {code: name for name, code in mapping.items()}


def build_author_distribution_figure(
    df: pd.DataFrame,
    *,
    selected_category_columns: list[str] | None = None,
    use_log_x: bool = False,
) -> tuple[go.Figure, dict[str, dict[int, str]]]:
    selected_columns = selected_category_columns or []
    followers = pd.to_numeric(df.get("owner_follower_count"), errors="coerce")
    valid_df = df.loc[followers.notna()].copy()
    valid_df["owner_follower_count"] = followers.loc[followers.notna()].astype(float)
    if use_log_x:
        valid_df = valid_df.loc[valid_df["owner_follower_count"] > 0].copy()

    figure = go.Figure()
    figure.add_trace(
        go.Histogram(
            x=valid_df["owner_follower_count"],
            name="全部作者粉丝分布",
            nbinsx=max(12, min(80, len(valid_df))),
            marker_color="#4C78A8",
            opacity=0.65,
            hovertemplate="粉丝量区间：%{x}<br>作者数：%{y}<extra></extra>",
        )
    )

    label_lookup = {item.column: item.label for item in AUTHOR_CATEGORY_OPTIONS}
    mapping_summary: dict[str, dict[int, str]] = {}
    for index, column in enumerate(selected_columns):
        if column not in valid_df.columns:
            continue
        encoded, mapping = _encode_category_values(valid_df, column)
        mapping_summary[column] = mapping
        figure.add_trace(
            go.Scattergl(
                x=valid_df["owner_follower_count"],
                y=encoded,
                mode="markers",
                name=label_lookup.get(column, column),
                marker={
                    "color": _OVERLAY_COLORS[index % len(_OVERLAY_COLORS)],
                    "opacity": 0.42,
                    "size": 8,
                },
                yaxis="y2",
                customdata=valid_df[[column]].astype(str).to_numpy(),
                hovertemplate="粉丝量：%{x}<br>编码值：%{y}<br>原值：%{customdata[0]}<extra></extra>",
            )
        )

    figure.update_layout(
        barmode="overlay",
        height=560,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        margin={"l": 40, "r": 40, "t": 40, "b": 40},
        xaxis={"title": "作者粉丝量", "type": "log" if use_log_x else "linear"},
        yaxis={"title": "作者数"},
        yaxis2={
            "title": "类别变量编码值",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "rangemode": "tozero",
        },
    )
    return figure, mapping_summary


def build_refinement_comparison_figure(
    full_df: pd.DataFrame,
    sampled_df: pd.DataFrame,
    *,
    use_log_x: bool = False,
) -> go.Figure:
    full_followers = pd.to_numeric(full_df.get("owner_follower_count"), errors="coerce")
    sampled_followers = pd.to_numeric(sampled_df.get("owner_follower_count"), errors="coerce")
    if use_log_x:
        full_followers = full_followers.loc[full_followers > 0]
        sampled_followers = sampled_followers.loc[sampled_followers > 0]

    figure = go.Figure()
    figure.add_trace(
        go.Histogram(
            x=full_followers.dropna(),
            name="精简前",
            nbinsx=max(12, min(80, len(full_df))),
            marker_color="#4C78A8",
            opacity=0.55,
        )
    )
    figure.add_trace(
        go.Histogram(
            x=sampled_followers.dropna(),
            name="精简后",
            nbinsx=max(12, min(80, len(full_df))),
            marker_color="#E45756",
            opacity=0.55,
        )
    )
    figure.update_layout(
        barmode="overlay",
        height=420,
        margin={"l": 40, "r": 40, "t": 40, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        xaxis={"title": "作者粉丝量", "type": "log" if use_log_x else "linear"},
        yaxis={"title": "作者数"},
    )
    return figure
