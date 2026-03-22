from __future__ import annotations

from pathlib import Path

import pandas as pd


DEFAULT_SORT_KEY = "bvid"
DEDUPE_TIME_CANDIDATE_COLUMNS = (
    "last_seen_at",
    "discovered_at",
    "snapshot_time",
    "crawl_time",
    "updated_at",
    "created_at",
)


def parse_comma_separated_keys(raw: str) -> list[str]:
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return list(dict.fromkeys(items))


def read_uploaded_dataframe(uploaded_file) -> pd.DataFrame:
    suffix = Path(uploaded_file.name).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(uploaded_file)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(uploaded_file)
    raise ValueError(f"Unsupported file type: {uploaded_file.name}")


def validate_columns(df: pd.DataFrame, keys: list[str], label: str) -> None:
    missing = [key for key in keys if key not in df.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"{label}不存在：{missing_text}")


def merge_dataframes(dfs: list[pd.DataFrame], sort_keys: list[str]) -> pd.DataFrame:
    if not dfs:
        raise ValueError("No dataframes were provided.")
    merged = pd.concat(dfs, ignore_index=True)
    if sort_keys:
        merged = merged.sort_values(
            by=sort_keys,
            ascending=[True] * len(sort_keys),
            kind="mergesort",
        )
    else:
        merged = merged.sort_values(
            by=[DEFAULT_SORT_KEY],
            ascending=[False],
            kind="mergesort",
        )
    return merged.reset_index(drop=True)


def resolve_output_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.suffix:
        return path
    return path.with_suffix(".csv")


def build_deduplicated_output_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}_deduplicated{path.suffix}")


def export_dataframe(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _resolve_dedupe_time_series(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
    for column in DEDUPE_TIME_CANDIDATE_COLUMNS:
        if column not in df.columns:
            continue
        parsed = pd.to_datetime(df[column], errors="coerce", utc=True)
        if parsed.notna().any():
            return column, parsed
    return None, None


def deduplicate_dataframe(
    df: pd.DataFrame,
    dedupe_keys: list[str],
    keep_keys: list[str] | None = None,
) -> tuple[pd.DataFrame, str | None]:
    if df.empty:
        result = df.copy()
        if keep_keys:
            validate_columns(result, keep_keys, "保留键")
            result = result.loc[:, keep_keys]
        return result, None

    active_dedupe_keys = dedupe_keys or list(df.columns)
    validate_columns(df, active_dedupe_keys, "去重键")
    if keep_keys:
        validate_columns(df, keep_keys, "保留键")

    dedupe_time_column, dedupe_time_series = _resolve_dedupe_time_series(df)
    working = df.copy()
    working["_dedupe_original_order"] = range(len(working))
    if dedupe_time_series is None:
        working["_dedupe_time"] = pd.NaT
    else:
        working["_dedupe_time"] = dedupe_time_series

    sort_columns = [*active_dedupe_keys, "_dedupe_original_order"]
    ascending = [True] * len(active_dedupe_keys) + [True]
    if dedupe_time_column is not None:
        sort_columns.insert(len(active_dedupe_keys), "_dedupe_time")
        ascending.insert(len(active_dedupe_keys), False)

    deduplicated = (
        working.sort_values(by=sort_columns, ascending=ascending, kind="mergesort", na_position="last")
        .drop_duplicates(subset=active_dedupe_keys, keep="first")
        .sort_values(by="_dedupe_original_order", kind="mergesort")
        .drop(columns=["_dedupe_original_order", "_dedupe_time"])
    )

    if keep_keys:
        deduplicated = deduplicated.loc[:, keep_keys]

    return deduplicated.reset_index(drop=True), dedupe_time_column
