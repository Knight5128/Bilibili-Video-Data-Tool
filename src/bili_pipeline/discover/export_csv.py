from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from bili_pipeline.models import DiscoverResult, VideoPoolEntry


def _to_iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def discover_entries_to_rows(entries: Iterable[VideoPoolEntry]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for e in entries:
        row = asdict(e)
        row["discovered_at"] = _to_iso(e.discovered_at)
        row["last_seen_at"] = _to_iso(e.last_seen_at)
        row["pubdate"] = _to_iso(e.pubdate)
        row["source_refs"] = ",".join(e.source_refs)
        rows.append(row)
    return rows


def export_discover_result_csv(result: DiscoverResult, csv_path: str | Path) -> Path:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = discover_entries_to_rows(result.entries)
    fieldnames: list[str] = []
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = [
            "bvid",
            "source_type",
            "source_ref",
            "discovered_at",
            "last_seen_at",
            "owner_mid",
            "tid",
            "pubdate",
            "duration_seconds",
            "seed_score",
            "source_refs",
        ]

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return path

