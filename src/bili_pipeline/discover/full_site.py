from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.models import DiscoverResult

from .bilibili_sources import BilibiliHotSource, BilibiliWeeklyHotSource, BilibiliZoneRecentVideosSource
from .builder import VideoPoolBuilder


@dataclass(slots=True)
class _NoopAuthorVideoSource:
    def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list:
        return []


def load_valid_partition_tids(csv_path: str | Path) -> list[int]:
    csv_path = Path(csv_path)
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    else:
        raise ValueError(f"无法读取有效分区列表：{csv_path}") from last_error

    if "tid" not in df.columns:
        raise ValueError("all_valid_tags.csv 中缺少 tid 列。")

    tids: list[int] = []
    seen: set[int] = set()
    for value in pd.to_numeric(df["tid"], errors="coerce").dropna().astype(int):
        if value in seen:
            continue
        seen.add(value)
        tids.append(value)
    return tids


def build_full_site_result(
    lookback_days: int,
    valid_tids: list[int],
    logger: Callable[[str], None] | None = None,
    *,
    hot_page_size: int = 20,
    hot_max_pages: int = 20,
    partition_page_size: int = 30,
    partition_max_pages: int = 200,
) -> DiscoverResult:
    now = datetime.now()
    since = now - timedelta(days=lookback_days)
    weeks_to_fetch = lookback_days // 7 + 1
    candidates = []

    hot_source = BilibiliHotSource(ps=hot_page_size, fetch_all_pages=True, max_pages=hot_max_pages)
    _collect_source_candidates("全站热门榜单", hot_source, candidates, logger)

    for week in range(1, weeks_to_fetch + 1):
        _collect_source_candidates(f"第 {week} 期每周必看", BilibiliWeeklyHotSource(week=week), candidates, logger)

    for tid in valid_tids:
        _collect_source_candidates(
            f"分区 tid={tid}",
            BilibiliZoneRecentVideosSource(
                tid=tid,
                since=since,
                page_size=partition_page_size,
                max_pages=partition_max_pages,
            ),
            candidates,
            logger,
        )

    if logger is not None:
        logger(f"[INFO]: 种子抓取完成，共获得 {len(candidates)} 条候选视频，开始按 bvid 去重")

    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=lookback_days, enable_author_backfill=False),
        hot_sources=[],
        partition_sources=[],
        author_source=_NoopAuthorVideoSource(),
    )
    result = builder.build_from_seed_candidates(candidates, now=now)

    if logger is not None:
        logger(f"[INFO]: 去重完成，最终保留 {len(result.entries)} 条视频")
    return result


def _collect_source_candidates(
    label: str,
    source,
    collector: list,
    logger: Callable[[str], None] | None,
) -> None:
    if logger is not None:
        logger(f"[INFO]: 正在抓取{label}中的视频")
    fetched = source.fetch()
    collector.extend(fetched)
    if logger is not None:
        logger(f"[INFO]: {label}抓取完成，新增 {len(fetched)} 条候选视频")
