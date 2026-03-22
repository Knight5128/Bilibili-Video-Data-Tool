from __future__ import annotations

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import (
    BilibiliHotSource,
    BilibiliUserRecentVideoSource,
    BilibiliZoneTop10Source,
    VideoPoolBuilder,
)
from bili_pipeline.models import DiscoverResult


def build_real_result(tid: int = 17, lookback_days: int = 90) -> DiscoverResult:
    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=lookback_days, partition_tid_whitelist={tid}),
        hot_sources=[BilibiliHotSource(ps=5)],
        partition_sources=[BilibiliZoneTop10Source(tid=tid, day=7)],
        author_source=BilibiliUserRecentVideoSource(page_size=10, max_pages=5),
    )
    return builder.build()


def print_real_result(tid: int = 17, lookback_days: int = 90, limit: int = 20) -> None:
    result = build_real_result(tid=tid, lookback_days=lookback_days)
    for entry in result.entries[:limit]:
        print(
            entry.bvid,
            entry.source_type,
            entry.owner_mid,
            entry.tid,
            entry.pubdate.isoformat() if entry.pubdate else None,
            entry.source_refs,
        )


if __name__ == "__main__":
    print_real_result()
