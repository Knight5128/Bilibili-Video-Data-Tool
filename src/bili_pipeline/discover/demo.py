from __future__ import annotations

from datetime import datetime, timedelta

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import VideoPoolBuilder
from bili_pipeline.discover.mock_sources import StaticAuthorVideoSource, StaticSeedSource
from bili_pipeline.models import CandidateVideo


def build_demo_result() -> None:
    now = datetime(2026, 3, 11, 12, 0, 0)
    hot_source = StaticSeedSource(
        items=[
            CandidateVideo(
                bvid="BV_hot_001",
                source_type="hot",
                source_ref="popular",
                discovered_at=now,
                owner_mid=1001,
                tid=17,
                pubdate=now - timedelta(days=2),
                duration_seconds=180,
                seed_score=99.0,
            )
        ]
    )
    partition_source = StaticSeedSource(
        items=[
            CandidateVideo(
                bvid="BV_part_001",
                source_type="partition",
                source_ref="tid:17",
                discovered_at=now,
                owner_mid=2001,
                tid=17,
                pubdate=now - timedelta(days=1),
                duration_seconds=240,
                seed_score=60.0,
            )
        ]
    )
    author_source = StaticAuthorVideoSource(
        mapping={
            1001: [
                CandidateVideo(
                    bvid="BV_author_001",
                    source_type="author_expand",
                    source_ref="owner:1001",
                    discovered_at=now,
                    owner_mid=1001,
                    tid=17,
                    pubdate=now - timedelta(days=30),
                    duration_seconds=120,
                ),
                CandidateVideo(
                    bvid="BV_old_ignored",
                    source_type="author_expand",
                    source_ref="owner:1001",
                    discovered_at=now,
                    owner_mid=1001,
                    tid=17,
                    pubdate=now - timedelta(days=150),
                    duration_seconds=120,
                ),
            ],
            2001: [
                CandidateVideo(
                    bvid="BV_author_002",
                    source_type="author_expand",
                    source_ref="owner:2001",
                    discovered_at=now,
                    owner_mid=2001,
                    tid=24,
                    pubdate=now - timedelta(days=7),
                    duration_seconds=260,
                )
            ],
        }
    )

    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=90, partition_tid_whitelist={17}),
        hot_sources=[hot_source],
        partition_sources=[partition_source],
        author_source=author_source,
    )
    result = builder.build(now=now)
    for entry in result.entries:
        print(
            entry.bvid,
            entry.source_type,
            entry.owner_mid,
            entry.tid,
            entry.source_refs,
        )


if __name__ == "__main__":
    build_demo_result()
