from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import VideoPoolBuilder
from bili_pipeline.discover.mock_sources import StaticAuthorVideoSource, StaticSeedSource
from bili_pipeline.models import CandidateVideo


class VideoPoolBuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 3, 11, 12, 0, 0)

    def test_build_merges_seed_and_recent_author_videos(self) -> None:
        hot = StaticSeedSource(
            [
                CandidateVideo(
                    bvid="BV1",
                    source_type="hot",
                    source_ref="popular",
                    discovered_at=self.now,
                    owner_mid=101,
                    tid=17,
                    pubdate=self.now - timedelta(days=1),
                    duration_seconds=200,
                )
            ]
        )
        partition = StaticSeedSource(
            [
                CandidateVideo(
                    bvid="BV2",
                    source_type="partition",
                    source_ref="tid:17",
                    discovered_at=self.now,
                    owner_mid=202,
                    tid=17,
                    pubdate=self.now - timedelta(days=1),
                    duration_seconds=210,
                )
            ]
        )
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV3",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=30),
                        duration_seconds=180,
                    ),
                    CandidateVideo(
                        bvid="BV4",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=120),
                        duration_seconds=180,
                    ),
                ]
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(lookback_days=90, partition_tid_whitelist={17}),
            hot_sources=[hot],
            partition_sources=[partition],
            author_source=author_source,
        )

        result = builder.build(now=self.now)
        self.assertEqual([101, 202], result.owner_mids)
        self.assertEqual(["BV1", "BV2", "BV3"], [entry.bvid for entry in result.entries])

    def test_seed_priority_beats_author_expand(self) -> None:
        hot = StaticSeedSource(
            [
                CandidateVideo(
                    bvid="BV1",
                    source_type="hot",
                    source_ref="popular",
                    discovered_at=self.now,
                    owner_mid=101,
                    tid=17,
                    pubdate=self.now - timedelta(days=1),
                    duration_seconds=200,
                )
            ]
        )
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV1",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=2),
                        duration_seconds=200,
                    )
                ]
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(lookback_days=90, partition_tid_whitelist={17}),
            hot_sources=[hot],
            partition_sources=[],
            author_source=author_source,
        )

        result = builder.build(now=self.now)
        self.assertEqual(1, len(result.entries))
        self.assertEqual("hot", result.entries[0].source_type)
        self.assertEqual(["popular", "owner:101"], result.entries[0].source_refs)

    def test_build_from_owner_mids_fetches_recent_author_videos(self) -> None:
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV_author_1",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=5),
                        duration_seconds=120,
                    )
                ],
                202: [
                    CandidateVideo(
                        bvid="BV_author_2",
                        source_type="author_expand",
                        source_ref="owner:202",
                        discovered_at=self.now,
                        owner_mid=202,
                        tid=33,
                        pubdate=self.now - timedelta(days=2),
                        duration_seconds=240,
                    )
                ],
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(lookback_days=90),
            hot_sources=[],
            partition_sources=[],
            author_source=author_source,
        )

        result = builder.build_from_owner_mids([202, 101, 202], now=self.now)
        self.assertEqual([101, 202], result.owner_mids)
        self.assertEqual(["BV_author_1", "BV_author_2"], [entry.bvid for entry in result.entries])

    def test_build_from_owner_mids_respects_start_and_end_date(self) -> None:
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV_before_window",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=40),
                        duration_seconds=120,
                    ),
                    CandidateVideo(
                        bvid="BV_inside_window",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=10),
                        duration_seconds=120,
                    ),
                    CandidateVideo(
                        bvid="BV_after_window",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now + timedelta(days=1),
                        duration_seconds=120,
                    ),
                ]
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(
                start_date=self.now - timedelta(days=20),
                end_date=self.now,
            ),
            hot_sources=[],
            partition_sources=[],
            author_source=author_source,
        )

        result = builder.build_from_owner_mids([101], now=self.now)
        self.assertEqual(["BV_inside_window"], [entry.bvid for entry in result.entries])

    def test_build_from_owner_mids_supports_owner_since_overrides(self) -> None:
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV_old_overlap",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=12),
                        duration_seconds=120,
                    ),
                    CandidateVideo(
                        bvid="BV_new_incremental",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=3),
                        duration_seconds=120,
                    ),
                ],
                202: [
                    CandidateVideo(
                        bvid="BV_owner_202",
                        source_type="author_expand",
                        source_ref="owner:202",
                        discovered_at=self.now,
                        owner_mid=202,
                        tid=33,
                        pubdate=self.now - timedelta(days=8),
                        duration_seconds=120,
                    )
                ],
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(
                start_date=self.now - timedelta(days=20),
                end_date=self.now,
            ),
            hot_sources=[],
            partition_sources=[],
            author_source=author_source,
        )

        result = builder.build_from_owner_mids(
            [101, 202],
            now=self.now,
            owner_since_overrides={101: self.now - timedelta(days=5)},
        )

        self.assertEqual(
            ["BV_new_incremental", "BV_owner_202"],
            [entry.bvid for entry in result.entries],
        )

    def test_build_from_owner_mids_skips_failed_author_and_reports_callback(self) -> None:
        now = self.now

        class PartiallyFailingAuthorVideoSource:
            def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list[CandidateVideo]:
                if owner_mid == 202:
                    raise RuntimeError("网络错误，状态码：412")
                return [
                    CandidateVideo(
                        bvid="BV_author_ok",
                        source_type="author_expand",
                        source_ref=f"owner:{owner_mid}",
                        discovered_at=now,
                        owner_mid=owner_mid,
                        tid=17,
                        pubdate=now - timedelta(days=3),
                        duration_seconds=180,
                    )
                ]

        failed_owner_mids: list[int] = []
        builder = VideoPoolBuilder(
            config=DiscoverConfig(lookback_days=90),
            hot_sources=[],
            partition_sources=[],
            author_source=PartiallyFailingAuthorVideoSource(),
        )

        result = builder.build_from_owner_mids(
            [101, 202],
            now=self.now,
            error_callback=lambda owner_mid, *_: failed_owner_mids.append(owner_mid),
        )

        self.assertEqual([101, 202], result.owner_mids)
        self.assertEqual(["BV_author_ok"], [entry.bvid for entry in result.entries])
        self.assertEqual([202], failed_owner_mids)

    def test_build_from_seed_candidates_can_disable_author_backfill(self) -> None:
        hot = StaticSeedSource(
            [
                CandidateVideo(
                    bvid="BV_seed_1",
                    source_type="hot",
                    source_ref="popular",
                    discovered_at=self.now,
                    owner_mid=101,
                    tid=17,
                    pubdate=self.now - timedelta(days=2),
                    duration_seconds=180,
                ),
                CandidateVideo(
                    bvid="BV_seed_1",
                    source_type="weekly_hot",
                    source_ref="weekly_hot:week=1",
                    discovered_at=self.now,
                    owner_mid=101,
                    tid=17,
                    pubdate=self.now - timedelta(days=2),
                    duration_seconds=180,
                ),
            ]
        )
        author_source = StaticAuthorVideoSource(
            {
                101: [
                    CandidateVideo(
                        bvid="BV_author_extra",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=self.now,
                        owner_mid=101,
                        tid=17,
                        pubdate=self.now - timedelta(days=5),
                        duration_seconds=120,
                    )
                ]
            }
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(lookback_days=90, enable_author_backfill=False),
            hot_sources=[hot],
            partition_sources=[],
            author_source=author_source,
        )

        result = builder.build_from_seed_candidates(hot.fetch(), now=self.now)
        self.assertEqual([101], result.owner_mids)
        self.assertEqual(["BV_seed_1"], [entry.bvid for entry in result.entries])
        self.assertEqual(["popular", "weekly_hot:week=1"], result.entries[0].source_refs)
        self.assertEqual("weekly_hot", result.entries[0].source_type)

    def test_build_from_seed_candidates_accepts_aware_pubdate_with_naive_window(self) -> None:
        aware_pubdate = datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
        hot = StaticSeedSource(
            [
                CandidateVideo(
                    bvid="BV_aware_seed",
                    source_type="hot",
                    source_ref="popular",
                    discovered_at=aware_pubdate,
                    owner_mid=101,
                    tid=17,
                    pubdate=aware_pubdate,
                    duration_seconds=180,
                )
            ]
        )

        builder = VideoPoolBuilder(
            config=DiscoverConfig(
                start_date=datetime(2026, 3, 1, 0, 0, 0),
                end_date=datetime(2026, 3, 11, 12, 0, 0),
                enable_author_backfill=False,
            ),
            hot_sources=[hot],
            partition_sources=[],
            author_source=StaticAuthorVideoSource({}),
        )

        result = builder.build_from_seed_candidates(hot.fetch(), now=self.now)

        self.assertEqual(["BV_aware_seed"], [entry.bvid for entry in result.entries])


if __name__ == "__main__":
    unittest.main()
