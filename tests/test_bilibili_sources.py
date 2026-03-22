from __future__ import annotations

from datetime import datetime
import unittest
from unittest.mock import AsyncMock, patch

from bili_pipeline.discover.bilibili_sources import (
    BilibiliHotSource,
    BilibiliUserRecentVideoSource,
    BilibiliWeeklyHotSource,
    BilibiliZoneRecentVideosSource,
    BilibiliZoneTop10Source,
    resolve_owner_mids_from_bvids,
)


class BilibiliSourceAdaptersTest(unittest.TestCase):
    @patch("bili_pipeline.discover.bilibili_sources.hot.get_hot_videos", new_callable=AsyncMock)
    def test_hot_source_parses_candidates(self, mock_get_hot_videos: AsyncMock) -> None:
        mock_get_hot_videos.return_value = {
            "list": [
                {
                    "bvid": "BV_hot",
                    "tid": 17,
                    "pubdate": 1773203400,
                    "duration": 155,
                    "owner": {"mid": 123},
                    "stat": {"view": 100, "like": 10, "reply": 3, "coin": 5},
                }
            ]
        }

        items = BilibiliHotSource(ps=1).fetch()
        self.assertEqual(1, len(items))
        self.assertEqual("BV_hot", items[0].bvid)
        self.assertEqual("hot", items[0].source_type)
        self.assertEqual(123, items[0].owner_mid)

    @patch("bili_pipeline.discover.bilibili_sources.hot.get_hot_videos", new_callable=AsyncMock)
    def test_hot_source_can_fetch_all_pages(self, mock_get_hot_videos: AsyncMock) -> None:
        mock_get_hot_videos.side_effect = [
            {
                "list": [
                    {
                        "bvid": "BV_hot_1",
                        "tid": 17,
                        "pubdate": 1773203400,
                        "duration": 155,
                        "owner": {"mid": 123},
                        "stat": {"view": 100, "like": 10, "reply": 3, "coin": 5},
                    }
                ],
                "no_more": False,
            },
            {
                "list": [
                    {
                        "bvid": "BV_hot_2",
                        "tid": 18,
                        "pubdate": 1773203400,
                        "duration": 166,
                        "owner": {"mid": 456},
                        "stat": {"view": 200, "like": 20, "reply": 4, "coin": 6},
                    }
                ],
                "no_more": True,
            },
        ]

        items = BilibiliHotSource(ps=1, fetch_all_pages=True, max_pages=5).fetch()
        self.assertEqual(["BV_hot_1", "BV_hot_2"], [item.bvid for item in items])
        self.assertEqual(2, mock_get_hot_videos.await_count)

    @patch("bili_pipeline.discover.bilibili_sources.hot.get_weekly_hot_videos", new_callable=AsyncMock)
    def test_weekly_hot_source_parses_candidates(self, mock_get_weekly_hot_videos: AsyncMock) -> None:
        mock_get_weekly_hot_videos.return_value = {
            "list": [
                {
                    "bvid": "BV_weekly",
                    "tid": 17,
                    "pubdate": 1773203400,
                    "duration": 321,
                    "owner": {"mid": 555},
                    "stat": {"view": 300, "like": 30, "reply": 5, "coin": 7},
                }
            ]
        }

        items = BilibiliWeeklyHotSource(week=3).fetch()
        self.assertEqual(1, len(items))
        self.assertEqual("weekly_hot", items[0].source_type)
        self.assertEqual(555, items[0].owner_mid)

    @patch("bili_pipeline.discover.bilibili_sources.video_zone.get_zone_top10", new_callable=AsyncMock)
    def test_zone_source_parses_candidates(self, mock_get_zone_top10: AsyncMock) -> None:
        mock_get_zone_top10.return_value = [
            {
                "bvid": "BV_zone",
                "typeid": 17,
                "created": 1773203400,
                "length": "02:35",
                "mid": 456,
                "play": 200,
                "favorites": 20,
            }
        ]

        items = BilibiliZoneTop10Source(tid=17).fetch()
        self.assertEqual(1, len(items))
        self.assertEqual("partition", items[0].source_type)
        self.assertEqual(155, items[0].duration_seconds)

    @patch("bili_pipeline.discover.bilibili_sources.video_zone.get_zone_new_videos", new_callable=AsyncMock)
    def test_zone_recent_source_stops_after_lookback(self, mock_get_zone_new_videos: AsyncMock) -> None:
        mock_get_zone_new_videos.side_effect = [
            {
                "archives": [
                    {
                        "bvid": "BV_recent_1",
                        "tid": 17,
                        "pubdate": 1773203400,
                        "duration": 155,
                        "owner": {"mid": 123},
                        "stat": {"view": 100, "like": 10, "reply": 3, "coin": 5},
                    },
                    {
                        "bvid": "BV_old_1",
                        "tid": 17,
                        "pubdate": 1760000000,
                        "duration": 160,
                        "owner": {"mid": 123},
                        "stat": {"view": 50, "like": 5, "reply": 1, "coin": 1},
                    },
                ]
            }
        ]

        items = BilibiliZoneRecentVideosSource(
            tid=17,
            since=datetime.fromtimestamp(1765000000),
            page_size=2,
            max_pages=3,
        ).fetch()
        self.assertEqual(["BV_recent_1"], [item.bvid for item in items])

    @patch("bili_pipeline.discover.bilibili_sources.User.get_videos", new_callable=AsyncMock)
    def test_user_recent_videos_stops_after_lookback(self, mock_get_videos: AsyncMock) -> None:
        mock_get_videos.side_effect = [
            {
                "list": {
                    "vlist": [
                        {
                            "bvid": "BV_recent",
                            "typeid": 17,
                            "created": 1771000000,
                            "length": "10:00",
                            "mid": 789,
                            "play": 1000,
                            "favorites": 100,
                        },
                        {
                            "bvid": "BV_old",
                            "typeid": 17,
                            "created": 1760000000,
                            "length": "09:00",
                            "mid": 789,
                            "play": 100,
                            "favorites": 10,
                        },
                    ]
                }
            }
        ]

        items = BilibiliUserRecentVideoSource(page_size=2, max_pages=3).fetch_recent_videos(
            owner_mid=789,
            since=datetime.fromtimestamp(1765000000),
        )
        self.assertEqual(["BV_recent"], [item.bvid for item in items])

    @patch("bili_pipeline.discover.bilibili_sources.Video.get_info", new_callable=AsyncMock)
    def test_resolve_owner_mids_from_bvids_collects_unique_authors(
        self,
        mock_get_info: AsyncMock,
    ) -> None:
        mock_get_info.side_effect = [
            {"owner": {"mid": 123}},
            {"owner": {"mid": 456}},
            RuntimeError("boom"),
            {"owner": {}},
        ]

        owner_mids, failed_bvids = resolve_owner_mids_from_bvids(
            ["BV1xx411c7mD", "BV1xx411c7mE", "BV1xx411c7mF", "BV1xx411c7mG"]
        )

        self.assertEqual([123, 456], owner_mids)
        self.assertEqual(["BV1xx411c7mF", "BV1xx411c7mG"], failed_bvids)

    @patch("bili_pipeline.discover.bilibili_sources.Video.get_info", new_callable=AsyncMock)
    def test_resolve_owner_mids_from_bvids_retries_412_once(
        self,
        mock_get_info: AsyncMock,
    ) -> None:
        mock_get_info.side_effect = [
            RuntimeError("网络错误，状态码：412"),
            {"owner": {"mid": 123}},
        ]

        owner_mids, failed_bvids = resolve_owner_mids_from_bvids(
            ["BV1xx411c7mD"],
            max_retries=1,
            retry_backoff_seconds=0,
        )

        self.assertEqual([123], owner_mids)
        self.assertEqual([], failed_bvids)
        self.assertEqual(2, mock_get_info.await_count)


if __name__ == "__main__":
    unittest.main()
