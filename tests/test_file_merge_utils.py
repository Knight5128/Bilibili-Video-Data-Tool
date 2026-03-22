from __future__ import annotations

import unittest

import pandas as pd

from bili_pipeline.utils.file_merge import deduplicate_dataframe, merge_dataframes


class FileMergeUtilsTest(unittest.TestCase):
    def test_merge_uses_default_bvid_desc_when_sort_keys_empty(self) -> None:
        merged = merge_dataframes(
            [
                pd.DataFrame([{"bvid": "BV1"}, {"bvid": "BV3"}]),
                pd.DataFrame([{"bvid": "BV2"}]),
            ],
            sort_keys=[],
        )
        self.assertEqual(["BV3", "BV2", "BV1"], merged["bvid"].tolist())

    def test_deduplicate_prefers_latest_time_and_keeps_first_on_equal_time(self) -> None:
        df = pd.DataFrame(
            [
                {"bvid": "BV1", "title": "older", "last_seen_at": "2026-03-10T10:00:00"},
                {"bvid": "BV1", "title": "newer", "last_seen_at": "2026-03-11T10:00:00"},
                {"bvid": "BV2", "title": "first-tie", "last_seen_at": "2026-03-12T10:00:00"},
                {"bvid": "BV2", "title": "second-tie", "last_seen_at": "2026-03-12T10:00:00"},
            ]
        )

        deduplicated, dedupe_time_column = deduplicate_dataframe(
            df,
            dedupe_keys=["bvid"],
            keep_keys=["bvid", "title"],
        )

        self.assertEqual("last_seen_at", dedupe_time_column)
        self.assertEqual(
            [
                {"bvid": "BV1", "title": "newer"},
                {"bvid": "BV2", "title": "first-tie"},
            ],
            deduplicated.to_dict("records"),
        )


if __name__ == "__main__":
    unittest.main()
