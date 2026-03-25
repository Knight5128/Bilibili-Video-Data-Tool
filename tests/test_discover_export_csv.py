from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
import tempfile
import unittest

from bili_pipeline.discover.export_csv import (
    discover_entries_to_full_export_rows,
    export_rows_csv,
    rankboard_entries_to_rows,
)
from bili_pipeline.models import RankboardEntry, VideoPoolEntry


class DiscoverExportCsvTest(unittest.TestCase):
    def test_rankboard_rows_keep_duplicate_bvids(self) -> None:
        now = datetime(2026, 3, 25, 12, 0, 0)
        entries = [
            RankboardEntry(
                board_rid=0,
                board_name="全站",
                board_rank=1,
                bvid="BV_DUP",
                source_type="rankboard",
                source_ref="rankboard:rid=0:rank=1",
                discovered_at=now,
                owner_mid=101,
                tid=17,
                pubdate=now,
                duration_seconds=180,
                seed_score=123.0,
                source_refs=["rankboard:rid=0:rank=1"],
            ),
            RankboardEntry(
                board_rid=1005,
                board_name="动画",
                board_rank=2,
                bvid="BV_DUP",
                source_type="rankboard",
                source_ref="rankboard:rid=1005:rank=2",
                discovered_at=now,
                owner_mid=101,
                tid=17,
                pubdate=now,
                duration_seconds=180,
                seed_score=123.0,
                source_refs=["rankboard:rid=1005:rank=2"],
            ),
        ]

        rows = rankboard_entries_to_rows(entries)
        self.assertEqual(2, len(rows))
        self.assertEqual(["全站", "动画"], [row["board_name"] for row in rows])

        with tempfile.TemporaryDirectory() as tmp_dir:
            out_path = Path(tmp_dir) / "rankboard.csv"
            export_rows_csv(rows, out_path)
            with out_path.open("r", encoding="utf-8-sig", newline="") as f:
                saved_rows = list(csv.DictReader(f))

        self.assertEqual(2, len(saved_rows))
        self.assertEqual(["1", "2"], [row["board_rank"] for row in saved_rows])
        self.assertEqual(["BV_DUP", "BV_DUP"], [row["bvid"] for row in saved_rows])

    def test_discover_rows_can_be_normalized_for_full_export(self) -> None:
        now = datetime(2026, 3, 25, 12, 0, 0)
        rows = discover_entries_to_full_export_rows(
            [
                VideoPoolEntry(
                    bvid="BV1",
                    source_type="hot",
                    source_ref="hot:pn=1:ps=20",
                    discovered_at=now,
                    last_seen_at=now,
                    owner_mid=123,
                    tid=17,
                    pubdate=now,
                    duration_seconds=155,
                    seed_score=456.0,
                    source_refs=["hot:pn=1:ps=20"],
                )
            ]
        )

        self.assertEqual(1, len(rows))
        self.assertIsNone(rows[0]["board_rid"])
        self.assertIsNone(rows[0]["board_name"])
        self.assertIsNone(rows[0]["board_rank"])
        self.assertEqual("BV1", rows[0]["bvid"])


if __name__ == "__main__":
    unittest.main()
