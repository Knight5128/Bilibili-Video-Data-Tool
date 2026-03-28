from __future__ import annotations

import csv
import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_auth = types.ModuleType("google.auth")
    google_auth.default = lambda scopes=None: (None, "stub-project")
    google_cloud = types.ModuleType("google.cloud")
    google_bigquery = types.ModuleType("google.cloud.bigquery")
    google_storage = types.ModuleType("google.cloud.storage")
    google_oauth2 = types.ModuleType("google.oauth2")
    google_service_account = types.ModuleType("google.oauth2.service_account")

    class _Dummy:
        def __init__(self, *args, **kwargs) -> None:
            return None

    google_bigquery.Client = _Dummy
    google_bigquery.DatasetReference = _Dummy
    google_bigquery.Dataset = _Dummy
    google_bigquery.Table = _Dummy
    google_bigquery.QueryJobConfig = _Dummy
    google_bigquery.ScalarQueryParameter = _Dummy
    google_bigquery.ArrayQueryParameter = _Dummy
    google_bigquery.SchemaField = _Dummy
    google_storage.Client = _Dummy

    class _Credentials:
        project_id = "stub-project"

        @classmethod
        def from_service_account_file(cls, *_args, **_kwargs):
            return cls()

    google_service_account.Credentials = _Credentials

    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.auth", google_auth)
    sys.modules.setdefault("google.cloud", google_cloud)
    sys.modules.setdefault("google.cloud.bigquery", google_bigquery)
    sys.modules.setdefault("google.cloud.storage", google_storage)
    sys.modules.setdefault("google.oauth2", google_oauth2)
    sys.modules.setdefault("google.oauth2.service_account", google_service_account)
    google_module.auth = google_auth
    google_module.cloud = google_cloud
    google_module.oauth2 = google_oauth2
    google_cloud.bigquery = google_bigquery
    google_cloud.storage = google_storage
    google_oauth2.service_account = google_service_account


_install_google_stubs()

from bili_pipeline.datahub.manual_batch_runner import discover_manual_batch_source_csvs, run_manual_realtime_batch_crawl
from bili_pipeline.models import CrawlTaskMode, GCPStorageConfig


class ManualBatchRunnerTest(unittest.TestCase):
    def test_discover_manual_batch_source_csvs_matches_expected_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            full_site_floorings_dir = root / "full_site_floorings"
            uid_expansions_dir = root / "uid_expansions" / "uid_expansion_20260315_20260329_120000"
            full_site_floorings_dir.mkdir(parents=True)
            uid_expansions_dir.mkdir(parents=True)

            (full_site_floorings_dir / "daily_hot.csv").write_text("bvid\nBV1\n", encoding="utf-8-sig")
            (full_site_floorings_dir / "daily_hot_authors.csv").write_text("owner_mid\n1\n", encoding="utf-8-sig")
            (uid_expansions_dir / "videolist_part_1.csv").write_text("bvid\nBV2\n", encoding="utf-8-sig")
            (uid_expansions_dir / "remaining_uids_part_1.csv").write_text("owner_mid\n2\n", encoding="utf-8-sig")

            source_paths = discover_manual_batch_source_csvs(root)

            self.assertEqual(
                ["full_site_floorings/daily_hot.csv", "uid_expansions/uid_expansion_20260315_20260329_120000/videolist_part_1.csv"],
                [path.relative_to(root).as_posix() for path in source_paths],
            )

    @patch("bili_pipeline.datahub.manual_batch_runner.crawl_bvid_list_from_csv")
    def test_run_manual_realtime_batch_crawl_filters_by_pubdate_window(self, mock_batch: Mock) -> None:
        mock_batch.return_value = SimpleNamespace(
            success_count=2,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            to_dict=lambda: {
                "success_count": 2,
                "failed_count": 0,
                "remaining_count": 0,
                "completed_all": True,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            full_site_floorings_dir = root / "full_site_floorings"
            uid_expansions_dir = root / "uid_expansions" / "uid_expansion_20260315_20260329_120000"
            full_site_floorings_dir.mkdir(parents=True)
            uid_expansions_dir.mkdir(parents=True)

            with (full_site_floorings_dir / "daily_hot.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_RECENT", "pubdate": "2026-03-28T12:00:00", "title": "recent"})
                writer.writerow({"bvid": "BV_OLD", "pubdate": "2026-03-01T12:00:00", "title": "old"})
                writer.writerow({"bvid": "BV_NO_DATE", "pubdate": "", "title": "missing"})

            with (uid_expansions_dir / "videolist_part_1.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_RECENT", "pubdate": "2026-03-28T12:00:00", "title": "recent-duplicate"})
                writer.writerow({"bvid": "BV_RECENT2", "pubdate": "2026-03-27T13:00:00", "title": "recent-2"})

            result = run_manual_realtime_batch_crawl(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                stream_data_time_window_hours=48,
                parallelism=2,
                comment_limit=10,
                consecutive_failure_limit=10,
                video_pool_root=root,
                manual_crawls_root_dir=manual_root,
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual(2, result.filtered_bvid_count)
            self.assertEqual(1, result.skipped_missing_pubdate_count)
            self.assertEqual(1, result.skipped_outside_window_count)

            filtered_csv = Path(result.filtered_csv_path or "")
            self.assertTrue(filtered_csv.exists())
            filtered_bvids = [row["bvid"] for row in csv.DictReader(filtered_csv.open("r", encoding="utf-8-sig", newline=""))]
            self.assertEqual(["BV_RECENT", "BV_RECENT2"], filtered_bvids)

            _, kwargs = mock_batch.call_args
            self.assertEqual(CrawlTaskMode.REALTIME_ONLY, kwargs["task_mode"])
            self.assertFalse(kwargs["enable_media"])
            self.assertEqual(filtered_csv.name, kwargs["source_csv_name"])
            self.assertEqual(filtered_csv.parent, Path(kwargs["session_dir"]))


if __name__ == "__main__":
    unittest.main()
