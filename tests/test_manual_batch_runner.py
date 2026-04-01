from __future__ import annotations

import csv
import os
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
    def test_discover_manual_batch_source_csvs_matches_explicit_selected_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            full_site_floorings_dir = root / "full_site_floorings"
            uid_expansions_dir = root / "uid_expansions" / "uid_expansion_20260315_20260329_120000"
            full_site_floorings_dir.mkdir(parents=True)
            uid_expansions_dir.mkdir(parents=True)

            (full_site_floorings_dir / "daily_hot-20260329_120000.csv").write_text("bvid\nBV1\n", encoding="utf-8-sig")
            (full_site_floorings_dir / "rankboard-20260329_120000.csv").write_text("bvid\nBV3\n", encoding="utf-8-sig")
            (full_site_floorings_dir / "weekly_pick-20260329_120000.csv").write_text("bvid\nBV4\n", encoding="utf-8-sig")
            (full_site_floorings_dir / "daily_hot_authors.csv").write_text("owner_mid\n1\n", encoding="utf-8-sig")
            (uid_expansions_dir / "videolist_part_1.csv").write_text("bvid\nBV2\n", encoding="utf-8-sig")
            (uid_expansions_dir / "remaining_uids_part_1.csv").write_text("owner_mid\n2\n", encoding="utf-8-sig")

            source_paths = discover_manual_batch_source_csvs(
                root,
                selected_flooring_csvs=["weekly_pick-20260329_120000.csv", "rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_20260315_20260329_120000"],
            )

            self.assertEqual(
                [
                    "full_site_floorings/weekly_pick-20260329_120000.csv",
                    "full_site_floorings/rankboard-20260329_120000.csv",
                    "uid_expansions/uid_expansion_20260315_20260329_120000/videolist_part_1.csv",
                ],
                [path.relative_to(root).as_posix() for path in source_paths],
            )

    def test_discover_manual_batch_source_csvs_uses_selected_uid_directories_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            full_site_floorings_dir = root / "full_site_floorings"
            uid_root = root / "uid_expansions"
            selected_dir = uid_root / "uid_expansion_selected"
            ignored_dir = uid_root / "uid_expansion_ignored"
            full_site_floorings_dir.mkdir(parents=True)
            selected_dir.mkdir(parents=True)
            ignored_dir.mkdir(parents=True)

            (full_site_floorings_dir / "daily_hot-20260329_120000.csv").write_text("bvid\nBVH\n", encoding="utf-8-sig")
            (full_site_floorings_dir / "rankboard-20260329_120000.csv").write_text("bvid\nBVR\n", encoding="utf-8-sig")
            (selected_dir / "videolist_part_1.csv").write_text("bvid\nBV_SELECTED\n", encoding="utf-8-sig")
            (ignored_dir / "videolist_part_1.csv").write_text("bvid\nBV_IGNORED\n", encoding="utf-8-sig")

            source_paths = discover_manual_batch_source_csvs(
                root,
                selected_flooring_csvs=["rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_selected"],
            )

            self.assertEqual(
                [
                    "full_site_floorings/rankboard-20260329_120000.csv",
                    "uid_expansions/uid_expansion_selected/videolist_part_1.csv",
                ],
                [path.relative_to(root).as_posix() for path in source_paths],
            )

    def test_discover_manual_batch_source_csvs_uses_latest_flooring_files_in_legacy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            full_site_floorings_dir = root / "full_site_floorings"
            uid_dir = root / "uid_expansions" / "uid_expansion_selected"
            full_site_floorings_dir.mkdir(parents=True)
            uid_dir.mkdir(parents=True)

            older_hot = full_site_floorings_dir / "daily_hot-20260328_120000.csv"
            newer_hot = full_site_floorings_dir / "daily_hot-20260329_120000.csv"
            older_rank = full_site_floorings_dir / "rankboard-20260328_120000.csv"
            newer_rank = full_site_floorings_dir / "rankboard-20260329_120000.csv"
            for path in (older_hot, newer_hot, older_rank, newer_rank):
                path.write_text("bvid\nBVX\n", encoding="utf-8-sig")
            older_hot.touch()
            older_rank.touch()
            newer_hot.touch()
            newer_rank.touch()
            (uid_dir / "videolist_part_1.csv").write_text("bvid\nBV_UID\n", encoding="utf-8-sig")

            source_paths = discover_manual_batch_source_csvs(
                root,
                selected_uid_task_dirs=["uid_expansion_selected"],
            )

            self.assertEqual(
                [
                    "full_site_floorings/daily_hot-20260329_120000.csv",
                    "full_site_floorings/rankboard-20260329_120000.csv",
                    "uid_expansions/uid_expansion_selected/videolist_part_1.csv",
                ],
                [path.relative_to(root).as_posix() for path in source_paths],
            )

    def test_discover_manual_batch_source_csvs_breaks_flooring_ties_by_desc_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            full_site_floorings_dir = root / "full_site_floorings"
            uid_dir = root / "uid_expansions" / "uid_expansion_selected"
            full_site_floorings_dir.mkdir(parents=True)
            uid_dir.mkdir(parents=True)

            hot_a = full_site_floorings_dir / "daily_hot-20260329_110000.csv"
            hot_b = full_site_floorings_dir / "daily_hot-20260329_120000.csv"
            rank_a = full_site_floorings_dir / "rankboard-20260329_110000.csv"
            rank_b = full_site_floorings_dir / "rankboard-20260329_120000.csv"
            for path in (hot_a, hot_b, rank_a, rank_b):
                path.write_text("bvid\nBVX\n", encoding="utf-8-sig")
                path.touch()
            shared_ts = hot_a.stat().st_mtime
            for path in (hot_a, hot_b, rank_a, rank_b):
                os.utime(path, (shared_ts, shared_ts))
            uid_file = uid_dir / "videolist_part_1.csv"
            uid_file.write_text("bvid\nBV_UID\n", encoding="utf-8-sig")

            source_paths = discover_manual_batch_source_csvs(
                root,
                selected_uid_task_dirs=["uid_expansion_selected"],
            )

            self.assertEqual(
                [
                    "full_site_floorings/daily_hot-20260329_120000.csv",
                    "full_site_floorings/rankboard-20260329_120000.csv",
                    "uid_expansions/uid_expansion_selected/videolist_part_1.csv",
                ],
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

            with (full_site_floorings_dir / "daily_hot-20260329_120000.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_RECENT", "pubdate": "2026-03-28T12:00:00", "title": "recent"})
                writer.writerow({"bvid": "BV_OLD", "pubdate": "2026-03-01T12:00:00", "title": "old"})
                writer.writerow({"bvid": "BV_NO_DATE", "pubdate": "", "title": "missing"})
            with (full_site_floorings_dir / "rankboard-20260329_120000.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_RANK", "pubdate": "2026-03-28T08:00:00", "title": "rank"})

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
                selected_flooring_csvs=["daily_hot-20260329_120000.csv", "rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_20260315_20260329_120000"],
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual(3, result.filtered_bvid_count)
            self.assertEqual(1, result.skipped_missing_pubdate_count)
            self.assertEqual(1, result.skipped_outside_window_count)

            filtered_csv = Path(result.filtered_csv_path or "")
            self.assertTrue(filtered_csv.exists())
            with filtered_csv.open("r", encoding="utf-8-sig", newline="") as handle:
                filtered_bvids = [row["bvid"] for row in csv.DictReader(handle)]
            self.assertEqual(["BV_RECENT", "BV_RANK", "BV_RECENT2"], filtered_bvids)

            _, kwargs = mock_batch.call_args
            self.assertEqual(CrawlTaskMode.REALTIME_ONLY, kwargs["task_mode"])
            self.assertFalse(kwargs["enable_media"])
            self.assertEqual(filtered_csv.name, kwargs["source_csv_name"])
            self.assertEqual(filtered_csv.parent, Path(kwargs["session_dir"]))

    @patch("bili_pipeline.datahub.manual_batch_runner.crawl_bvid_list_from_csv")
    def test_run_manual_realtime_batch_crawl_prefers_uid_expansions_on_equal_pubdate(self, mock_batch: Mock) -> None:
        mock_batch.return_value = SimpleNamespace(
            success_count=1,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            to_dict=lambda: {
                "success_count": 1,
                "failed_count": 0,
                "remaining_count": 0,
                "completed_all": True,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            floorings_dir = root / "full_site_floorings"
            uid_dir = root / "uid_expansions" / "uid_expansion_selected"
            floorings_dir.mkdir(parents=True)
            uid_dir.mkdir(parents=True)

            same_pubdate = "2026-03-28T12:00:00"
            with (floorings_dir / "daily_hot-20260329_120000.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title", "source_tag"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_DUP", "pubdate": same_pubdate, "title": "hot", "source_tag": "hot"})
            with (floorings_dir / "rankboard-20260329_120000.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title", "source_tag"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_DUP", "pubdate": same_pubdate, "title": "rank", "source_tag": "rank"})
            with (uid_dir / "videolist_part_1.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid", "pubdate", "title", "source_tag"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_DUP", "pubdate": same_pubdate, "title": "uid", "source_tag": "uid"})

            result = run_manual_realtime_batch_crawl(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                stream_data_time_window_hours=48,
                parallelism=1,
                comment_limit=10,
                consecutive_failure_limit=10,
                video_pool_root=root,
                manual_crawls_root_dir=manual_root,
                selected_flooring_csvs=["daily_hot-20260329_120000.csv", "rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_selected"],
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            filtered_csv = Path(result.filtered_csv_path or "")
            with filtered_csv.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(1, len(rows))
            self.assertEqual("uid", rows[0]["source_tag"])
            self.assertIn("uid_expansions", rows[0]["source_group"])

    @patch("bili_pipeline.datahub.manual_batch_runner.crawl_bvid_list_from_csv")
    def test_run_manual_realtime_batch_crawl_uses_stat_comment_session_prefix(self, mock_batch: Mock) -> None:
        mock_batch.return_value = SimpleNamespace(
            success_count=1,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            to_dict=lambda: {
                "success_count": 1,
                "failed_count": 0,
                "remaining_count": 0,
                "completed_all": True,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            floorings_dir = root / "full_site_floorings"
            uid_dir = root / "uid_expansions" / "uid_expansion_selected"
            floorings_dir.mkdir(parents=True)
            uid_dir.mkdir(parents=True)
            (floorings_dir / "daily_hot-20260329_120000.csv").write_text("bvid,pubdate\nBV1,2026-03-28T12:00:00\n", encoding="utf-8-sig")
            (floorings_dir / "rankboard-20260329_120000.csv").write_text("bvid,pubdate\nBV2,2026-03-28T13:00:00\n", encoding="utf-8-sig")
            (uid_dir / "videolist_part_1.csv").write_text("bvid,pubdate\nBV3,2026-03-28T14:00:00\n", encoding="utf-8-sig")

            result = run_manual_realtime_batch_crawl(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                stream_data_time_window_hours=72,
                parallelism=1,
                comment_limit=10,
                consecutive_failure_limit=10,
                video_pool_root=root,
                manual_crawls_root_dir=manual_root,
                selected_flooring_csvs=["daily_hot-20260329_120000.csv", "rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_selected"],
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            session_dir = Path(result.session_dir)
            self.assertTrue(session_dir.name.startswith("manual_crawl_stat_comment_"))
            self.assertEqual("filtered_video_list.csv", Path(result.filtered_csv_path or "").name)

    @patch("bili_pipeline.datahub.manual_batch_runner.crawl_bvid_list_from_csv")
    def test_run_manual_realtime_batch_crawl_preserves_remaining_csv_in_session_dir(self, mock_batch: Mock) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            floorings_dir = root / "full_site_floorings"
            uid_dir = root / "uid_expansions" / "uid_expansion_selected"
            floorings_dir.mkdir(parents=True)
            uid_dir.mkdir(parents=True)
            (floorings_dir / "daily_hot-20260329_120000.csv").write_text("bvid,pubdate\nBV1,2026-03-28T12:00:00\n", encoding="utf-8-sig")
            (floorings_dir / "rankboard-20260329_120000.csv").write_text("bvid,pubdate\nBV2,2026-03-28T13:00:00\n", encoding="utf-8-sig")
            (uid_dir / "videolist_part_1.csv").write_text("bvid,pubdate\nBV3,2026-03-28T14:00:00\n", encoding="utf-8-sig")

            def _crawl_side_effect(*args, **kwargs):
                session_dir = Path(kwargs["session_dir"])
                remaining_csv = session_dir / "remaining_bvids_part_1.csv"
                remaining_csv.write_text("bvid\nBV3\n", encoding="utf-8-sig")
                return SimpleNamespace(
                    success_count=2,
                    failed_count=1,
                    remaining_count=1,
                    completed_all=False,
                    remaining_csv_path=str(remaining_csv),
                    to_dict=lambda: {
                        "success_count": 2,
                        "failed_count": 1,
                        "remaining_count": 1,
                        "completed_all": False,
                        "remaining_csv_path": str(remaining_csv),
                    },
                )

            mock_batch.side_effect = _crawl_side_effect

            result = run_manual_realtime_batch_crawl(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                stream_data_time_window_hours=72,
                parallelism=1,
                comment_limit=10,
                consecutive_failure_limit=10,
                video_pool_root=root,
                manual_crawls_root_dir=manual_root,
                selected_flooring_csvs=["daily_hot-20260329_120000.csv", "rankboard-20260329_120000.csv"],
                selected_uid_task_dirs=["uid_expansion_selected"],
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            self.assertEqual("partial", result.status)
            self.assertTrue(Path(result.crawl_report.remaining_csv_path).is_file())
            self.assertEqual(Path(result.session_dir), Path(result.crawl_report.remaining_csv_path).parent)

    @patch("bili_pipeline.datahub.manual_batch_runner.crawl_bvid_list_from_csv")
    def test_run_manual_realtime_batch_crawl_supports_flooring_only_selection(self, mock_batch: Mock) -> None:
        mock_batch.return_value = SimpleNamespace(
            success_count=1,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            to_dict=lambda: {
                "success_count": 1,
                "failed_count": 0,
                "remaining_count": 0,
                "completed_all": True,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            floorings_dir = root / "full_site_floorings"
            floorings_dir.mkdir(parents=True)

            (floorings_dir / "custom_flooring-20260329_120000.csv").write_text(
                "bvid,pubdate,title\nBV_ONLY,2026-03-28T12:00:00,flooring-only\n",
                encoding="utf-8-sig",
            )

            result = run_manual_realtime_batch_crawl(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                stream_data_time_window_hours=72,
                parallelism=1,
                comment_limit=10,
                consecutive_failure_limit=10,
                video_pool_root=root,
                manual_crawls_root_dir=manual_root,
                selected_flooring_csvs=["custom_flooring-20260329_120000.csv"],
                selected_uid_task_dirs=[],
                started_at=datetime(2026, 3, 29, 12, 0, 0),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual(1, result.filtered_bvid_count)
            _, kwargs = mock_batch.call_args
            self.assertEqual("filtered_video_list.csv", Path(kwargs["source_csv_name"]).name)

    def test_run_manual_realtime_batch_crawl_rejects_empty_explicit_source_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "video_pool"
            manual_root = Path(tmp_dir) / "video_data" / "manual_crawls"
            (root / "full_site_floorings").mkdir(parents=True)
            (root / "uid_expansions").mkdir(parents=True)

            with self.assertRaises(ValueError) as ctx:
                run_manual_realtime_batch_crawl(
                    gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
                    stream_data_time_window_hours=72,
                    parallelism=1,
                    comment_limit=10,
                    consecutive_failure_limit=10,
                    video_pool_root=root,
                    manual_crawls_root_dir=manual_root,
                    selected_flooring_csvs=[],
                    selected_uid_task_dirs=[],
                    started_at=datetime(2026, 3, 29, 12, 0, 0),
                )

            self.assertIn("至少选择", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
