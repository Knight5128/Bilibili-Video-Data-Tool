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

from bili_pipeline import crawl_api
from bili_pipeline.models import CrawlTaskMode, FullCrawlSummary


class _FakeStore:
    def save_run_start(self, *_args, **_kwargs) -> None:
        return None

    def save_run_item(self, *_args, **_kwargs) -> None:
        return None

    def finalize_run(self, *_args, **_kwargs) -> None:
        return None


class CrawlTaskModeTest(unittest.TestCase):
    @patch("bili_pipeline.crawl_api._resolve_gcp_config", return_value=SimpleNamespace(bigquery_dataset="ds", gcs_bucket_name="bucket"))
    @patch("bili_pipeline.crawl_api.crawl_media_assets")
    @patch("bili_pipeline.crawl_api.crawl_latest_comments")
    @patch("bili_pipeline.crawl_api.crawl_stat_snapshot")
    @patch("bili_pipeline.crawl_api.crawl_video_meta")
    def test_crawl_full_video_bundle_realtime_only_skips_meta_and_media(
        self,
        mock_meta: Mock,
        mock_stat: Mock,
        mock_comment: Mock,
        mock_media: Mock,
        _mock_resolve: Mock,
    ) -> None:
        mock_stat.return_value = {"ok": True}
        mock_comment.return_value = {"ok": True}

        summary = crawl_api.crawl_full_video_bundle(
            "BV1",
            task_mode=CrawlTaskMode.REALTIME_ONLY,
            enable_media=True,
            gcp_config=SimpleNamespace(),
        )

        self.assertEqual(CrawlTaskMode.REALTIME_ONLY.value, summary.task_mode)
        self.assertTrue(summary.meta_ok)
        self.assertTrue(summary.stat_ok)
        self.assertTrue(summary.comment_ok)
        self.assertTrue(summary.media_ok)
        mock_meta.assert_not_called()
        mock_media.assert_not_called()
        mock_stat.assert_called_once()
        mock_comment.assert_called_once()

    @patch("bili_pipeline.crawl_api._resolve_gcp_config", return_value=SimpleNamespace(bigquery_dataset="ds", gcs_bucket_name="bucket"))
    @patch("bili_pipeline.crawl_api.crawl_media_assets")
    @patch("bili_pipeline.crawl_api.crawl_latest_comments")
    @patch("bili_pipeline.crawl_api.crawl_stat_snapshot")
    @patch("bili_pipeline.crawl_api.crawl_video_meta")
    def test_crawl_full_video_bundle_once_only_without_media_skips_realtime(
        self,
        mock_meta: Mock,
        mock_stat: Mock,
        mock_comment: Mock,
        mock_media: Mock,
        _mock_resolve: Mock,
    ) -> None:
        mock_meta.return_value = {"ok": True}

        summary = crawl_api.crawl_full_video_bundle(
            "BV2",
            task_mode=CrawlTaskMode.ONCE_ONLY,
            enable_media=False,
            gcp_config=SimpleNamespace(),
        )

        self.assertEqual(CrawlTaskMode.ONCE_ONLY.value, summary.task_mode)
        self.assertTrue(summary.meta_ok)
        self.assertTrue(summary.stat_ok)
        self.assertTrue(summary.comment_ok)
        self.assertTrue(summary.media_ok)
        mock_meta.assert_called_once()
        mock_stat.assert_not_called()
        mock_comment.assert_not_called()
        mock_media.assert_not_called()

    @patch("bili_pipeline.crawl_api._store", return_value=_FakeStore())
    @patch("bili_pipeline.crawl_api._resolve_gcp_config", return_value=SimpleNamespace(bigquery_dataset="ds", gcs_bucket_name="bucket"))
    @patch("bili_pipeline.crawl_api.crawl_full_video_bundle")
    def test_crawl_bvid_list_from_csv_records_task_mode(
        self,
        mock_bundle: Mock,
        _mock_resolve: Mock,
        _mock_store: Mock,
    ) -> None:
        summaries = [
            FullCrawlSummary(
                bvid="BV_SUCCESS",
                meta_ok=True,
                stat_ok=True,
                comment_ok=True,
                media_ok=True,
                snapshot_time=datetime.now(),
                task_mode=CrawlTaskMode.REALTIME_ONLY.value,
            ),
            FullCrawlSummary(
                bvid="BV_FAIL",
                meta_ok=True,
                stat_ok=False,
                comment_ok=True,
                media_ok=True,
                snapshot_time=datetime.now(),
                task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                errors=["stat: fail"],
            ),
        ]
        mock_bundle.side_effect = summaries

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "input.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_SUCCESS"})
                writer.writerow({"bvid": "BV_FAIL"})

            report = crawl_api.crawl_bvid_list_from_csv(
                csv_path,
                parallelism=1,
                task_mode=CrawlTaskMode.REALTIME_ONLY,
                enable_media=False,
                gcp_config=SimpleNamespace(),
                output_root_dir=tmp_dir,
            )

        self.assertEqual(CrawlTaskMode.REALTIME_ONLY.value, report.task_mode)
        self.assertEqual(1, report.success_count)
        self.assertEqual(1, report.failed_count)
        self.assertEqual(1, report.remaining_count)
        self.assertFalse(report.completed_all)

    @patch("bili_pipeline.crawl_api._store", return_value=_FakeStore())
    @patch("bili_pipeline.crawl_api._resolve_gcp_config", return_value=SimpleNamespace(bigquery_dataset="ds", gcs_bucket_name="bucket"))
    @patch("bili_pipeline.crawl_api.crawl_full_video_bundle")
    def test_crawl_bvid_list_from_csv_respects_explicit_session_dir(
        self,
        mock_bundle: Mock,
        _mock_resolve: Mock,
        _mock_store: Mock,
    ) -> None:
        mock_bundle.return_value = FullCrawlSummary(
            bvid="BV_SESSION",
            meta_ok=True,
            stat_ok=True,
            comment_ok=True,
            media_ok=True,
            snapshot_time=datetime.now(),
            task_mode=CrawlTaskMode.REALTIME_ONLY.value,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "input.csv"
            session_dir = Path(tmp_dir) / "manual_crawl_20260329_120000"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV_SESSION"})

            report = crawl_api.crawl_bvid_list_from_csv(
                csv_path,
                parallelism=1,
                task_mode=CrawlTaskMode.REALTIME_ONLY,
                enable_media=False,
                gcp_config=SimpleNamespace(),
                output_root_dir=tmp_dir,
                session_dir=session_dir,
            )

            self.assertEqual(str(session_dir), report.session_dir)
            self.assertTrue((session_dir / "original_bvids.csv").exists())
            self.assertTrue(Path(report.task_log_path or "").exists())


if __name__ == "__main__":
    unittest.main()
