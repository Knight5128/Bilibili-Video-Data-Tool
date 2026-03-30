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

import pandas as pd


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

from bili_pipeline.datahub.manual_media_runner import (
    classify_manual_media_error_text,
    run_manual_media_mode_a,
    run_manual_media_mode_b,
    sync_manual_media_waitlist,
)
from bili_pipeline.models import CrawlTaskMode, GCPStorageConfig


class _FakeReport(SimpleNamespace):
    def to_dict(self):
        return dict(self.__dict__)


class _FakeStore:
    def __init__(self, waitlist_rows=None, completed_bvids=None) -> None:
        self._waitlist_rows = waitlist_rows or []
        self._completed_bvids = completed_bvids or set()

    def export_manual_media_waitlist_rows(self):
        return list(self._waitlist_rows)

    def fetch_completed_media_metadata_bvids(self, bvids):
        return {bvid for bvid in bvids if bvid in self._completed_bvids}


class ManualMediaRunnerTest(unittest.TestCase):
    def test_sync_manual_media_waitlist_writes_dataset_named_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = _FakeStore(
                waitlist_rows=[
                    {"bvid": "BV1", "has_stat_snapshot": True, "has_comment_snapshot": False},
                    {"bvid": "BV2", "has_stat_snapshot": False, "has_comment_snapshot": True},
                ]
            )

            result = sync_manual_media_waitlist(
                store=store,
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="demo_ds", gcs_bucket_name="bucket"),
                manual_crawls_root_dir=tmp_dir,
            )

            waitlist_path = Path(result.waitlist_path)
            self.assertEqual("manual_crawl_media_waitlist_demo_ds.csv", waitlist_path.name)
            self.assertEqual(2, result.pending_count)
            rows = list(csv.DictReader(waitlist_path.open("r", encoding="utf-8-sig", newline="")))
            self.assertEqual(["BV1", "BV2"], [row["bvid"] for row in rows])

    @patch("bili_pipeline.datahub.manual_media_runner.crawl_bvid_list_from_csv")
    def test_run_manual_media_mode_b_deduplicates_and_filters_completed_items(self, mock_batch: Mock) -> None:
        mock_batch.return_value = _FakeReport(
            success_count=1,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            stop_reason="done",
            summaries=[],
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = _FakeStore(completed_bvids={"BV_DONE"})
            result = run_manual_media_mode_b(
                uploaded_frames=[
                    pd.DataFrame([{"bvid": "BV_DONE"}, {"bvid": "BV_TODO"}]),
                    pd.DataFrame([{"bvid": "BV_TODO"}]),
                ],
                uploaded_names=["a.csv", "b.csv"],
                store=store,
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="demo_ds", gcs_bucket_name="bucket"),
                manual_crawls_root_dir=tmp_dir,
                started_at=datetime(2026, 3, 31, 12, 0, 0),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual(2, result.input_bvid_count)
            self.assertEqual(1, result.skipped_completed_count)
            filtered_rows = list(
                csv.DictReader(Path(result.filtered_csv_path).open("r", encoding="utf-8-sig", newline=""))
            )
            self.assertEqual(["BV_TODO"], [row["bvid"] for row in filtered_rows])
            _, kwargs = mock_batch.call_args
            self.assertEqual(CrawlTaskMode.ONCE_ONLY, kwargs["task_mode"])
            self.assertTrue(kwargs["enable_media"])
            self.assertEqual(Path(result.session_dir), Path(kwargs["session_dir"]))

    @patch("bili_pipeline.datahub.manual_media_runner.time.sleep")
    @patch("bili_pipeline.datahub.manual_media_runner.sync_manual_media_waitlist")
    @patch("bili_pipeline.datahub.manual_media_runner.crawl_bvid_list_from_csv")
    def test_run_manual_media_mode_a_risk_sleep_creates_followup_task(
        self,
        mock_batch: Mock,
        mock_sync: Mock,
        mock_sleep: Mock,
    ) -> None:
        risk_report = _FakeReport(
            success_count=0,
            failed_count=1,
            remaining_count=1,
            completed_all=False,
            stop_reason="risk",
            summaries=[SimpleNamespace(bvid="BV_RISK", errors=["接口返回错误代码：-352，风控校验失败"])],
        )
        success_report = _FakeReport(
            success_count=1,
            failed_count=0,
            remaining_count=0,
            completed_all=True,
            stop_reason="done",
            summaries=[],
        )
        mock_batch.side_effect = [risk_report, success_report]

        with tempfile.TemporaryDirectory() as tmp_dir:
            waitlist_path = Path(tmp_dir) / "manual_crawl_media_waitlist_demo_ds.csv"
            pd.DataFrame([{"bvid": "BV_RISK"}]).to_csv(waitlist_path, index=False, encoding="utf-8-sig")
            mock_sync.return_value = SimpleNamespace(
                waitlist_path=str(waitlist_path),
                pending_count=1,
                status="completed",
            )

            result = run_manual_media_mode_a(
                store=_FakeStore(),
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="demo_ds", gcs_bucket_name="bucket"),
                manual_crawls_root_dir=tmp_dir,
                started_at=datetime(2026, 3, 31, 13, 0, 0),
                enable_sleep_resume=True,
                sleep_minutes=5,
            )

            self.assertEqual("completed", result.status)
            self.assertEqual(2, mock_batch.call_count)
            self.assertEqual(2, mock_sync.call_count)
            mock_sleep.assert_called_once_with(5 * 60)
            first_session_dir = Path(mock_batch.call_args_list[0].kwargs["session_dir"])
            second_session_dir = Path(mock_batch.call_args_list[1].kwargs["session_dir"])
            self.assertNotEqual(first_session_dir, second_session_dir)
            self.assertTrue(first_session_dir.name.startswith("manual_crawl_media_mode_A_"))
            self.assertTrue(second_session_dir.name.startswith("manual_crawl_media_mode_A_"))

    def test_classify_manual_media_error_text_distinguishes_risk_and_winerror(self) -> None:
        self.assertEqual("risk", classify_manual_media_error_text("状态码 412，触发风控"))
        self.assertEqual("winerror", classify_manual_media_error_text("[WinError 1450] Insufficient system resources"))
        self.assertEqual("other", classify_manual_media_error_text("plain runtime error"))


if __name__ == "__main__":
    unittest.main()
