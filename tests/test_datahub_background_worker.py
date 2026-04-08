from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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

MODULE_PATH = Path(__file__).resolve().parent.parent / "scripts" / "datahub_background_worker.py"
SPEC = importlib.util.spec_from_file_location("datahub_background_worker_module", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
worker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = worker
SPEC.loader.exec_module(worker)


class DatahubBackgroundWorkerTest(unittest.TestCase):
    def _build_config(self, task_kind: str) -> dict:
        return {
            "task_kind": task_kind,
            "gcp_payload": {
                "gcp_project_id": "demo-project",
                "bigquery_dataset": "demo_dataset",
                "gcs_bucket_name": "demo-bucket",
            },
            "payload": {
                "manual_crawls_root_dir": "outputs/video_data/manual_crawls",
                "parallelism": 1,
                "comment_limit": 10,
                "consecutive_failure_limit": 10,
                "max_height": 1080,
                "chunk_size_mb": 4,
                "truncate_seconds": 30,
            },
        }

    @patch.object(worker, "BigQueryCrawlerStore")
    @patch.object(worker, "run_manual_media_mode_a")
    def test_run_task_defaults_sleep_controls_for_manual_media_mode_a(
        self,
        mock_run_manual_media_mode_a,
        _mock_store,
    ) -> None:
        mock_run_manual_media_mode_a.return_value = SimpleNamespace(to_dict=lambda: {"status": "completed"})

        result = worker._run_task(Path("task-dir"), self._build_config("manual_media_mode_a"))

        self.assertEqual({"status": "completed"}, result)
        _, kwargs = mock_run_manual_media_mode_a.call_args
        self.assertFalse(kwargs["enable_sleep_resume"])
        self.assertEqual(5, kwargs["sleep_minutes"])

    @patch.object(worker, "BigQueryCrawlerStore")
    @patch.object(worker, "run_manual_media_mode_b")
    def test_run_task_defaults_sleep_controls_for_manual_media_mode_b(
        self,
        mock_run_manual_media_mode_b,
        _mock_store,
    ) -> None:
        mock_run_manual_media_mode_b.return_value = SimpleNamespace(to_dict=lambda: {"status": "completed"})
        config = self._build_config("manual_media_mode_b")
        config["payload"]["uploaded_file_paths"] = ["demo.csv"]
        config["payload"]["uploaded_names"] = ["demo.csv"]

        with patch.object(worker, "_read_uploaded_frames", return_value=[]):
            result = worker._run_task(Path("task-dir"), config)

        self.assertEqual({"status": "completed"}, result)
        _, kwargs = mock_run_manual_media_mode_b.call_args
        self.assertFalse(kwargs["enable_sleep_resume"])
        self.assertEqual(5, kwargs["sleep_minutes"])


if __name__ == "__main__":
    unittest.main()
