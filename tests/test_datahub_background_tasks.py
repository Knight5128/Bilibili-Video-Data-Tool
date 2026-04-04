from __future__ import annotations

import csv
import subprocess
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
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

from bili_pipeline.datahub.background_tasks import (
    BACKGROUND_TASK_MISSING_PROCESS_GRACE_SECONDS,
    background_task_stop_requested,
    background_task_is_running,
    create_background_task_dir,
    finalize_background_task_status,
    is_background_task_process_running,
    launch_background_worker,
    load_background_task_result,
    load_background_task_stop_request,
    load_registered_background_task_status,
    load_background_task_status,
    load_cookie_text,
    request_background_task_stop,
    register_active_background_task,
    run_batched_crawl_from_csv,
    save_cookie_text,
    update_background_task_status,
    write_background_task_config,
)
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode


class DataHubBackgroundTasksTest(unittest.TestCase):
    def test_cookie_text_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cookie_path = Path(tmp_dir) / "cookie.txt"
            save_cookie_text("SESSDATA=a; bili_jct=b;", path=cookie_path)
            self.assertEqual("SESSDATA=a; bili_jct=b;", load_cookie_text(cookie_path))

    def test_background_task_status_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=Path(tmp_dir),
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            status_path = update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "task_kind": "manual_dynamic_batch",
                },
            )
            self.assertTrue(status_path.exists())
            self.assertEqual("running", load_background_task_status(task_dir)["status"])

    def test_background_task_stop_request_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = Path(tmp_dir) / "task"
            task_dir.mkdir()

            request_background_task_stop(task_dir, reason="stop now")

            self.assertTrue(background_task_stop_requested(task_dir))
            payload = load_background_task_stop_request(task_dir)
            self.assertEqual("stop now", payload["reason"])
            self.assertTrue(payload["stop_requested"])

    def test_register_active_background_task_writes_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = Path(tmp_dir) / "task"
            task_dir.mkdir()
            registry_path = register_active_background_task(
                "manual_media",
                task_dir=task_dir,
                registry_root=Path(tmp_dir),
                pid=12345,
            )
            self.assertTrue(registry_path.exists())
            self.assertIn("manual_media", registry_path.name)

    @unittest.skipIf(getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0) == 0, "Windows breakaway flag unavailable")
    @patch("bili_pipeline.datahub.background_tasks.subprocess.Popen")
    def test_launch_background_worker_uses_breakaway_flag_on_windows(self, mock_popen) -> None:
        mock_popen.return_value = types.SimpleNamespace(pid=12345)
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = Path(tmp_dir) / "task"
            pid = launch_background_worker(task_dir)

        self.assertEqual(12345, pid)
        creationflags = int(mock_popen.call_args.kwargs["creationflags"])
        self.assertTrue(creationflags & getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        self.assertTrue(creationflags & getattr(subprocess, "DETACHED_PROCESS", 0))
        self.assertTrue(creationflags & getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0))

    @patch("bili_pipeline.datahub.background_tasks.os.kill", side_effect=SystemError("kill failed"))
    def test_is_background_task_process_running_returns_false_when_os_kill_raises_systemerror(self, _mock_kill) -> None:
        self.assertFalse(is_background_task_process_running(22288))

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_marks_dead_running_task_failed(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T12:00:00",
                },
            )

            registry_payload, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertIsNotNone(registry_payload)
            self.assertEqual("failed", status_payload["status"])
            self.assertTrue(status_payload["stale"])
            self.assertEqual("TaskProcessMissing", status_payload["error"]["type"])
            self.assertIn("no longer running", status_payload["error"]["message"])
            self.assertFalse(background_task_is_running("manual_dynamic_batch", registry_root=root))
            persisted = load_background_task_status(task_dir)
            self.assertEqual("failed", persisted["status"])
            self.assertTrue(persisted["stale"])
            self.assertIn("finished_at", persisted)

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_keeps_recent_heartbeat_running(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            recent_heartbeat = (datetime.now() - timedelta(seconds=15)).isoformat()
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T12:00:00",
                    "last_progress_at": recent_heartbeat,
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertEqual("running", status_payload["status"])
            self.assertEqual(recent_heartbeat, status_payload["last_progress_at"])
            self.assertNotIn("stale", status_payload)

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_marks_missing_process_failed_after_grace_period(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            stale_heartbeat = (
                datetime.now() - timedelta(seconds=BACKGROUND_TASK_MISSING_PROCESS_GRACE_SECONDS + 15)
            ).isoformat()
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T12:00:00",
                    "last_progress_at": stale_heartbeat,
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertEqual("failed", status_payload["status"])
            self.assertTrue(status_payload["stale"])
            self.assertEqual("TaskProcessMissing", status_payload["error"]["type"])

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_falls_back_to_local_task_dir_name(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 13, 0, 0),
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=Path("D:/legacy/Bilibili-Datahub/outputs/video_data/background_tasks") / task_dir.name,
                registry_root=root,
                pid=22288,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/background_tasks/" + task_dir.name,
                    "pid": 22288,
                    "started_at": "2026-04-01T13:00:00",
                },
            )

            registry_payload, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertIsNotNone(registry_payload)
            self.assertEqual(str(task_dir), registry_payload["task_dir"])
            self.assertEqual("failed", status_payload["status"])
            self.assertEqual(str(task_dir), status_payload["task_dir"])

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_recovers_completed_dynamic_task_from_batch_state(
        self,
        _mock_running,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            manual_crawls_root = root / "manual_crawls"
            session_dir = manual_crawls_root / "manual_crawl_stat_comment_20260401_120001"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "manual_crawl_state.json").write_text(
                """{
  "status": "crawling",
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_stat_comment_20260401_120001",
  "started_at": "2026-04-01T12:00:00"
}
""",
                encoding="utf-8",
            )
            (session_dir / "batch_crawl_state.json").write_text(
                """{
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_stat_comment_20260401_120001",
  "session_completed_at": "2026-04-01T13:45:00",
  "completed_all": true
}
""",
                encoding="utf-8",
            )
            write_background_task_config(
                task_dir,
                {
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "payload": {
                        "manual_crawls_root_dir": str(manual_crawls_root),
                    },
                },
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T12:00:00",
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertEqual("completed", status_payload["status"])
            self.assertTrue(status_payload["stale"])
            self.assertEqual("RecoveredFromBatchState", status_payload["recovery"]["type"])
            self.assertEqual("completed", status_payload["result"]["status"])
            self.assertEqual(str(session_dir), status_payload["result"]["session_dir"])

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_recovers_completed_dynamic_task_with_legacy_manual_crawls_root(
        self,
        _mock_running,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "background_tasks"
            root.mkdir(parents=True, exist_ok=True)
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 14, 0, 0),
            )
            manual_crawls_root = root.parent / "manual_crawls"
            session_dir = manual_crawls_root / "manual_crawl_stat_comment_20260401_140001"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "manual_crawl_state.json").write_text(
                """{
  "status": "crawling",
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_stat_comment_20260401_140001",
  "started_at": "2026-04-01T14:00:00"
}
""",
                encoding="utf-8",
            )
            (session_dir / "batch_crawl_state.json").write_text(
                """{
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_stat_comment_20260401_140001",
  "session_completed_at": "2026-04-01T14:45:00",
  "completed_all": true
}
""",
                encoding="utf-8",
            )
            write_background_task_config(
                task_dir,
                {
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "payload": {
                        "manual_crawls_root_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls",
                    },
                },
            )
            register_active_background_task(
                "manual_dynamic_batch",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "failed",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T14:00:00",
                    "stale": True,
                    "error": {
                        "type": "TaskProcessMissing",
                        "message": "Background task process is no longer running; marking stale task as failed.",
                    },
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_dynamic_batch",
                registry_root=root,
            )

            self.assertEqual("completed", status_payload["status"])
            self.assertEqual(str(session_dir), status_payload["result"]["session_dir"])

    def test_finalize_background_task_status_writes_completed_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = create_background_task_dir(
                "manual_media_mode_a",
                root_dir=Path(tmp_dir),
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )

            status_path = finalize_background_task_status(
                task_dir,
                scope="manual_media",
                task_kind="manual_media_mode_a",
                pid=12345,
                status="completed",
                result={"status": "completed", "task_count": 3},
            )

            self.assertTrue(status_path.exists())
            payload = load_background_task_status(task_dir)
            self.assertEqual("completed", payload["status"])
            self.assertEqual({"status": "completed", "task_count": 3}, payload["result"])
            self.assertEqual(12345, payload["pid"])
            self.assertIn("finished_at", payload)
            self.assertIn("last_progress_at", payload)

    def test_finalize_background_task_status_persists_full_result_in_separate_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=Path(tmp_dir),
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            full_result = {
                "status": "completed",
                "task_count": 2,
                "crawl_reports": [{"run_id": f"run-{idx}", "success_count": 1} for idx in range(500)],
            }

            finalize_background_task_status(
                task_dir,
                scope="manual_dynamic_batch",
                task_kind="manual_dynamic_batch",
                pid=12345,
                status="completed",
                result=full_result,
            )

            payload = load_background_task_status(task_dir)
            self.assertEqual("completed", payload["status"])
            self.assertIn("result", payload)
            self.assertIn("result_path", payload)
            self.assertNotEqual(full_result, payload["result"])
            self.assertNotIn("crawl_reports", payload["result"])
            self.assertEqual(500, payload["result"]["omitted_fields"]["crawl_reports"]["count"])
            persisted_result = load_background_task_result(task_dir)
            self.assertEqual(full_result, persisted_result)

    def test_load_background_task_status_compacts_legacy_inline_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=Path(tmp_dir),
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            full_result = {
                "status": "completed",
                "task_count": 2,
                "crawl_reports": [{"run_id": f"run-{idx}", "success_count": 1} for idx in range(500)],
            }
            update_background_task_status(
                task_dir,
                {
                    "status": "completed",
                    "scope": "manual_dynamic_batch",
                    "task_kind": "manual_dynamic_batch",
                    "task_dir": str(task_dir),
                    "result": full_result,
                },
            )

            payload = load_background_task_status(task_dir)

            self.assertIn("result_path", payload)
            self.assertNotEqual(full_result, payload["result"])
            self.assertEqual(full_result, load_background_task_result(task_dir))

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_recovers_completed_media_mode_a_task(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "background_tasks"
            root.mkdir(parents=True, exist_ok=True)
            task_dir = create_background_task_dir(
                "manual_media_mode_a",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 15, 0, 0),
            )
            manual_crawls_root = root.parent / "manual_crawls"
            session_dir = manual_crawls_root / "manual_crawl_media_mode_A_20260401_150000"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "manual_crawl_media_state.json").write_text(
                """{
  "mode": "A",
  "status": "completed",
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_media_mode_A_20260401_150000",
  "state_path": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_media_mode_A_20260401_150000/manual_crawl_media_state.json",
  "input_bvid_count": 5,
  "submitted_bvid_count": 5,
  "task_count": 2,
  "sleep_count": 1,
  "cookie_refresh_count": 0,
  "started_at": "2026-04-01T15:00:00",
  "finished_at": "2026-04-01T15:20:00"
}
""",
                encoding="utf-8",
            )
            write_background_task_config(
                task_dir,
                {
                    "scope": "manual_media",
                    "task_kind": "manual_media_mode_a",
                    "task_dir": str(task_dir),
                    "payload": {
                        "manual_crawls_root_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls",
                    },
                },
            )
            register_active_background_task(
                "manual_media",
                task_dir=task_dir,
                registry_root=root,
                pid=22288,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "scope": "manual_media",
                    "task_kind": "manual_media_mode_a",
                    "task_dir": str(task_dir),
                    "pid": 22288,
                    "started_at": "2026-04-01T15:00:00",
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_media",
                registry_root=root,
            )

            self.assertEqual("completed", status_payload["status"])
            self.assertTrue(status_payload["stale"])
            self.assertEqual("RecoveredFromManualMediaState", status_payload["recovery"]["type"])
            self.assertEqual("A", status_payload["result"]["mode"])
            self.assertEqual(str(session_dir), status_payload["result"]["session_dir"])

    @patch("bili_pipeline.datahub.background_tasks.is_background_task_process_running", return_value=False)
    def test_load_registered_background_task_status_recovers_partial_media_mode_b_task(self, _mock_running) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "background_tasks"
            root.mkdir(parents=True, exist_ok=True)
            task_dir = create_background_task_dir(
                "manual_media_mode_b",
                root_dir=root,
                started_at=datetime(2026, 4, 1, 16, 0, 0),
            )
            manual_crawls_root = root.parent / "manual_crawls"
            session_dir = manual_crawls_root / "manual_crawl_media_mode_B_20260401_160000"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "manual_crawl_media_state.json").write_text(
                """{
  "mode": "B",
  "status": "partial",
  "session_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_media_mode_B_20260401_160000",
  "state_path": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls/manual_crawl_media_mode_B_20260401_160000/manual_crawl_media_state.json",
  "input_bvid_count": 10,
  "submitted_bvid_count": 8,
  "task_count": 1,
  "sleep_count": 0,
  "cookie_refresh_count": 0,
  "started_at": "2026-04-01T16:00:00",
  "finished_at": "2026-04-01T16:10:00",
  "stop_category": "risk"
}
""",
                encoding="utf-8",
            )
            write_background_task_config(
                task_dir,
                {
                    "scope": "manual_media",
                    "task_kind": "manual_media_mode_b",
                    "task_dir": str(task_dir),
                    "payload": {
                        "manual_crawls_root_dir": "D:/legacy/Bilibili-Datahub/outputs/video_data/manual_crawls",
                    },
                },
            )
            register_active_background_task(
                "manual_media",
                task_dir=task_dir,
                registry_root=root,
                pid=22289,
            )
            update_background_task_status(
                task_dir,
                {
                    "status": "failed",
                    "scope": "manual_media",
                    "task_kind": "manual_media_mode_b",
                    "task_dir": str(task_dir),
                    "pid": 22289,
                    "started_at": "2026-04-01T16:00:00",
                    "stale": True,
                    "error": {
                        "type": "TaskProcessMissing",
                        "message": "Background task process is no longer running; marking stale task as failed.",
                    },
                },
            )

            _, status_payload = load_registered_background_task_status(
                "manual_media",
                registry_root=root,
            )

            self.assertEqual("partial", status_payload["status"])
            self.assertEqual("RecoveredFromManualMediaState", status_payload["recovery"]["type"])
            self.assertEqual("B", status_payload["result"]["mode"])
            self.assertEqual("partial", status_payload["result"]["status"])

    def test_run_batched_crawl_from_csv_refreshes_cookie_per_batch_and_remaining_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "input.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV1"})
                writer.writerow({"bvid": "BV2"})
                writer.writerow({"bvid": "BV3"})

            remaining_csv = Path(tmp_dir) / "remaining.csv"
            with remaining_csv.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV2"})

            provider_calls: list[int] = []
            crawl_inputs: list[str] = []

            def provider():
                provider_calls.append(1)
                return object()

            def fake_crawl(target_csv, **kwargs):
                crawl_inputs.append(str(target_csv))
                if len(crawl_inputs) == 1:
                    return BatchCrawlReport(
                        run_id="run-1",
                        total_bvids=2,
                        processed_count=1,
                        success_count=1,
                        failed_count=1,
                        remaining_count=1,
                        started_at=datetime(2026, 4, 1, 12, 0, 0),
                        finished_at=datetime(2026, 4, 1, 12, 1, 0),
                        task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                        completed_all=False,
                        stop_reason="HTTP 412 precondition failed",
                        remaining_csv_path=str(remaining_csv),
                        session_dir=str(Path(tmp_dir) / "session"),
                        logs_dir=str(Path(tmp_dir) / "logs"),
                    )
                return BatchCrawlReport(
                    run_id=f"run-{len(crawl_inputs)}",
                    total_bvids=1,
                    processed_count=1,
                    success_count=1,
                    failed_count=0,
                    remaining_count=0,
                    started_at=datetime(2026, 4, 1, 12, 2, 0),
                    finished_at=datetime(2026, 4, 1, 12, 3, 0),
                    task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                    completed_all=True,
                    stop_reason="done",
                    session_dir=str(Path(tmp_dir) / "session"),
                    logs_dir=str(Path(tmp_dir) / "logs"),
                )

            outcome = run_batched_crawl_from_csv(
                csv_path,
                batch_size=2,
                credential_provider=provider,
                crawl_fn=fake_crawl,
                should_retry_remaining_fn=lambda report: "412" in str(report.stop_reason),
                parallelism=1,
                enable_media=False,
                task_mode=CrawlTaskMode.REALTIME_ONLY,
                session_dir=Path(tmp_dir) / "session",
                output_root_dir=Path(tmp_dir),
            )

            self.assertEqual(3, len(outcome.reports))
            self.assertEqual(3, outcome.credential_refresh_count)
            self.assertEqual(3, len(provider_calls))
            self.assertEqual(str(remaining_csv), crawl_inputs[1])

    def test_run_batched_crawl_from_csv_stops_before_next_batch_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "input.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV1"})
                writer.writerow({"bvid": "BV2"})

            calls: list[str] = []

            def crawl_fn(current_csv_path, **_kwargs):
                rows = list(csv.DictReader(Path(current_csv_path).open("r", encoding="utf-8-sig", newline="")))
                calls.extend(row["bvid"] for row in rows)
                return BatchCrawlReport(
                    run_id="run-stop",
                    total_bvids=len(rows),
                    processed_count=len(rows),
                    success_count=len(rows),
                    failed_count=0,
                    remaining_count=0,
                    started_at=datetime(2026, 4, 1, 12, 0, 0),
                    finished_at=datetime(2026, 4, 1, 12, 0, 1),
                    task_mode=CrawlTaskMode.MEDIA_ONLY.value,
                    completed_all=True,
                )

            stop_checks = iter([False, False, True])
            outcome = run_batched_crawl_from_csv(
                csv_path,
                batch_size=1,
                crawl_fn=crawl_fn,
                should_stop=lambda: next(stop_checks, True),
            )

            self.assertEqual(["BV1"], calls)
            self.assertEqual(1, len(outcome.reports))
            self.assertTrue(outcome.stopped_by_request)


if __name__ == "__main__":
    unittest.main()
