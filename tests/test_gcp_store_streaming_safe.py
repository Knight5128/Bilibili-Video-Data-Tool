from __future__ import annotations

import sys
import types
import unittest


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

from bili_pipeline.storage.gcp_store import BigQueryCrawlerStore


class GcpStoreStreamingSafeTest(unittest.TestCase):
    def _build_store(self) -> BigQueryCrawlerStore:
        store = object.__new__(BigQueryCrawlerStore)
        store.project_id = "proj"
        store.dataset_name = "dataset"
        return store

    def test_save_run_start_buffers_locally_and_finalize_inserts_without_query_mutation(self) -> None:
        store = self._build_store()
        inserted: list[tuple[str, dict]] = []

        def fail_query(_sql, params=None):
            raise AssertionError("save/finalize run should not issue UPDATE/DELETE queries")

        def capture_insert(table_name: str, row: dict) -> None:
            inserted.append((table_name, dict(row)))

        store._query = fail_query  # type: ignore[method-assign]
        store._insert_row = capture_insert  # type: ignore[method-assign]

        store.save_run_start("run-1", "batch_csv", {"note": "x"})
        self.assertEqual([], inserted)

        store.finalize_run("run-1", total_bvids=10, success_count=8, failed_count=2)

        self.assertEqual(1, len(inserted))
        table_name, row = inserted[0]
        self.assertEqual("crawl_runs", table_name)
        self.assertEqual("run-1", row["run_id"])
        self.assertEqual("batch_csv", row["mode"])
        self.assertEqual(10, row["total_bvids"])
        self.assertEqual(8, row["success_count"])
        self.assertEqual(2, row["failed_count"])
        self.assertTrue(row["started_at"])
        self.assertTrue(row["finished_at"])

    def test_begin_and_finalize_media_asset_insert_complete_row_without_update_query(self) -> None:
        store = self._build_store()
        inserted: list[tuple[str, dict]] = []

        def fail_query(_sql, params=None):
            raise AssertionError("media asset finalize should not issue UPDATE/DELETE queries")

        def capture_insert(table_name: str, row: dict) -> None:
            inserted.append((table_name, dict(row)))

        store._query = fail_query  # type: ignore[method-assign]
        store._insert_row = capture_insert  # type: ignore[method-assign]

        asset_id = store.begin_media_asset(
            bvid="BV1",
            cid=123,
            asset_type="video",
            storage_backend="gcs",
            object_key="foo/bar",
            bucket_name="bucket",
            storage_endpoint="https://storage.googleapis.com",
            object_url="gs://bucket/foo/bar",
            format_selected="1080p",
            mime_type="video/mp4",
            upload_session_id="upload-1",
            raw_payload={"url": "https://example.com"},
        )
        self.assertEqual([], inserted)

        store.finalize_media_asset(
            asset_id,
            file_size=12345,
            sha256="abc",
            chunk_count=4,
            etag="etag-1",
        )

        self.assertEqual(1, len(inserted))
        table_name, row = inserted[0]
        self.assertEqual("assets", table_name)
        self.assertEqual(asset_id, row["asset_id"])
        self.assertEqual("BV1", row["bvid"])
        self.assertEqual(12345, row["file_size"])
        self.assertEqual("abc", row["sha256"])
        self.assertEqual(4, row["chunk_count"])
        self.assertEqual("etag-1", row["etag"])

    def test_save_video_meta_inserts_latest_snapshot_without_delete_query(self) -> None:
        store = self._build_store()
        inserted: list[tuple[str, dict]] = []

        def fail_query(_sql, params=None):
            raise AssertionError("save_video_meta should not issue DELETE query")

        def capture_insert(table_name: str, row: dict) -> None:
            inserted.append((table_name, dict(row)))

        store._query = fail_query  # type: ignore[method-assign]
        store._insert_row = capture_insert  # type: ignore[method-assign]

        result = types.SimpleNamespace(
            bvid="BV_META",
            aid=1,
            title="title",
            desc="desc",
            pic="pic",
            dynamic="dynamic",
            tags=["a"],
            tag_details=[{"tag_name": "a"}],
            videos=1,
            tid=1,
            tid_v2=2,
            tname="t",
            tname_v2="t2",
            copyright=1,
            owner_mid=100,
            owner_name="owner",
            owner_face="face",
            owner_sign="sign",
            owner_gender="unknown",
            owner_level=6,
            owner_verified=False,
            owner_verified_title="",
            owner_vip_type=0,
            owner_follower_count=1,
            owner_following_count=2,
            owner_video_count=3,
            is_activity_participant=False,
            duration=10,
            state=0,
            pubdate=None,
            cid=11,
            resolution_width=1920,
            resolution_height=1080,
            resolution_rotate=0,
            is_story=False,
            is_interactive_video=False,
            is_downloadable=True,
            is_reprint_allowed=True,
            is_collaboration=False,
            is_360=False,
            is_paid_video=False,
            pages_info=[],
            rights={},
            subtitle={},
            uploader_profile={},
            uploader_relation={},
            uploader_overview={},
            raw_payload={},
        )

        store.save_video_meta(result)

        self.assertEqual(1, len(inserted))
        table_name, row = inserted[0]
        self.assertEqual("videos", table_name)
        self.assertEqual("BV_META", row["bvid"])
        self.assertTrue(row["updated_at"])

    def test_fetch_video_row_orders_latest_first(self) -> None:
        store = self._build_store()
        captured: dict[str, object] = {}

        def fake_query(sql, params=None):
            captured["sql"] = sql
            return []

        store._query = fake_query  # type: ignore[method-assign]
        self.assertIsNone(store.fetch_video_row("BV1"))
        self.assertIn("ORDER BY updated_at DESC", str(captured["sql"]))

    def test_fetch_all_asset_rows_returns_latest_row_per_asset_key(self) -> None:
        store = self._build_store()
        captured: dict[str, object] = {}

        def fake_query(sql, params=None):
            captured["sql"] = sql
            return []

        store._query = fake_query  # type: ignore[method-assign]
        self.assertEqual([], store.fetch_all_asset_rows("BV1"))
        sql = str(captured["sql"])
        self.assertIn("ROW_NUMBER()", sql)
        self.assertIn("PARTITION BY bvid, cid, asset_type", sql)


if __name__ == "__main__":
    unittest.main()
