from __future__ import annotations

import sys
import types
import unittest
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

from bili_pipeline.storage.gcp_store import BigQueryCrawlerStore


class _ArrayQueryParameter:
    def __init__(self, name: str, type_name: str, values: list[str]) -> None:
        self.name = name
        self.type_name = type_name
        self.values = values


class GcpStoreManualMediaHelpersTest(unittest.TestCase):
    def _build_store(self) -> BigQueryCrawlerStore:
        store = object.__new__(BigQueryCrawlerStore)
        store.project_id = "proj"
        store.dataset_name = "dataset"
        return store

    def test_export_manual_media_waitlist_rows_returns_rows_and_queries_expected_tables(self) -> None:
        store = self._build_store()
        captured: dict[str, object] = {}

        def fake_query(sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "bvid": "BV_STAT_ONLY",
                    "has_stat_snapshot": True,
                    "has_comment_snapshot": False,
                },
                {
                    "bvid": "BV_COMMENT_ONLY",
                    "has_stat_snapshot": False,
                    "has_comment_snapshot": True,
                },
            ]

        store._query = fake_query  # type: ignore[method-assign]

        rows = store.export_manual_media_waitlist_rows()

        self.assertEqual(
            [
                {
                    "bvid": "BV_STAT_ONLY",
                    "has_stat_snapshot": True,
                    "has_comment_snapshot": False,
                },
                {
                    "bvid": "BV_COMMENT_ONLY",
                    "has_stat_snapshot": False,
                    "has_comment_snapshot": True,
                },
            ],
            rows,
        )
        sql = str(captured["sql"])
        self.assertIn("video_stat_snapshots", sql)
        self.assertIn("topn_comment_snapshots", sql)
        self.assertIn("videos", sql)
        self.assertIn("assets", sql)
        self.assertIn("asset_type = 'video'", sql)
        self.assertIn("asset_type = 'audio'", sql)

    @patch("bili_pipeline.storage.gcp_store.bigquery.ArrayQueryParameter", _ArrayQueryParameter)
    def test_fetch_completed_media_metadata_bvids_returns_only_completed_bvids(self) -> None:
        store = self._build_store()
        captured: dict[str, object] = {}

        def fake_query(sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [{"bvid": "BV_DONE"}]

        store._query = fake_query  # type: ignore[method-assign]

        completed = store.fetch_completed_media_metadata_bvids(["BV_DONE", "BV_PENDING"])

        self.assertEqual({"BV_DONE"}, completed)
        sql = str(captured["sql"])
        self.assertIn("UNNEST(@bvids)", sql)
        self.assertIn("videos", sql)
        self.assertIn("assets", sql)
        self.assertIn("asset_type = 'video'", sql)
        self.assertIn("asset_type = 'audio'", sql)
        params = captured["params"]
        self.assertIsNotNone(params)
        self.assertEqual("bvids", params[0].name)
        self.assertEqual(["BV_DONE", "BV_PENDING"], params[0].values)

    def test_fetch_completed_media_metadata_bvids_short_circuits_empty_input(self) -> None:
        store = self._build_store()

        def fail_query(_sql, params=None):
            raise AssertionError("query should not run for empty input")

        store._query = fail_query  # type: ignore[method-assign]

        completed = store.fetch_completed_media_metadata_bvids([])

        self.assertEqual(set(), completed)


if __name__ == "__main__":
    unittest.main()
