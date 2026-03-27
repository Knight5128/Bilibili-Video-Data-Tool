from __future__ import annotations

import io
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from werkzeug.datastructures import FileStorage

from bili_pipeline.cloud_tracker.admin import parse_owner_mid_upload
from bili_pipeline.cloud_tracker.app import create_app
from bili_pipeline.cloud_tracker.store import TrackerStore


class _FakeBigQueryClient:
    def __init__(self) -> None:
        self.insert_calls: list[tuple[str, list[dict[str, object]]]] = []

    def insert_rows_json(self, table_id: str, rows: list[dict[str, object]]) -> list[object]:
        self.insert_calls.append((table_id, rows))
        return []


class CloudTrackerUploadTest(unittest.TestCase):
    def test_parse_owner_mid_upload_requires_owner_mid_column(self) -> None:
        upload = FileStorage(stream=io.BytesIO("uid\n123\n".encode("utf-8")), filename="bad.csv")

        with self.assertRaisesRegex(ValueError, "owner_mid"):
            parse_owner_mid_upload(upload)

    def test_upload_authors_returns_400_for_invalid_csv(self) -> None:
        fake_store = SimpleNamespace(replace_author_sources=Mock())
        fake_runner = SimpleNamespace(
            settings=SimpleNamespace(admin_token="secret-token"),
            tracker_store=fake_store,
        )

        with patch("bili_pipeline.cloud_tracker.app.TrackerRunner", return_value=fake_runner):
            app = create_app(settings=object())

        client = app.test_client()
        response = client.post(
            "/admin/authors/upload",
            headers={"Authorization": "Bearer secret-token"},
            data={
                "file": (io.BytesIO("uid\n123\n".encode("utf-8")), "bad.csv"),
                "source_name": "selected_authors",
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(400, response.status_code)
        self.assertEqual("上传的 CSV 必须包含 owner_mid 列。", response.get_json()["error"])
        fake_store.replace_author_sources.assert_not_called()


class TrackerStoreReplaceAuthorSourcesTest(unittest.TestCase):
    def test_replace_author_sources_uses_bigquery_compatible_delete(self) -> None:
        store = object.__new__(TrackerStore)
        store.client = _FakeBigQueryClient()
        queries: list[str] = []
        store._query = lambda sql, params=None: queries.append(sql) or []
        store.tracker_table_id = lambda suffix: f"project.dataset.tracker_{suffix}"

        saved = store.replace_author_sources(
            owner_mids=[30222764, 3670216, 30222764],
            source_name="selected_authors",
        )

        self.assertEqual(2, saved)
        self.assertEqual(
            "DELETE FROM `project.dataset.tracker_author_sources` WHERE TRUE",
            queries[0],
        )
        self.assertEqual(1, len(store.client.insert_calls))
        inserted_table_id, inserted_rows = store.client.insert_calls[0]
        self.assertEqual("project.dataset.tracker_author_sources", inserted_table_id)
        self.assertEqual([3670216, 30222764], [row["owner_mid"] for row in inserted_rows])


if __name__ == "__main__":
    unittest.main()
