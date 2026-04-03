from __future__ import annotations

import json
import threading
import time
import types
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO
from urllib.parse import quote

try:
    import google.auth as google_auth
    from google.cloud import bigquery, storage
    from google.oauth2 import service_account

    _HAS_GOOGLE_CLOUD = True
except ModuleNotFoundError:  # pragma: no cover - exercised in dependency-light test environments.
    google_auth = None
    _HAS_GOOGLE_CLOUD = False

    class _MissingGoogleClass:
        def __init__(self, *args, **kwargs) -> None:
            raise ModuleNotFoundError("google-cloud dependencies are required to use GCP storage backends.")

    def _schema_field(*args, **kwargs):
        return None

    bigquery = types.SimpleNamespace(
        Client=_MissingGoogleClass,
        DatasetReference=_MissingGoogleClass,
        Dataset=_MissingGoogleClass,
        Table=_MissingGoogleClass,
        QueryJobConfig=_MissingGoogleClass,
        ScalarQueryParameter=_MissingGoogleClass,
        ArrayQueryParameter=_MissingGoogleClass,
        SchemaField=_schema_field,
        table=types.SimpleNamespace(Row=dict),
    )
    storage = types.SimpleNamespace(Client=_MissingGoogleClass)
    service_account = types.SimpleNamespace(Credentials=_MissingGoogleClass)

from bili_pipeline.models import (
    FullCrawlSummary,
    GCPStorageConfig,
    MediaAssetRef,
    MediaResult,
    MetaResult,
    StatSnapshot,
    CommentSnapshot,
)

_BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
_RETRYABLE_BIGQUERY_INSERT_EXCEPTION_NAMES = {
    "RetryError",
    "ConnectionError",
    "ConnectionResetError",
    "ReadTimeout",
    "Timeout",
    "SSLError",
    "MaxRetryError",
    "ProtocolError",
    "ServiceUnavailable",
    "TooManyRequests",
    "InternalServerError",
    "BadGateway",
    "GatewayTimeout",
}
_RETRYABLE_BIGQUERY_INSERT_MESSAGE_FRAGMENTS = (
    "eof occurred in violation of protocol",
    "max retries exceeded",
    "connection aborted",
    "connection reset",
    "temporarily unavailable",
    "timed out",
    "timeout of",
)


def _ensure_google_cloud_dependencies() -> None:
    if not _HAS_GOOGLE_CLOUD:
        raise ModuleNotFoundError("google-cloud dependencies are required to use GCP storage backends.")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list)):
        return value
    return json.loads(str(value))


def _timestamp_now() -> str:
    return datetime.now().isoformat()


def _is_retryable_bigquery_insert_exception(exc: Exception) -> bool:
    current: BaseException | None = exc
    checked = 0
    while current is not None and checked < 6:
        if current.__class__.__name__ in _RETRYABLE_BIGQUERY_INSERT_EXCEPTION_NAMES:
            return True
        message = str(current).strip().lower()
        if any(fragment in message for fragment in _RETRYABLE_BIGQUERY_INSERT_MESSAGE_FRAGMENTS):
            return True
        current = current.__cause__ or current.__context__
        checked += 1
    return False


def _build_credentials(config: GCPStorageConfig) -> tuple[Any | None, str | None]:
    _ensure_google_cloud_dependencies()
    if config.credentials_path.strip():
        credentials = service_account.Credentials.from_service_account_file(
            config.credentials_path.strip(),
            scopes=_BIGQUERY_SCOPES,
        )
        project_id = config.project_id.strip() or credentials.project_id
        return credentials, project_id
    credentials, project_id = google_auth.default(scopes=_BIGQUERY_SCOPES)
    resolved_project = config.project_id.strip() or project_id
    return credentials, resolved_project


@dataclass(slots=True)
class GcsUploadCommitResult:
    etag: str
    object_url: str
    part_count: int


class GcsMultipartUploadSession:
    def __init__(
        self,
        *,
        store: GcsMediaStore,
        object_key: str,
        mime_type: str,
        metadata: dict[str, str] | None = None,
        chunk_size: int = 4 * 1024 * 1024,
    ) -> None:
        self.store = store
        self.object_key = object_key
        self.blob = store.bucket.blob(object_key)
        self.blob.content_type = mime_type
        if metadata:
            self.blob.metadata = metadata
        self._writer: BinaryIO = self.blob.open("wb", chunk_size=max(chunk_size, 256 * 1024))
        self._part_count = 0
        self._closed = False

    def upload_part(self, chunk: bytes) -> None:
        self._writer.write(chunk)
        self._part_count += 1

    def complete(self) -> GcsUploadCommitResult:
        if not self._closed:
            self._writer.close()
            self._closed = True
        self.blob.reload()
        return GcsUploadCommitResult(
            etag=(self.blob.etag or "").strip('"'),
            object_url=self.store.build_object_url(self.object_key),
            part_count=self._part_count,
        )

    def abort(self) -> None:
        if not self._closed:
            self._writer.close()
            self._closed = True
        try:
            self.blob.delete()
        except Exception:
            pass

    @property
    def part_count(self) -> int:
        return self._part_count


class GcsMediaStore:
    def __init__(self, config: GCPStorageConfig) -> None:
        _ensure_google_cloud_dependencies()
        if not config.is_enabled():
            raise ValueError("GCP storage configuration is incomplete.")
        credentials, project_id = _build_credentials(config)
        self.config = config
        self.project_id = project_id or config.project_id.strip()
        self.client = storage.Client(project=self.project_id or None, credentials=credentials)
        self.bucket = self.client.bucket(config.bucket_name)

    @property
    def bucket_name(self) -> str:
        return self.config.bucket_name

    @property
    def endpoint(self) -> str:
        return self.config.endpoint_with_scheme()

    def build_object_url(self, object_key: str) -> str:
        key = quote(object_key.lstrip("/"))
        public_base_url = self.config.public_base_url.strip().rstrip("/")
        if public_base_url:
            return f"{public_base_url}/{key}"
        return self.config.build_gcs_uri(object_key)

    def create_multipart_upload(
        self,
        *,
        object_key: str,
        mime_type: str,
        metadata: dict[str, str] | None = None,
        chunk_size: int = 4 * 1024 * 1024,
    ) -> GcsMultipartUploadSession:
        return GcsMultipartUploadSession(
            store=self,
            object_key=object_key,
            mime_type=mime_type,
            metadata=metadata,
            chunk_size=chunk_size,
        )

    def download_object_bytes(self, object_key: str) -> bytes:
        return self.bucket.blob(object_key).download_as_bytes()

    def test_connection(self) -> dict[str, Any]:
        bucket = self.client.get_bucket(self.bucket_name)
        return {
            "project_id": self.project_id,
            "bucket_name": bucket.name,
            "location": bucket.location,
            "storage_class": bucket.storage_class,
            "public_base_url": self.config.public_base_url.strip() or None,
        }


class BigQueryCrawlerStore:
    _VIDEOS_SCHEMA = [
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("aid", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("desc_text", "STRING"),
        bigquery.SchemaField("pic", "STRING"),
        bigquery.SchemaField("dynamic_text", "STRING"),
        bigquery.SchemaField("tags_json", "STRING"),
        bigquery.SchemaField("tag_details_json", "STRING"),
        bigquery.SchemaField("videos_count", "INT64"),
        bigquery.SchemaField("tid", "INT64"),
        bigquery.SchemaField("tid_v2", "INT64"),
        bigquery.SchemaField("tname", "STRING"),
        bigquery.SchemaField("tname_v2", "STRING"),
        bigquery.SchemaField("copyright", "INT64"),
        bigquery.SchemaField("owner_mid", "INT64"),
        bigquery.SchemaField("owner_name", "STRING"),
        bigquery.SchemaField("owner_face", "STRING"),
        bigquery.SchemaField("owner_sign", "STRING"),
        bigquery.SchemaField("owner_gender", "STRING"),
        bigquery.SchemaField("owner_level", "INT64"),
        bigquery.SchemaField("owner_verified", "BOOL"),
        bigquery.SchemaField("owner_verified_title", "STRING"),
        bigquery.SchemaField("owner_vip_type", "INT64"),
        bigquery.SchemaField("owner_follower_count", "INT64"),
        bigquery.SchemaField("owner_following_count", "INT64"),
        bigquery.SchemaField("owner_video_count", "INT64"),
        bigquery.SchemaField("is_activity_participant", "BOOL"),
        bigquery.SchemaField("duration_seconds", "INT64"),
        bigquery.SchemaField("state", "INT64"),
        bigquery.SchemaField("pubdate", "STRING"),
        bigquery.SchemaField("cid", "INT64"),
        bigquery.SchemaField("resolution_width", "INT64"),
        bigquery.SchemaField("resolution_height", "INT64"),
        bigquery.SchemaField("resolution_rotate", "INT64"),
        bigquery.SchemaField("is_story", "BOOL"),
        bigquery.SchemaField("is_interactive_video", "BOOL"),
        bigquery.SchemaField("is_downloadable", "BOOL"),
        bigquery.SchemaField("is_reprint_allowed", "BOOL"),
        bigquery.SchemaField("is_collaboration", "BOOL"),
        bigquery.SchemaField("is_360", "BOOL"),
        bigquery.SchemaField("is_paid_video", "BOOL"),
        bigquery.SchemaField("pages_info_json", "STRING"),
        bigquery.SchemaField("rights_json", "STRING"),
        bigquery.SchemaField("subtitle_json", "STRING"),
        bigquery.SchemaField("uploader_profile_json", "STRING"),
        bigquery.SchemaField("uploader_relation_json", "STRING"),
        bigquery.SchemaField("uploader_overview_json", "STRING"),
        bigquery.SchemaField("raw_payload_json", "STRING"),
        bigquery.SchemaField("updated_at", "STRING"),
    ]
    _STAT_SCHEMA = [
        bigquery.SchemaField("stat_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_time", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stat_view", "INT64"),
        bigquery.SchemaField("stat_like", "INT64"),
        bigquery.SchemaField("stat_coin", "INT64"),
        bigquery.SchemaField("stat_favorite", "INT64"),
        bigquery.SchemaField("stat_share", "INT64"),
        bigquery.SchemaField("stat_reply", "INT64"),
        bigquery.SchemaField("stat_danmu", "INT64"),
        bigquery.SchemaField("stat_dislike", "INT64"),
        bigquery.SchemaField("stat_his_rank", "INT64"),
        bigquery.SchemaField("stat_now_rank", "INT64"),
        bigquery.SchemaField("stat_evaluation", "STRING"),
        bigquery.SchemaField("source_pool", "STRING"),
        bigquery.SchemaField("raw_payload_json", "STRING"),
    ]
    _COMMENT_SCHEMA = [
        bigquery.SchemaField("comment_snapshot_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_time", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("limit_n", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("items_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_payload_json", "STRING"),
    ]
    _ASSET_SCHEMA = [
        bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cid", "INT64"),
        bigquery.SchemaField("asset_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("storage_backend", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("object_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bucket_name", "STRING"),
        bigquery.SchemaField("storage_endpoint", "STRING"),
        bigquery.SchemaField("object_url", "STRING"),
        bigquery.SchemaField("format_selected", "STRING"),
        bigquery.SchemaField("mime_type", "STRING"),
        bigquery.SchemaField("file_size", "INT64"),
        bigquery.SchemaField("sha256", "STRING"),
        bigquery.SchemaField("etag", "STRING"),
        bigquery.SchemaField("chunk_count", "INT64"),
        bigquery.SchemaField("upload_session_id", "STRING"),
        bigquery.SchemaField("raw_payload_json", "STRING"),
        bigquery.SchemaField("uploaded_at", "STRING"),
    ]
    _RUN_SCHEMA = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mode", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("finished_at", "STRING"),
        bigquery.SchemaField("total_bvids", "INT64"),
        bigquery.SchemaField("success_count", "INT64"),
        bigquery.SchemaField("failed_count", "INT64"),
        bigquery.SchemaField("notes_json", "STRING"),
    ]
    _RUN_ITEM_SCHEMA = [
        bigquery.SchemaField("run_item_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_time", "STRING"),
        bigquery.SchemaField("meta_ok", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("stat_ok", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("comment_ok", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("media_ok", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("errors_json", "STRING"),
        bigquery.SchemaField("summary_json", "STRING"),
    ]

    def __init__(self, config: GCPStorageConfig) -> None:
        _ensure_google_cloud_dependencies()
        if not config.is_enabled():
            raise ValueError("GCP storage configuration is incomplete.")
        credentials, project_id = _build_credentials(config)
        self.config = config
        self._credentials = credentials
        self.project_id = project_id or config.project_id.strip()
        if not self.project_id:
            raise ValueError("Unable to resolve Google Cloud project ID.")
        self.dataset_name = config.bigquery_dataset.strip()
        self.client = self._build_bigquery_client()
        self._lock = threading.Lock()
        self._pending_runs: dict[str, dict[str, Any]] = {}
        self._pending_assets: dict[str, dict[str, Any]] = {}
        self._insert_retry_attempts = 2
        self._insert_retry_backoff_seconds = 5
        self._ensure_dataset_and_tables()

    def _dataset_ref(self) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(self.project_id, self.dataset_name)

    def _build_bigquery_client(self):
        return bigquery.Client(project=self.project_id, credentials=self._credentials)

    def _reset_bigquery_client(self) -> None:
        self.client = self._build_bigquery_client()

    def _table_id(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_name}.{table_name}"

    def _ensure_runtime_state(self) -> None:
        if not hasattr(self, "_pending_runs"):
            self._pending_runs = {}
        if not hasattr(self, "_pending_assets"):
            self._pending_assets = {}

    def _ensure_dataset_and_tables(self) -> None:
        with self._lock:
            dataset_ref = self._dataset_ref()
            dataset = bigquery.Dataset(dataset_ref)
            if self.config.gcp_region.strip():
                dataset.location = self.config.gcp_region.strip()
            self.client.create_dataset(dataset, exists_ok=True)
            for table_name, schema in (
                ("videos", self._VIDEOS_SCHEMA),
                ("video_stat_snapshots", self._STAT_SCHEMA),
                ("topn_comment_snapshots", self._COMMENT_SCHEMA),
                ("assets", self._ASSET_SCHEMA),
                ("crawl_runs", self._RUN_SCHEMA),
                ("crawl_run_items", self._RUN_ITEM_SCHEMA),
            ):
                table = bigquery.Table(self._table_id(table_name), schema=schema)
                self.client.create_table(table, exists_ok=True)

    def _query(self, sql: str, params: list[bigquery.ScalarQueryParameter] | None = None) -> list[bigquery.table.Row]:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        return list(self.client.query(sql, job_config=job_config).result())

    def _insert_row(self, table_name: str, row: dict[str, Any]) -> None:
        max_attempts = max(1, int(getattr(self, "_insert_retry_attempts", 1) or 1))
        backoff_seconds = max(0, int(getattr(self, "_insert_retry_backoff_seconds", 0) or 0))
        for attempt in range(1, max_attempts + 1):
            try:
                errors = self.client.insert_rows_json(self._table_id(table_name), [row])
            except Exception as exc:
                if attempt >= max_attempts or not _is_retryable_bigquery_insert_exception(exc):
                    raise
                self._reset_bigquery_client()
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds * attempt)
                continue
            if errors:
                raise RuntimeError(f"Failed to insert row into {table_name}: {errors}")
            return

    def _decode_json_fields(self, row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
        for field_name in fields:
            row[field_name] = _json_loads(row.get(field_name))
        return row

    def save_video_meta(self, result: MetaResult) -> None:
        self._insert_row(
            "videos",
            {
                "bvid": result.bvid,
                "aid": result.aid,
                "title": result.title,
                "desc_text": result.desc,
                "pic": result.pic,
                "dynamic_text": result.dynamic,
                "tags_json": _json_dumps(result.tags),
                "tag_details_json": _json_dumps(result.tag_details),
                "videos_count": result.videos,
                "tid": result.tid,
                "tid_v2": result.tid_v2,
                "tname": result.tname,
                "tname_v2": result.tname_v2,
                "copyright": result.copyright,
                "owner_mid": result.owner_mid,
                "owner_name": result.owner_name,
                "owner_face": result.owner_face,
                "owner_sign": result.owner_sign,
                "owner_gender": result.owner_gender,
                "owner_level": result.owner_level,
                "owner_verified": result.owner_verified,
                "owner_verified_title": result.owner_verified_title,
                "owner_vip_type": result.owner_vip_type,
                "owner_follower_count": result.owner_follower_count,
                "owner_following_count": result.owner_following_count,
                "owner_video_count": result.owner_video_count,
                "is_activity_participant": result.is_activity_participant,
                "duration_seconds": result.duration,
                "state": result.state,
                "pubdate": result.pubdate.isoformat() if result.pubdate else None,
                "cid": result.cid,
                "resolution_width": result.resolution_width,
                "resolution_height": result.resolution_height,
                "resolution_rotate": result.resolution_rotate,
                "is_story": result.is_story,
                "is_interactive_video": result.is_interactive_video,
                "is_downloadable": result.is_downloadable,
                "is_reprint_allowed": result.is_reprint_allowed,
                "is_collaboration": result.is_collaboration,
                "is_360": result.is_360,
                "is_paid_video": result.is_paid_video,
                "pages_info_json": _json_dumps(result.pages_info),
                "rights_json": _json_dumps(result.rights),
                "subtitle_json": _json_dumps(result.subtitle),
                "uploader_profile_json": _json_dumps(result.uploader_profile),
                "uploader_relation_json": _json_dumps(result.uploader_relation),
                "uploader_overview_json": _json_dumps(result.uploader_overview),
                "raw_payload_json": _json_dumps(result.raw_payload),
                "updated_at": _timestamp_now(),
            },
        )

    def save_stat_snapshot(self, snapshot: StatSnapshot) -> None:
        self._insert_row(
            "video_stat_snapshots",
            {
                "stat_id": uuid.uuid4().hex,
                "bvid": snapshot.bvid,
                "snapshot_time": snapshot.snapshot_time.isoformat(),
                "stat_view": snapshot.stat_view,
                "stat_like": snapshot.stat_like,
                "stat_coin": snapshot.stat_coin,
                "stat_favorite": snapshot.stat_favorite,
                "stat_share": snapshot.stat_share,
                "stat_reply": snapshot.stat_reply,
                "stat_danmu": snapshot.stat_danmu,
                "stat_dislike": snapshot.stat_dislike,
                "stat_his_rank": snapshot.stat_his_rank,
                "stat_now_rank": snapshot.stat_now_rank,
                "stat_evaluation": snapshot.stat_evaluation,
                "source_pool": snapshot.source_pool,
                "raw_payload_json": _json_dumps(snapshot.raw_payload),
            },
        )

    def save_comment_snapshot(self, snapshot: CommentSnapshot) -> None:
        self._insert_row(
            "topn_comment_snapshots",
            {
                "comment_snapshot_id": uuid.uuid4().hex,
                "bvid": snapshot.bvid,
                "snapshot_time": snapshot.snapshot_time.isoformat(),
                "limit_n": snapshot.limit,
                "items_json": _json_dumps([item.to_dict() for item in snapshot.items]),
                "raw_payload_json": _json_dumps(snapshot.raw_payload),
            },
        )

    def begin_media_asset(
        self,
        *,
        bvid: str,
        cid: int | None,
        asset_type: str,
        storage_backend: str,
        object_key: str,
        bucket_name: str | None,
        storage_endpoint: str | None,
        object_url: str | None,
        format_selected: str,
        mime_type: str,
        upload_session_id: str,
        raw_payload: dict[str, Any] | None = None,
    ) -> str:
        self._ensure_runtime_state()
        asset_id = uuid.uuid4().hex
        self._pending_assets[asset_id] = {
            "asset_id": asset_id,
            "bvid": bvid,
            "cid": cid,
            "asset_type": asset_type,
            "storage_backend": storage_backend,
            "object_key": object_key,
            "bucket_name": bucket_name,
            "storage_endpoint": storage_endpoint,
            "object_url": object_url,
            "format_selected": format_selected,
            "mime_type": mime_type,
            "upload_session_id": upload_session_id,
            "raw_payload_json": _json_dumps(raw_payload or {}),
        }
        return asset_id

    def finalize_media_asset(
        self,
        asset_id: str,
        *,
        file_size: int,
        sha256: str,
        chunk_count: int,
        etag: str = "",
    ) -> None:
        self._ensure_runtime_state()
        pending = self._pending_assets.pop(asset_id, None)
        if pending is None:
            raise KeyError(f"Unknown pending asset_id: {asset_id}")
        self._insert_row(
            "assets",
            {
                **pending,
                "file_size": file_size,
                "sha256": sha256,
                "etag": etag,
                "chunk_count": chunk_count,
                "uploaded_at": _timestamp_now(),
            },
        )

    def save_run_start(self, run_id: str, mode: str, notes: dict[str, Any]) -> None:
        self._ensure_runtime_state()
        self._pending_runs[run_id] = {
            "run_id": run_id,
            "mode": mode,
            "started_at": _timestamp_now(),
            "notes_json": _json_dumps(notes),
        }

    def save_run_item(self, run_id: str, summary: FullCrawlSummary) -> None:
        self._insert_row(
            "crawl_run_items",
            {
                "run_item_id": uuid.uuid4().hex,
                "run_id": run_id,
                "bvid": summary.bvid,
                "snapshot_time": summary.snapshot_time.isoformat() if summary.snapshot_time else None,
                "meta_ok": summary.meta_ok,
                "stat_ok": summary.stat_ok,
                "comment_ok": summary.comment_ok,
                "media_ok": summary.media_ok,
                "errors_json": _json_dumps(summary.errors),
                "summary_json": _json_dumps(summary.to_dict()),
            },
        )

    def finalize_run(self, run_id: str, *, total_bvids: int, success_count: int, failed_count: int) -> None:
        self._ensure_runtime_state()
        pending = self._pending_runs.pop(run_id, None) or {
            "run_id": run_id,
            "mode": "unknown",
            "started_at": _timestamp_now(),
            "notes_json": _json_dumps({}),
        }
        self._insert_row(
            "crawl_runs",
            {
                **pending,
                "finished_at": _timestamp_now(),
                "total_bvids": total_bvids,
                "success_count": success_count,
                "failed_count": failed_count,
            },
        )

    def export_manual_media_waitlist_rows(self) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            WITH candidate_bvids AS (
                SELECT bvid, TRUE AS has_stat_snapshot, FALSE AS has_comment_snapshot
                FROM `{self._table_id('video_stat_snapshots')}`
                UNION ALL
                SELECT bvid, FALSE AS has_stat_snapshot, TRUE AS has_comment_snapshot
                FROM `{self._table_id('topn_comment_snapshots')}`
            ),
            candidate_rollup AS (
                SELECT
                    bvid,
                    LOGICAL_OR(has_stat_snapshot) AS has_stat_snapshot,
                    LOGICAL_OR(has_comment_snapshot) AS has_comment_snapshot
                FROM candidate_bvids
                GROUP BY bvid
            ),
            completed_bvids AS (
                SELECT DISTINCT v.bvid
                FROM `{self._table_id('videos')}` AS v
                INNER JOIN (
                    SELECT bvid
                    FROM `{self._table_id('assets')}`
                    WHERE asset_type = 'video' OR asset_type = 'audio'
                    GROUP BY bvid
                    HAVING COUNT(DISTINCT asset_type) = 2
                ) AS ready_assets
                ON ready_assets.bvid = v.bvid
            )
            SELECT
                candidate_rollup.bvid,
                candidate_rollup.has_stat_snapshot,
                candidate_rollup.has_comment_snapshot
            FROM candidate_rollup
            LEFT JOIN completed_bvids
            ON completed_bvids.bvid = candidate_rollup.bvid
            WHERE completed_bvids.bvid IS NULL
            ORDER BY candidate_rollup.bvid
            """
        )
        return [dict(row.items()) for row in rows]

    def fetch_completed_media_metadata_bvids(self, bvids: list[str]) -> set[str]:
        normalized_bvids = [str(bvid).strip() for bvid in bvids if str(bvid).strip()]
        if not normalized_bvids:
            return set()
        unique_bvids = list(dict.fromkeys(normalized_bvids))
        rows = self._query(
            f"""
            WITH input_bvids AS (
                SELECT bvid
                FROM UNNEST(@bvids) AS bvid
            ),
            completed_bvids AS (
                SELECT DISTINCT input_bvids.bvid
                FROM input_bvids
                INNER JOIN `{self._table_id('videos')}` AS v
                ON v.bvid = input_bvids.bvid
                INNER JOIN (
                    SELECT bvid
                    FROM `{self._table_id('assets')}`
                    WHERE asset_type = 'video' OR asset_type = 'audio'
                    GROUP BY bvid
                    HAVING COUNT(DISTINCT asset_type) = 2
                ) AS ready_assets
                ON ready_assets.bvid = input_bvids.bvid
            )
            SELECT bvid
            FROM completed_bvids
            ORDER BY bvid
            """,
            [bigquery.ArrayQueryParameter("bvids", "STRING", unique_bvids)],
        )
        return {str(row["bvid"]).strip() for row in rows if str(row["bvid"]).strip()}

    def fetch_all_asset_rows(self, bvid: str) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT bvid, cid, asset_type, storage_backend, object_key, bucket_name,
                   storage_endpoint, object_url, format_selected, mime_type,
                   file_size, sha256, etag, chunk_count, upload_session_id, uploaded_at
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY bvid, cid, asset_type
                           ORDER BY uploaded_at DESC, asset_id DESC
                       ) AS row_num
                FROM `{self._table_id('assets')}`
                WHERE bvid = @bvid
            )
            WHERE row_num = 1
            ORDER BY asset_type
            """,
            [bigquery.ScalarQueryParameter("bvid", "STRING", bvid)],
        )
        return [dict(row.items()) for row in rows]

    def fetch_video_row(self, bvid: str) -> dict[str, Any] | None:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self._table_id('videos')}`
            WHERE bvid = @bvid
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            [bigquery.ScalarQueryParameter("bvid", "STRING", bvid)],
        )
        if not rows:
            return None
        return self._decode_json_fields(
            dict(rows[0].items()),
            (
                "tags_json",
                "tag_details_json",
                "pages_info_json",
                "rights_json",
                "subtitle_json",
                "uploader_profile_json",
                "uploader_relation_json",
                "uploader_overview_json",
                "raw_payload_json",
            ),
        )

    def fetch_latest_stat_snapshot_row(self, bvid: str) -> dict[str, Any] | None:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self._table_id('video_stat_snapshots')}`
            WHERE bvid = @bvid
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            [bigquery.ScalarQueryParameter("bvid", "STRING", bvid)],
        )
        if not rows:
            return None
        return self._decode_json_fields(dict(rows[0].items()), ("raw_payload_json",))

    def fetch_latest_comment_snapshot_row(self, bvid: str) -> dict[str, Any] | None:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self._table_id('topn_comment_snapshots')}`
            WHERE bvid = @bvid
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            [bigquery.ScalarQueryParameter("bvid", "STRING", bvid)],
        )
        if not rows:
            return None
        return self._decode_json_fields(dict(rows[0].items()), ("items_json", "raw_payload_json"))

    def build_media_result(self, bvid: str, cid: int | None, upload_session_id: str) -> MediaResult:
        rows = self.fetch_all_asset_rows(bvid)
        video_asset: MediaAssetRef | None = None
        audio_asset: MediaAssetRef | None = None
        for row in rows:
            asset = MediaAssetRef(
                asset_type=row["asset_type"],
                storage_backend=row["storage_backend"] or "gcs",
                object_key=row["object_key"],
                format_selected=row["format_selected"] or "",
                mime_type=row["mime_type"] or "application/octet-stream",
                file_size=row["file_size"] or 0,
                sha256=row["sha256"] or "",
                chunk_count=row["chunk_count"] or 0,
                bucket_name=row.get("bucket_name"),
                storage_endpoint=row.get("storage_endpoint"),
                object_url=row.get("object_url"),
                etag=row.get("etag") or None,
            )
            if row["asset_type"] == "video":
                video_asset = asset
            elif row["asset_type"] == "audio":
                audio_asset = asset
        return MediaResult(
            bvid=bvid,
            cid=cid,
            video_asset=video_asset,
            audio_asset=audio_asset,
            upload_session_id=upload_session_id,
        )

    def test_connection(self) -> dict[str, Any]:
        dataset = self.client.get_dataset(self._dataset_ref())
        return {
            "project_id": self.project_id,
            "dataset_id": dataset.dataset_id,
            "location": dataset.location,
            "full_dataset_id": f"{dataset.project}.{dataset.dataset_id}",
        }
