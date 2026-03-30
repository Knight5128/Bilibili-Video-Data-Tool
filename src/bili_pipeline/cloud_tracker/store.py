from __future__ import annotations

import csv
import io
import json
import types
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

try:
    from google.cloud import bigquery

    _HAS_GOOGLE_CLOUD = True
except ModuleNotFoundError:  # pragma: no cover - exercised in dependency-light test environments.
    _HAS_GOOGLE_CLOUD = False

    class _MissingGoogleClass:
        def __init__(self, *args, **kwargs) -> None:
            raise ModuleNotFoundError("google-cloud dependencies are required to use TrackerStore.")

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
        LoadJobConfig=_MissingGoogleClass,
        SchemaField=_schema_field,
        table=types.SimpleNamespace(Row=dict),
    )

from bili_pipeline.models import GCPStorageConfig
from bili_pipeline.storage.gcp_store import _build_credentials


def _ensure_google_cloud_dependencies() -> None:
    if not _HAS_GOOGLE_CLOUD:
        raise ModuleNotFoundError("google-cloud dependencies are required to use TrackerStore.")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_iso(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return default


@dataclass(slots=True)
class DiscoveredVideoRow:
    bvid: str
    owner_mid: int | None
    pubdate: datetime | None
    discovered_at: datetime
    tracking_deadline: datetime | None
    status: str
    discovery_sources: list[str]


class TrackerStore:
    _AUTHOR_SCHEMA = [
        bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("owner_mid", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("enabled", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("uploaded_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_checked_at", "STRING"),
        bigquery.SchemaField("last_success_at", "STRING"),
        bigquery.SchemaField("last_error", "STRING"),
        bigquery.SchemaField("source_payload_json", "STRING"),
    ]
    _WATCHLIST_SCHEMA = [
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("owner_mid", "INT64"),
        bigquery.SchemaField("pubdate", "STRING"),
        bigquery.SchemaField("first_discovered_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_discovered_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tracking_deadline", "STRING"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("discovery_sources_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_stat_snapshot_at", "STRING"),
        bigquery.SchemaField("last_comment_snapshot_at", "STRING"),
        bigquery.SchemaField("last_error", "STRING"),
        bigquery.SchemaField("last_error_at", "STRING"),
        bigquery.SchemaField("stat_fail_count", "INT64"),
        bigquery.SchemaField("comment_fail_count", "INT64"),
    ]
    _QUEUE_SCHEMA = [
        bigquery.SchemaField("bvid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("owner_mid", "INT64"),
        bigquery.SchemaField("pubdate", "STRING"),
        bigquery.SchemaField("discovered_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_discovered_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("discovery_sources_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("exported_at", "STRING"),
    ]
    _CONTROL_SCHEMA = [
        bigquery.SchemaField("control_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_interval_hours", "INT64"),
        bigquery.SchemaField("tracking_window_days", "INT64"),
        bigquery.SchemaField("comment_limit", "INT64"),
        bigquery.SchemaField("author_bootstrap_days", "INT64"),
        bigquery.SchemaField("max_videos_per_cycle", "INT64"),
        bigquery.SchemaField("paused_until", "STRING"),
        bigquery.SchemaField("pause_reason", "STRING"),
        bigquery.SchemaField("consecutive_risk_hits", "INT64"),
        bigquery.SchemaField("last_risk_at", "STRING"),
        bigquery.SchemaField("lock_owner", "STRING"),
        bigquery.SchemaField("lock_until", "STRING"),
        bigquery.SchemaField("updated_at", "STRING", mode="REQUIRED"),
    ]
    _RUN_LOG_SCHEMA = [
        bigquery.SchemaField("log_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("finished_at", "STRING"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phase", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("discovered_count", "INT64"),
        bigquery.SchemaField("tracked_count", "INT64"),
        bigquery.SchemaField("success_count", "INT64"),
        bigquery.SchemaField("failed_count", "INT64"),
        bigquery.SchemaField("skipped_count", "INT64"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("details_json", "STRING"),
    ]

    def __init__(self, config: GCPStorageConfig, *, table_prefix: str = "tracker") -> None:
        _ensure_google_cloud_dependencies()
        credentials, project_id = _build_credentials(config)
        self.config = config
        self.project_id = (project_id or config.project_id).strip()
        if not self.project_id:
            raise ValueError("Unable to resolve Google Cloud project ID for tracker store.")
        self.dataset_name = config.bigquery_dataset.strip()
        self.table_prefix = table_prefix.strip().strip("_") or "tracker"
        self.client = bigquery.Client(project=self.project_id, credentials=credentials)
        self._ensure_dataset_and_tables()

    def _dataset_ref(self) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(self.project_id, self.dataset_name)

    def table_id(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_name}.{table_name}"

    def tracker_table_id(self, suffix: str) -> str:
        return self.table_id(f"{self.table_prefix}_{suffix}")

    def _ensure_dataset_and_tables(self) -> None:
        dataset_ref = self._dataset_ref()
        dataset = bigquery.Dataset(dataset_ref)
        if self.config.gcp_region.strip():
            dataset.location = self.config.gcp_region.strip()
        self.client.create_dataset(dataset, exists_ok=True)
        for name, schema in (
            ("author_sources", self._AUTHOR_SCHEMA),
            ("video_watchlist", self._WATCHLIST_SCHEMA),
            ("meta_media_queue", self._QUEUE_SCHEMA),
            ("run_control", self._CONTROL_SCHEMA),
            ("run_logs", self._RUN_LOG_SCHEMA),
        ):
            self.client.create_table(bigquery.Table(self.tracker_table_id(name), schema=schema), exists_ok=True)

    def _query(
        self,
        sql: str,
        params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] | None = None,
    ) -> list[bigquery.table.Row]:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        return list(self.client.query(sql, job_config=job_config).result())

    def _append_rows(
        self,
        *,
        table_id: str,
        schema: list[bigquery.SchemaField],
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        if hasattr(self.client, "load_table_from_json") and _HAS_GOOGLE_CLOUD:
            job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
            self.client.load_table_from_json(rows, table_id, job_config=job_config).result()
            return
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Failed to append rows to {table_id}: {errors}")

    def _load_temp_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        schema: list[bigquery.SchemaField],
        suffix: str,
    ) -> str:
        table_name = f"{self.table_prefix}_tmp_{suffix}_{uuid.uuid4().hex[:12]}"
        table_id = self.table_id(table_name)
        table = bigquery.Table(table_id, schema=schema)
        table.expires = _utcnow() + timedelta(hours=1)
        self.client.create_table(table)
        if rows:
            job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
            self.client.load_table_from_json(rows, table_id, job_config=job_config).result()
        return table_id

    def _delete_table_if_exists(self, table_id: str) -> None:
        try:
            self.client.delete_table(table_id, not_found_ok=True)
        except Exception:
            pass

    def ensure_control_row(self, defaults: dict[str, Any]) -> dict[str, Any]:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self.tracker_table_id('run_control')}`
            WHERE control_key = 'singleton'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        if not rows:
            row = {
                "control_key": "singleton",
                "crawl_interval_hours": defaults.get("crawl_interval_hours"),
                "tracking_window_days": defaults.get("tracking_window_days"),
                "comment_limit": defaults.get("comment_limit"),
                "author_bootstrap_days": defaults.get("author_bootstrap_days"),
                "max_videos_per_cycle": defaults.get("max_videos_per_cycle"),
                "paused_until": defaults.get("paused_until"),
                "pause_reason": defaults.get("pause_reason", ""),
                "consecutive_risk_hits": defaults.get("consecutive_risk_hits", 0),
                "last_risk_at": defaults.get("last_risk_at"),
                "lock_owner": "",
                "lock_until": None,
                "updated_at": _iso(_utcnow()),
            }
            self._append_rows(
                table_id=self.tracker_table_id("run_control"),
                schema=self._CONTROL_SCHEMA,
                rows=[row],
            )
            return self._normalize_control_row(row)
        return self._normalize_control_row(dict(rows[0].items()))

    def _normalize_control_row(self, row: dict[str, Any]) -> dict[str, Any]:
        row["paused_until"] = _parse_iso(row.get("paused_until"))
        row["last_risk_at"] = _parse_iso(row.get("last_risk_at"))
        row["lock_until"] = _parse_iso(row.get("lock_until"))
        return row

    def get_control(self) -> dict[str, Any]:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self.tracker_table_id('run_control')}`
            WHERE control_key = 'singleton'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        if not rows:
            raise RuntimeError("Tracker control row is missing.")
        return self._normalize_control_row(dict(rows[0].items()))

    def update_control(self, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.get_control()
        next_row = {
            "control_key": "singleton",
            "crawl_interval_hours": int(updates.get("crawl_interval_hours", current.get("crawl_interval_hours") or 0)),
            "tracking_window_days": int(updates.get("tracking_window_days", current.get("tracking_window_days") or 0)),
            "comment_limit": int(updates.get("comment_limit", current.get("comment_limit") or 0)),
            "author_bootstrap_days": int(updates.get("author_bootstrap_days", current.get("author_bootstrap_days") or 0)),
            "max_videos_per_cycle": int(updates.get("max_videos_per_cycle", current.get("max_videos_per_cycle") or 0)),
            "paused_until": _iso(updates["paused_until"]) if "paused_until" in updates else _iso(current.get("paused_until")),
            "pause_reason": str(updates.get("pause_reason", current.get("pause_reason") or "")),
            "consecutive_risk_hits": int(updates.get("consecutive_risk_hits", current.get("consecutive_risk_hits") or 0)),
            "last_risk_at": _iso(updates["last_risk_at"]) if "last_risk_at" in updates else _iso(current.get("last_risk_at")),
            "lock_owner": str(updates.get("lock_owner", current.get("lock_owner") or "")),
            "lock_until": _iso(updates["lock_until"]) if "lock_until" in updates else _iso(current.get("lock_until")),
            "updated_at": _iso(_utcnow()),
        }
        self._append_rows(
            table_id=self.tracker_table_id("run_control"),
            schema=self._CONTROL_SCHEMA,
            rows=[next_row],
        )
        return self.get_control()

    def acquire_lock(self, run_id: str, ttl_minutes: int) -> bool:
        current = self.get_control()
        now = _utcnow()
        lock_until = current.get("lock_until")
        if lock_until is not None and lock_until > now and current.get("lock_owner") not in ("", run_id, None):
            return False
        self.update_control(
            {
                "lock_owner": run_id,
                "lock_until": now + timedelta(minutes=max(1, ttl_minutes)),
            }
        )
        refreshed = self.get_control()
        return refreshed.get("lock_owner") == run_id

    def release_lock(self, run_id: str) -> None:
        current = self.get_control()
        if current.get("lock_owner") != run_id:
            return
        self.update_control({"lock_owner": "", "lock_until": None})

    def replace_author_sources(
        self,
        *,
        owner_mids: Iterable[int],
        source_name: str,
        payload: dict[str, Any] | None = None,
    ) -> int:
        normalized = sorted({int(owner_mid) for owner_mid in owner_mids})
        self._query(f"DELETE FROM `{self.tracker_table_id('author_sources')}` WHERE TRUE")
        uploaded_at = _iso(_utcnow())
        rows = [
            {
                "source_id": uuid.uuid4().hex,
                "source_name": source_name,
                "owner_mid": owner_mid,
                "enabled": True,
                "uploaded_at": uploaded_at,
                "updated_at": uploaded_at,
                "last_checked_at": None,
                "last_success_at": None,
                "last_error": "",
                "source_payload_json": _json_dumps(payload or {}),
            }
            for owner_mid in normalized
        ]
        self._append_rows(
            table_id=self.tracker_table_id("author_sources"),
            schema=self._AUTHOR_SCHEMA,
            rows=rows,
        )
        return len(normalized)

    def list_author_sources(self) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT source_name, owner_mid, enabled, uploaded_at, updated_at,
                   last_checked_at, last_success_at, last_error
            FROM `{self.tracker_table_id('author_sources')}`
            WHERE enabled = TRUE
            ORDER BY owner_mid
            """
        )
        return [dict(row.items()) for row in rows]

    def count_active_authors(self) -> int:
        rows = self._query(
            f"""
            SELECT COUNT(1) AS total
            FROM `{self.tracker_table_id('author_sources')}`
            WHERE enabled = TRUE
            """
        )
        return int(rows[0]["total"]) if rows else 0

    def mark_author_checked(self, owner_mid: int, *, success: bool, error: str = "") -> None:
        now = _iso(_utcnow())
        self._query(
            f"""
            UPDATE `{self.tracker_table_id('author_sources')}`
            SET last_checked_at = @now,
                last_success_at = IF(@success, @now, last_success_at),
                last_error = @error,
                updated_at = @now
            WHERE owner_mid = @owner_mid
            """,
            [
                bigquery.ScalarQueryParameter("now", "STRING", now),
                bigquery.ScalarQueryParameter("success", "BOOL", success),
                bigquery.ScalarQueryParameter("error", "STRING", error),
                bigquery.ScalarQueryParameter("owner_mid", "INT64", owner_mid),
            ],
        )

    def upsert_discovered_videos(self, videos: list[DiscoveredVideoRow]) -> int:
        if not videos:
            return 0
        stage_rows = [
            {
                "bvid": item.bvid,
                "owner_mid": item.owner_mid,
                "pubdate": _iso(item.pubdate),
                "first_discovered_at": _iso(item.discovered_at),
                "last_discovered_at": _iso(item.discovered_at),
                "tracking_deadline": _iso(item.tracking_deadline),
                "status": item.status,
                "discovery_sources_json": _json_dumps(item.discovery_sources),
            }
            for item in videos
        ]
        stage_id = self._load_temp_rows(rows=stage_rows, schema=self._WATCHLIST_SCHEMA, suffix="watchlist")
        try:
            self._query(
                f"""
                MERGE `{self.tracker_table_id('video_watchlist')}` AS target
                USING `{stage_id}` AS stage
                ON target.bvid = stage.bvid
                WHEN MATCHED THEN
                  UPDATE SET
                    owner_mid = COALESCE(target.owner_mid, stage.owner_mid),
                    pubdate = COALESCE(target.pubdate, stage.pubdate),
                    first_discovered_at = IF(target.first_discovered_at < stage.first_discovered_at, target.first_discovered_at, stage.first_discovered_at),
                    last_discovered_at = IF(target.last_discovered_at > stage.last_discovered_at, target.last_discovered_at, stage.last_discovered_at),
                    tracking_deadline = COALESCE(stage.tracking_deadline, target.tracking_deadline),
                    status = IF(stage.status = 'active', 'active', target.status),
                    discovery_sources_json = stage.discovery_sources_json
                WHEN NOT MATCHED THEN
                  INSERT (bvid, owner_mid, pubdate, first_discovered_at, last_discovered_at, tracking_deadline, status, discovery_sources_json, last_stat_snapshot_at, last_comment_snapshot_at, last_error, last_error_at, stat_fail_count, comment_fail_count)
                  VALUES (stage.bvid, stage.owner_mid, stage.pubdate, stage.first_discovered_at, stage.last_discovered_at, stage.tracking_deadline, stage.status, stage.discovery_sources_json, NULL, NULL, '', NULL, 0, 0)
                """
            )
        finally:
            self._delete_table_if_exists(stage_id)

        queue_rows = [
            {
                "bvid": item.bvid,
                "owner_mid": item.owner_mid,
                "pubdate": _iso(item.pubdate),
                "discovered_at": _iso(item.discovered_at),
                "last_discovered_at": _iso(item.discovered_at),
                "discovery_sources_json": _json_dumps(item.discovery_sources),
                "exported_at": None,
            }
            for item in videos
        ]
        stage_id = self._load_temp_rows(rows=queue_rows, schema=self._QUEUE_SCHEMA, suffix="queue")
        try:
            self._query(
                f"""
                MERGE `{self.tracker_table_id('meta_media_queue')}` AS target
                USING `{stage_id}` AS stage
                ON target.bvid = stage.bvid
                WHEN MATCHED THEN
                  UPDATE SET
                    owner_mid = COALESCE(target.owner_mid, stage.owner_mid),
                    pubdate = COALESCE(target.pubdate, stage.pubdate),
                    discovered_at = IF(target.discovered_at < stage.discovered_at, target.discovered_at, stage.discovered_at),
                    last_discovered_at = IF(target.last_discovered_at > stage.last_discovered_at, target.last_discovered_at, stage.last_discovered_at),
                    discovery_sources_json = stage.discovery_sources_json
                WHEN NOT MATCHED THEN
                  INSERT (bvid, owner_mid, pubdate, discovered_at, last_discovered_at, discovery_sources_json, exported_at)
                  VALUES (stage.bvid, stage.owner_mid, stage.pubdate, stage.discovered_at, stage.last_discovered_at, stage.discovery_sources_json, NULL)
                """
            )
        finally:
            self._delete_table_if_exists(stage_id)
        return len(videos)

    def list_active_watch_videos(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT bvid, owner_mid, pubdate, first_discovered_at, last_discovered_at,
                   tracking_deadline, status, discovery_sources_json,
                   last_stat_snapshot_at, last_comment_snapshot_at,
                   stat_fail_count, comment_fail_count
            FROM `{self.tracker_table_id('video_watchlist')}`
            WHERE status = 'active'
              AND (tracking_deadline IS NULL OR tracking_deadline >= @now)
            ORDER BY COALESCE(last_stat_snapshot_at, first_discovered_at), first_discovered_at
            LIMIT @limit
            """,
            [
                bigquery.ScalarQueryParameter("now", "STRING", _iso(_utcnow())),
                bigquery.ScalarQueryParameter("limit", "INT64", max(1, limit)),
            ],
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row.items())
            item["pubdate"] = _parse_iso(item.get("pubdate"))
            item["tracking_deadline"] = _parse_iso(item.get("tracking_deadline"))
            item["last_stat_snapshot_at"] = _parse_iso(item.get("last_stat_snapshot_at"))
            item["last_comment_snapshot_at"] = _parse_iso(item.get("last_comment_snapshot_at"))
            item["discovery_sources"] = _json_loads(item.pop("discovery_sources_json", "[]"), [])
            result.append(item)
        return result

    def expire_watchlist(self) -> int:
        self._query(
            f"""
            UPDATE `{self.tracker_table_id('video_watchlist')}`
            SET status = 'expired'
            WHERE status = 'active'
              AND tracking_deadline IS NOT NULL
              AND tracking_deadline < @now
            """,
            [bigquery.ScalarQueryParameter("now", "STRING", _iso(_utcnow()))],
        )
        rows = self._query(
            f"SELECT COUNT(1) AS total FROM `{self.tracker_table_id('video_watchlist')}` WHERE status = 'expired'"
        )
        return int(rows[0]["total"]) if rows else 0

    def record_snapshot_success(
        self,
        bvid: str,
        *,
        stat_time: datetime | None = None,
        comment_time: datetime | None = None,
    ) -> None:
        now = _iso(_utcnow())
        assignments = ["last_error = ''", "last_error_at = NULL"]
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("bvid", "STRING", bvid),
            bigquery.ScalarQueryParameter("now", "STRING", now),
        ]
        if stat_time is not None:
            assignments.extend(["last_stat_snapshot_at = @stat_time", "stat_fail_count = 0"])
            params.append(bigquery.ScalarQueryParameter("stat_time", "STRING", _iso(stat_time)))
        if comment_time is not None:
            assignments.extend(["last_comment_snapshot_at = @comment_time", "comment_fail_count = 0"])
            params.append(bigquery.ScalarQueryParameter("comment_time", "STRING", _iso(comment_time)))
        assignments.append("status = 'active'")
        self._query(
            f"""
            UPDATE `{self.tracker_table_id('video_watchlist')}`
            SET {", ".join(assignments)}
            WHERE bvid = @bvid
            """,
            params,
        )

    def record_snapshot_failure(self, bvid: str, *, stage: str, error: str) -> None:
        counter_field = "stat_fail_count" if stage == "stat" else "comment_fail_count"
        self._query(
            f"""
            UPDATE `{self.tracker_table_id('video_watchlist')}`
            SET {counter_field} = COALESCE({counter_field}, 0) + 1,
                last_error = @error,
                last_error_at = @now
            WHERE bvid = @bvid
            """,
            [
                bigquery.ScalarQueryParameter("error", "STRING", error[:1024]),
                bigquery.ScalarQueryParameter("now", "STRING", _iso(_utcnow())),
                bigquery.ScalarQueryParameter("bvid", "STRING", bvid),
            ],
        )

    def apply_risk_backoff(self, *, reason: str, base_minutes: int, max_minutes: int) -> dict[str, Any]:
        control = self.get_control()
        hits = int(control.get("consecutive_risk_hits") or 0) + 1
        minutes = min(max(1, base_minutes) * (2 ** max(0, hits - 1)), max(1, max_minutes))
        paused_until = _utcnow() + timedelta(minutes=minutes)
        return self.update_control(
            {
                "paused_until": paused_until,
                "pause_reason": reason[:1024],
                "consecutive_risk_hits": hits,
                "last_risk_at": _utcnow(),
            }
        )

    def clear_risk_backoff(self) -> dict[str, Any]:
        return self.update_control(
            {
                "paused_until": None,
                "pause_reason": "",
                "consecutive_risk_hits": 0,
                "last_risk_at": None,
            }
        )

    def insert_run_log(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime | None,
        status: str,
        phase: str,
        message: str,
        details: dict[str, Any] | None = None,
        discovered_count: int | None = None,
        tracked_count: int | None = None,
        success_count: int | None = None,
        failed_count: int | None = None,
        skipped_count: int | None = None,
    ) -> None:
        row = {
            "log_id": uuid.uuid4().hex,
            "run_id": run_id,
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "status": status,
            "phase": phase,
            "discovered_count": discovered_count,
            "tracked_count": tracked_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "message": message[:1024],
            "details_json": _json_dumps(details or {}),
        }
        self._append_rows(
            table_id=self.tracker_table_id("run_logs"),
            schema=self._RUN_LOG_SCHEMA,
            rows=[row],
        )

    def list_recent_run_logs(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT *
            FROM `{self.tracker_table_id('run_logs')}`
            ORDER BY started_at DESC
            LIMIT @limit
            """,
            [bigquery.ScalarQueryParameter("limit", "INT64", max(1, limit))],
        )
        result = []
        for row in rows:
            item = dict(row.items())
            item["details"] = _json_loads(item.pop("details_json", "{}"), {})
            result.append(item)
        return result

    def dashboard_metrics(self) -> dict[str, Any]:
        rows = self._query(
            f"""
            SELECT
              (SELECT COUNT(1) FROM `{self.tracker_table_id('author_sources')}` WHERE enabled = TRUE) AS active_author_count,
              (SELECT COUNT(1) FROM `{self.tracker_table_id('video_watchlist')}` WHERE status = 'active') AS active_watch_video_count,
              (SELECT COUNT(1) FROM `{self.tracker_table_id('video_watchlist')}` WHERE status = 'expired') AS expired_watch_video_count,
              (SELECT COUNT(1) FROM `{self.tracker_table_id('meta_media_queue')}`) AS meta_media_queue_total,
              (SELECT COUNT(1)
                 FROM `{self.tracker_table_id('meta_media_queue')}` AS q
                 LEFT JOIN `{self.table_id('videos')}` AS v
                   ON v.bvid = q.bvid
                 LEFT JOIN (
                     SELECT bvid, COUNT(DISTINCT IF(asset_type IN ('video', 'audio'), asset_type, NULL)) AS asset_type_count
                     FROM `{self.table_id('assets')}`
                     GROUP BY bvid
                 ) AS a
                   ON a.bvid = q.bvid
                 WHERE v.bvid IS NULL OR COALESCE(a.asset_type_count, 0) < 2) AS meta_media_queue_pending,
              (SELECT COUNT(1) FROM `{self.table_id('video_stat_snapshots')}`) AS stat_snapshot_total,
              (SELECT COUNT(1)
                 FROM `{self.table_id('video_stat_snapshots')}`
                 WHERE snapshot_time >= @window_start) AS stat_snapshot_last_24h,
              (SELECT COUNT(1) FROM `{self.table_id('topn_comment_snapshots')}`) AS comment_snapshot_total,
              (SELECT COUNT(1)
                 FROM `{self.table_id('topn_comment_snapshots')}`
                 WHERE snapshot_time >= @window_start) AS comment_snapshot_last_24h,
              (SELECT COUNT(1)
                 FROM `{self.tracker_table_id('run_logs')}`
                 WHERE started_at >= @window_start) AS run_logs_last_24h
            """
            ,
            [bigquery.ScalarQueryParameter("window_start", "STRING", _iso(_utcnow() - timedelta(hours=24)))],
        )
        if not rows:
            return {}
        return dict(rows[0].items())

    def export_meta_media_queue_rows(self) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT
              q.bvid,
              q.owner_mid,
              q.pubdate,
              q.discovered_at,
              q.last_discovered_at,
              q.discovery_sources_json,
              q.exported_at,
              IF(v.bvid IS NULL, FALSE, TRUE) AS meta_crawled,
              IF(COUNT(DISTINCT IF(a.asset_type IN ('video', 'audio'), a.asset_type, NULL)) = 2, TRUE, FALSE) AS media_crawled
            FROM `{self.tracker_table_id('meta_media_queue')}` AS q
            LEFT JOIN `{self.table_id('videos')}` AS v
              ON v.bvid = q.bvid
            LEFT JOIN `{self.table_id('assets')}` AS a
              ON a.bvid = q.bvid
            GROUP BY
              q.bvid, q.owner_mid, q.pubdate, q.discovered_at, q.last_discovered_at,
              q.discovery_sources_json, q.exported_at, meta_crawled
            ORDER BY q.discovered_at DESC
            """
        )
        result = []
        for row in rows:
            item = dict(row.items())
            item["discovery_sources"] = _json_loads(item.pop("discovery_sources_json", "[]"), [])
            result.append(item)
        return result

    def mark_queue_exported(self, bvids: list[str]) -> None:
        normalized = sorted(set(bvids))
        if not normalized:
            return
        self._query(
            f"""
            UPDATE `{self.tracker_table_id('meta_media_queue')}`
            SET exported_at = @exported_at
            WHERE bvid IN UNNEST(@bvids)
            """,
            [
                bigquery.ScalarQueryParameter("exported_at", "STRING", _iso(_utcnow())),
                bigquery.ArrayQueryParameter("bvids", "STRING", normalized),
            ],
        )

    def count_watchlist_statuses(self) -> dict[str, int]:
        rows = self._query(
            f"""
            SELECT status, COUNT(1) AS total
            FROM `{self.tracker_table_id('video_watchlist')}`
            GROUP BY status
            """
        )
        counts = {"active": 0, "expired": 0}
        for row in rows:
            counts[str(row["status"])] = int(row["total"])
        return counts

    def watchlist_rows(self, *, only_active: bool) -> list[dict[str, Any]]:
        sql = f"""
            SELECT bvid, owner_mid, pubdate, first_discovered_at, last_discovered_at,
                   tracking_deadline, status, discovery_sources_json,
                   last_stat_snapshot_at, last_comment_snapshot_at,
                   stat_fail_count, comment_fail_count, last_error, last_error_at
            FROM `{self.tracker_table_id('video_watchlist')}`
        """
        if only_active:
            sql += " WHERE status = 'active'"
        sql += " ORDER BY first_discovered_at DESC"
        rows = self._query(sql)
        result = []
        for row in rows:
            item = dict(row.items())
            item["discovery_sources"] = _json_loads(item.pop("discovery_sources_json", "[]"), [])
            result.append(item)
        return result

    @staticmethod
    def rows_to_csv(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "bvid\n"
        output = io.StringIO()
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized = {
                key: _json_dumps(value) if isinstance(value, (list, dict)) else value
                for key, value in row.items()
            }
            writer.writerow(serialized)
        return output.getvalue()
