from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from bili_pipeline.models import (
    CommentSnapshot,
    FullCrawlSummary,
    MediaAssetRef,
    MediaResult,
    MetaResult,
    StatSnapshot,
)


class SQLiteCrawlerStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=60, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _ensure_tables(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    bvid TEXT PRIMARY KEY,
                    aid INTEGER,
                    title TEXT,
                    desc_text TEXT,
                    pic TEXT,
                    dynamic_text TEXT,
                    tags_json TEXT,
                    tag_details_json TEXT,
                    videos_count INTEGER,
                    tid INTEGER,
                    tid_v2 INTEGER,
                    tname TEXT,
                    tname_v2 TEXT,
                    copyright INTEGER,
                    owner_mid INTEGER,
                    owner_name TEXT,
                    owner_face TEXT,
                    owner_sign TEXT,
                    owner_gender TEXT,
                    owner_level INTEGER,
                    owner_verified INTEGER,
                    owner_verified_title TEXT,
                    owner_vip_type INTEGER,
                    owner_follower_count INTEGER,
                    owner_following_count INTEGER,
                    owner_video_count INTEGER,
                    is_activity_participant INTEGER,
                    duration_seconds INTEGER,
                    state INTEGER,
                    pubdate TEXT,
                    cid INTEGER,
                    resolution_width INTEGER,
                    resolution_height INTEGER,
                    resolution_rotate INTEGER,
                    is_story INTEGER,
                    is_interactive_video INTEGER,
                    is_downloadable INTEGER,
                    is_reprint_allowed INTEGER,
                    is_collaboration INTEGER,
                    is_360 INTEGER,
                    is_paid_video INTEGER,
                    pages_info_json TEXT,
                    rights_json TEXT,
                    subtitle_json TEXT,
                    uploader_profile_json TEXT,
                    uploader_relation_json TEXT,
                    uploader_overview_json TEXT,
                    raw_payload_json TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS video_stat_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bvid TEXT NOT NULL,
                    snapshot_time TEXT NOT NULL,
                    stat_view INTEGER,
                    stat_like INTEGER,
                    stat_coin INTEGER,
                    stat_favorite INTEGER,
                    stat_share INTEGER,
                    stat_reply INTEGER,
                    stat_danmu INTEGER,
                    stat_dislike INTEGER,
                    stat_his_rank INTEGER,
                    stat_now_rank INTEGER,
                    stat_evaluation TEXT,
                    source_pool TEXT,
                    raw_payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS topn_comment_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bvid TEXT NOT NULL,
                    snapshot_time TEXT NOT NULL,
                    limit_n INTEGER NOT NULL,
                    items_json TEXT NOT NULL,
                    raw_payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bvid TEXT NOT NULL,
                    cid INTEGER,
                    asset_type TEXT NOT NULL,
                    storage_backend TEXT NOT NULL DEFAULT 'sqlite',
                    object_key TEXT NOT NULL,
                    bucket_name TEXT,
                    storage_endpoint TEXT,
                    object_url TEXT,
                    format_selected TEXT,
                    mime_type TEXT,
                    file_size INTEGER DEFAULT 0,
                    sha256 TEXT DEFAULT '',
                    etag TEXT DEFAULT '',
                    chunk_count INTEGER DEFAULT 0,
                    upload_session_id TEXT,
                    raw_payload_json TEXT,
                    uploaded_at TEXT,
                    UNIQUE (bvid, cid, asset_type)
                );

                CREATE TABLE IF NOT EXISTS asset_chunks (
                    asset_id INTEGER NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    chunk_data BLOB NOT NULL,
                    PRIMARY KEY (asset_id, chunk_index),
                    FOREIGN KEY(asset_id) REFERENCES assets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS crawl_runs (
                    run_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    total_bvids INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    notes_json TEXT
                );

                CREATE TABLE IF NOT EXISTS crawl_run_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bvid TEXT NOT NULL,
                    snapshot_time TEXT,
                    meta_ok INTEGER NOT NULL,
                    stat_ok INTEGER NOT NULL,
                    comment_ok INTEGER NOT NULL,
                    media_ok INTEGER NOT NULL,
                    errors_json TEXT,
                    summary_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES crawl_runs(run_id) ON DELETE CASCADE
                );
                """
            )
            self._ensure_video_columns(conn)
            self._ensure_stat_columns(conn)
            self._ensure_asset_columns(conn)

    def _ensure_video_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(videos)").fetchall()}
        required_columns = {
            "aid": "ALTER TABLE videos ADD COLUMN aid INTEGER",
            "pic": "ALTER TABLE videos ADD COLUMN pic TEXT",
            "dynamic_text": "ALTER TABLE videos ADD COLUMN dynamic_text TEXT",
            "tag_details_json": "ALTER TABLE videos ADD COLUMN tag_details_json TEXT",
            "videos_count": "ALTER TABLE videos ADD COLUMN videos_count INTEGER",
            "tid": "ALTER TABLE videos ADD COLUMN tid INTEGER",
            "tid_v2": "ALTER TABLE videos ADD COLUMN tid_v2 INTEGER",
            "tname": "ALTER TABLE videos ADD COLUMN tname TEXT",
            "tname_v2": "ALTER TABLE videos ADD COLUMN tname_v2 TEXT",
            "copyright": "ALTER TABLE videos ADD COLUMN copyright INTEGER",
            "owner_face": "ALTER TABLE videos ADD COLUMN owner_face TEXT",
            "owner_sign": "ALTER TABLE videos ADD COLUMN owner_sign TEXT",
            "owner_gender": "ALTER TABLE videos ADD COLUMN owner_gender TEXT",
            "owner_level": "ALTER TABLE videos ADD COLUMN owner_level INTEGER",
            "owner_verified": "ALTER TABLE videos ADD COLUMN owner_verified INTEGER",
            "owner_verified_title": "ALTER TABLE videos ADD COLUMN owner_verified_title TEXT",
            "owner_vip_type": "ALTER TABLE videos ADD COLUMN owner_vip_type INTEGER",
            "owner_following_count": "ALTER TABLE videos ADD COLUMN owner_following_count INTEGER",
            "duration_seconds": "ALTER TABLE videos ADD COLUMN duration_seconds INTEGER",
            "state": "ALTER TABLE videos ADD COLUMN state INTEGER",
            "resolution_width": "ALTER TABLE videos ADD COLUMN resolution_width INTEGER",
            "resolution_height": "ALTER TABLE videos ADD COLUMN resolution_height INTEGER",
            "resolution_rotate": "ALTER TABLE videos ADD COLUMN resolution_rotate INTEGER",
            "is_story": "ALTER TABLE videos ADD COLUMN is_story INTEGER",
            "is_interactive_video": "ALTER TABLE videos ADD COLUMN is_interactive_video INTEGER",
            "is_downloadable": "ALTER TABLE videos ADD COLUMN is_downloadable INTEGER",
            "is_reprint_allowed": "ALTER TABLE videos ADD COLUMN is_reprint_allowed INTEGER",
            "is_collaboration": "ALTER TABLE videos ADD COLUMN is_collaboration INTEGER",
            "is_360": "ALTER TABLE videos ADD COLUMN is_360 INTEGER",
            "is_paid_video": "ALTER TABLE videos ADD COLUMN is_paid_video INTEGER",
            "rights_json": "ALTER TABLE videos ADD COLUMN rights_json TEXT",
            "subtitle_json": "ALTER TABLE videos ADD COLUMN subtitle_json TEXT",
            "uploader_profile_json": "ALTER TABLE videos ADD COLUMN uploader_profile_json TEXT",
            "uploader_relation_json": "ALTER TABLE videos ADD COLUMN uploader_relation_json TEXT",
            "uploader_overview_json": "ALTER TABLE videos ADD COLUMN uploader_overview_json TEXT",
        }
        for column_name, statement in required_columns.items():
            if column_name not in existing_columns:
                conn.execute(statement)

    def _ensure_stat_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(video_stat_snapshots)").fetchall()}
        required_columns = {
            "stat_dislike": "ALTER TABLE video_stat_snapshots ADD COLUMN stat_dislike INTEGER",
            "stat_his_rank": "ALTER TABLE video_stat_snapshots ADD COLUMN stat_his_rank INTEGER",
            "stat_now_rank": "ALTER TABLE video_stat_snapshots ADD COLUMN stat_now_rank INTEGER",
            "stat_evaluation": "ALTER TABLE video_stat_snapshots ADD COLUMN stat_evaluation TEXT",
        }
        for column_name, statement in required_columns.items():
            if column_name not in existing_columns:
                conn.execute(statement)

    def _ensure_asset_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(assets)").fetchall()
        }
        required_columns = {
            "storage_backend": "ALTER TABLE assets ADD COLUMN storage_backend TEXT NOT NULL DEFAULT 'sqlite'",
            "bucket_name": "ALTER TABLE assets ADD COLUMN bucket_name TEXT",
            "storage_endpoint": "ALTER TABLE assets ADD COLUMN storage_endpoint TEXT",
            "object_url": "ALTER TABLE assets ADD COLUMN object_url TEXT",
            "etag": "ALTER TABLE assets ADD COLUMN etag TEXT DEFAULT ''",
        }
        for column_name, statement in required_columns.items():
            if column_name not in existing_columns:
                conn.execute(statement)

    def save_video_meta(self, result: MetaResult) -> None:
        with self._lock, self._connect() as conn:
            values_placeholders = ", ".join(["?"] * 49)
            conn.execute(
                f"""
                INSERT INTO videos (
                    bvid, aid, title, desc_text, pic, dynamic_text, tags_json, tag_details_json,
                    videos_count, tid, tid_v2, tname, tname_v2, copyright, owner_mid, owner_name,
                    owner_face, owner_sign, owner_gender, owner_level, owner_verified,
                    owner_verified_title, owner_vip_type, owner_follower_count,
                    owner_following_count, owner_video_count, is_activity_participant,
                    duration_seconds, state, pubdate, cid, resolution_width, resolution_height,
                    resolution_rotate, is_story, is_interactive_video, is_downloadable,
                    is_reprint_allowed, is_collaboration, is_360, is_paid_video,
                    pages_info_json, rights_json, subtitle_json, uploader_profile_json,
                    uploader_relation_json, uploader_overview_json, raw_payload_json, updated_at
                ) VALUES ({values_placeholders})
                ON CONFLICT(bvid) DO UPDATE SET
                    aid=excluded.aid,
                    title=excluded.title,
                    desc_text=excluded.desc_text,
                    pic=excluded.pic,
                    dynamic_text=excluded.dynamic_text,
                    tags_json=excluded.tags_json,
                    tag_details_json=excluded.tag_details_json,
                    videos_count=excluded.videos_count,
                    tid=excluded.tid,
                    tid_v2=excluded.tid_v2,
                    tname=excluded.tname,
                    tname_v2=excluded.tname_v2,
                    copyright=excluded.copyright,
                    owner_mid=excluded.owner_mid,
                    owner_name=excluded.owner_name,
                    owner_face=excluded.owner_face,
                    owner_sign=excluded.owner_sign,
                    owner_gender=excluded.owner_gender,
                    owner_level=excluded.owner_level,
                    owner_verified=excluded.owner_verified,
                    owner_verified_title=excluded.owner_verified_title,
                    owner_vip_type=excluded.owner_vip_type,
                    owner_follower_count=excluded.owner_follower_count,
                    owner_following_count=excluded.owner_following_count,
                    owner_video_count=excluded.owner_video_count,
                    is_activity_participant=excluded.is_activity_participant,
                    duration_seconds=excluded.duration_seconds,
                    state=excluded.state,
                    pubdate=excluded.pubdate,
                    cid=excluded.cid,
                    resolution_width=excluded.resolution_width,
                    resolution_height=excluded.resolution_height,
                    resolution_rotate=excluded.resolution_rotate,
                    is_story=excluded.is_story,
                    is_interactive_video=excluded.is_interactive_video,
                    is_downloadable=excluded.is_downloadable,
                    is_reprint_allowed=excluded.is_reprint_allowed,
                    is_collaboration=excluded.is_collaboration,
                    is_360=excluded.is_360,
                    is_paid_video=excluded.is_paid_video,
                    pages_info_json=excluded.pages_info_json,
                    rights_json=excluded.rights_json,
                    subtitle_json=excluded.subtitle_json,
                    uploader_profile_json=excluded.uploader_profile_json,
                    uploader_relation_json=excluded.uploader_relation_json,
                    uploader_overview_json=excluded.uploader_overview_json,
                    raw_payload_json=excluded.raw_payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    result.bvid,
                    result.aid,
                    result.title,
                    result.desc,
                    result.pic,
                    result.dynamic,
                    json.dumps(result.tags, ensure_ascii=False),
                    json.dumps(result.tag_details, ensure_ascii=False),
                    result.videos,
                    result.tid,
                    result.tid_v2,
                    result.tname,
                    result.tname_v2,
                    result.copyright,
                    result.owner_mid,
                    result.owner_name,
                    result.owner_face,
                    result.owner_sign,
                    result.owner_gender,
                    result.owner_level,
                    int(result.owner_verified),
                    result.owner_verified_title,
                    result.owner_vip_type,
                    result.owner_follower_count,
                    result.owner_following_count,
                    result.owner_video_count,
                    int(result.is_activity_participant),
                    result.duration,
                    result.state,
                    result.pubdate.isoformat() if result.pubdate else None,
                    result.cid,
                    result.resolution_width,
                    result.resolution_height,
                    result.resolution_rotate,
                    int(result.is_story),
                    int(result.is_interactive_video),
                    int(result.is_downloadable),
                    int(result.is_reprint_allowed),
                    int(result.is_collaboration),
                    int(result.is_360),
                    int(result.is_paid_video),
                    json.dumps(result.pages_info, ensure_ascii=False),
                    json.dumps(result.rights, ensure_ascii=False),
                    json.dumps(result.subtitle, ensure_ascii=False),
                    json.dumps(result.uploader_profile, ensure_ascii=False),
                    json.dumps(result.uploader_relation, ensure_ascii=False),
                    json.dumps(result.uploader_overview, ensure_ascii=False),
                    json.dumps(result.raw_payload, ensure_ascii=False),
                    datetime.now().isoformat(),
                ),
            )

    def save_stat_snapshot(self, snapshot: StatSnapshot) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO video_stat_snapshots (
                    bvid, snapshot_time, stat_view, stat_like, stat_coin,
                    stat_favorite, stat_share, stat_reply, stat_danmu,
                    stat_dislike, stat_his_rank, stat_now_rank, stat_evaluation,
                    source_pool, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.bvid,
                    snapshot.snapshot_time.isoformat(),
                    snapshot.stat_view,
                    snapshot.stat_like,
                    snapshot.stat_coin,
                    snapshot.stat_favorite,
                    snapshot.stat_share,
                    snapshot.stat_reply,
                    snapshot.stat_danmu,
                    snapshot.stat_dislike,
                    snapshot.stat_his_rank,
                    snapshot.stat_now_rank,
                    snapshot.stat_evaluation,
                    snapshot.source_pool,
                    json.dumps(snapshot.raw_payload, ensure_ascii=False),
                ),
            )

    @staticmethod
    def _decode_json_fields(row: dict[str, Any], field_names: tuple[str, ...]) -> dict[str, Any]:
        decoded = dict(row)
        for field_name in field_names:
            value = decoded.get(field_name)
            if not value:
                decoded[field_name] = [] if field_name.endswith("tags_json") else {} if field_name.endswith("_json") else value
                continue
            try:
                decoded[field_name] = json.loads(value)
            except (TypeError, ValueError):
                continue
        return decoded

    def save_comment_snapshot(self, snapshot: CommentSnapshot) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO topn_comment_snapshots (
                    bvid, snapshot_time, limit_n, items_json, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot.bvid,
                    snapshot.snapshot_time.isoformat(),
                    snapshot.limit,
                    json.dumps([item.to_dict() for item in snapshot.items], ensure_ascii=False),
                    json.dumps(snapshot.raw_payload, ensure_ascii=False),
                ),
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
    ) -> int:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM asset_chunks WHERE asset_id IN (SELECT id FROM assets WHERE bvid=? AND cid IS ? AND asset_type=?)", (bvid, cid, asset_type))
            conn.execute("DELETE FROM assets WHERE bvid=? AND cid IS ? AND asset_type=?", (bvid, cid, asset_type))
            cursor = conn.execute(
                """
                INSERT INTO assets (
                    bvid, cid, asset_type, storage_backend, object_key,
                    bucket_name, storage_endpoint, object_url, format_selected,
                    mime_type, upload_session_id, raw_payload_json, uploaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bvid,
                    cid,
                    asset_type,
                    storage_backend,
                    object_key,
                    bucket_name,
                    storage_endpoint,
                    object_url,
                    format_selected,
                    mime_type,
                    upload_session_id,
                    json.dumps(raw_payload or {}, ensure_ascii=False),
                    datetime.now().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def append_media_chunk(self, asset_id: int, chunk_index: int, chunk: bytes) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO asset_chunks (asset_id, chunk_index, chunk_size, chunk_data)
                VALUES (?, ?, ?, ?)
                """,
                (asset_id, chunk_index, len(chunk), sqlite3.Binary(chunk)),
            )

    def finalize_media_asset(
        self,
        asset_id: int,
        *,
        file_size: int,
        sha256: str,
        chunk_count: int,
        etag: str = "",
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE assets
                SET file_size=?, sha256=?, etag=?, chunk_count=?, uploaded_at=?
                WHERE id=?
                """,
                (file_size, sha256, etag, chunk_count, datetime.now().isoformat(), asset_id),
            )

    def read_asset_bytes(self, bvid: str, cid: int | None, asset_type: str) -> bytes:
        """Reconstruct a single asset file from stored chunks."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM assets
                WHERE bvid=? AND cid IS ? AND asset_type=?
                """,
                (bvid, cid, asset_type),
            ).fetchone()
            if row is None:
                return b""
            asset_id = int(row["id"])
            chunks = conn.execute(
                """
                SELECT chunk_data
                FROM asset_chunks
                WHERE asset_id=?
                ORDER BY chunk_index
                """,
                (asset_id,),
            ).fetchall()
            return b"".join(chunk["chunk_data"] for chunk in chunks)

    def save_run_start(self, run_id: str, mode: str, notes: dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO crawl_runs (
                    run_id, mode, started_at, finished_at, total_bvids,
                    success_count, failed_count, notes_json
                ) VALUES (?, ?, ?, NULL, 0, 0, 0, ?)
                """,
                (run_id, mode, datetime.now().isoformat(), json.dumps(notes, ensure_ascii=False)),
            )

    def save_run_item(self, run_id: str, summary: FullCrawlSummary) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO crawl_run_items (
                    run_id, bvid, snapshot_time, meta_ok, stat_ok, comment_ok,
                    media_ok, errors_json, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    summary.bvid,
                    summary.snapshot_time.isoformat() if summary.snapshot_time else None,
                    int(summary.meta_ok),
                    int(summary.stat_ok),
                    int(summary.comment_ok),
                    int(summary.media_ok),
                    json.dumps(summary.errors, ensure_ascii=False),
                    json.dumps(summary.to_dict(), ensure_ascii=False),
                ),
            )

    def finalize_run(self, run_id: str, *, total_bvids: int, success_count: int, failed_count: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE crawl_runs
                SET finished_at=?, total_bvids=?, success_count=?, failed_count=?
                WHERE run_id=?
                """,
                (datetime.now().isoformat(), total_bvids, success_count, failed_count, run_id),
            )

    def fetch_all_asset_rows(self, bvid: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT bvid, cid, asset_type, storage_backend, object_key, bucket_name,
                       storage_endpoint, object_url, format_selected, mime_type,
                       file_size, sha256, etag, chunk_count, upload_session_id, uploaded_at
                FROM assets
                WHERE bvid=?
                ORDER BY asset_type
                """,
                (bvid,),
            ).fetchall()
            return [dict(row) for row in rows]

    def fetch_video_row(self, bvid: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM videos WHERE bvid=?", (bvid,)).fetchone()
            if row is None:
                return None
            return self._decode_json_fields(
                dict(row),
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
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM video_stat_snapshots
                WHERE bvid=?
                ORDER BY snapshot_time DESC, id DESC
                LIMIT 1
                """,
                (bvid,),
            ).fetchone()
            if row is None:
                return None
            return self._decode_json_fields(dict(row), ("raw_payload_json",))

    def fetch_latest_comment_snapshot_row(self, bvid: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM topn_comment_snapshots
                WHERE bvid=?
                ORDER BY snapshot_time DESC, id DESC
                LIMIT 1
                """,
                (bvid,),
            ).fetchone()
            if row is None:
                return None
            return self._decode_json_fields(dict(row), ("items_json", "raw_payload_json"))

    def build_media_result(self, bvid: str, cid: int | None, upload_session_id: str) -> MediaResult:
        rows = self.fetch_all_asset_rows(bvid)
        video_asset: MediaAssetRef | None = None
        audio_asset: MediaAssetRef | None = None
        for row in rows:
            asset = MediaAssetRef(
                asset_type=row["asset_type"],
                storage_backend=row["storage_backend"] or "sqlite",
                object_key=row["object_key"],
                format_selected=row["format_selected"] or "",
                mime_type=row["mime_type"] or "application/octet-stream",
                file_size=row["file_size"] or 0,
                sha256=row["sha256"] or "",
                chunk_count=row["chunk_count"] or 0,
                bucket_name=row["bucket_name"],
                storage_endpoint=row["storage_endpoint"],
                object_url=row["object_url"],
                etag=row["etag"] or None,
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
