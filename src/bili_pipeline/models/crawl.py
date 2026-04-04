from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable
from typing import Any


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class CrawlTaskMode(str, Enum):
    FULL_BUNDLE = "full_bundle"
    REALTIME_ONLY = "realtime_only"
    ONCE_ONLY = "once_only"
    META_ONLY = "meta_only"
    MEDIA_ONLY = "media_only"

    def includes_meta(self) -> bool:
        return self in {self.FULL_BUNDLE, self.ONCE_ONLY, self.META_ONLY}

    def includes_realtime(self) -> bool:
        return self in {self.FULL_BUNDLE, self.REALTIME_ONLY}

    def includes_media(self) -> bool:
        return self in {self.FULL_BUNDLE, self.ONCE_ONLY, self.MEDIA_ONLY}


@dataclass(slots=True)
class CommentItem:
    rpid: int
    message: str
    like: int
    ctime: datetime | None
    mid: int | None
    uname: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rpid": self.rpid,
            "message": self.message,
            "like": self.like,
            "ctime": _dt(self.ctime),
            "mid": self.mid,
            "uname": self.uname,
        }


@dataclass(slots=True)
class StatSnapshot:
    bvid: str
    snapshot_time: datetime
    stat_view: int
    stat_like: int
    stat_coin: int
    stat_favorite: int
    stat_share: int
    stat_reply: int
    stat_danmu: int
    stat_dislike: int = 0
    stat_his_rank: int = 0
    stat_now_rank: int = 0
    stat_evaluation: str = ""
    source_pool: str = "video_pool"
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bvid": self.bvid,
            "snapshot_time": _dt(self.snapshot_time),
            "stat_view": self.stat_view,
            "stat_like": self.stat_like,
            "stat_coin": self.stat_coin,
            "stat_favorite": self.stat_favorite,
            "stat_share": self.stat_share,
            "stat_reply": self.stat_reply,
            "stat_danmu": self.stat_danmu,
            "stat_dislike": self.stat_dislike,
            "stat_his_rank": self.stat_his_rank,
            "stat_now_rank": self.stat_now_rank,
            "stat_evaluation": self.stat_evaluation,
            "source_pool": self.source_pool,
        }


@dataclass(slots=True)
class MetaResult:
    bvid: str
    aid: int | None
    title: str
    desc: str
    pic: str
    dynamic: str
    tags: list[str]
    tag_details: list[dict[str, Any]]
    videos: int | None
    tid: int | None
    tid_v2: int | None
    tname: str
    tname_v2: str
    copyright: int | None
    owner_mid: int | None
    owner_name: str | None
    owner_face: str | None
    owner_sign: str | None
    owner_gender: str | None
    owner_level: int | None
    owner_verified: bool
    owner_verified_title: str | None
    owner_vip_type: int | None
    owner_follower_count: int | None
    owner_following_count: int | None
    owner_video_count: int | None
    is_activity_participant: bool
    duration: int | None
    state: int | None
    pubdate: datetime | None
    cid: int | None
    resolution_width: int | None
    resolution_height: int | None
    resolution_rotate: int | None
    is_story: bool
    is_interactive_video: bool
    is_downloadable: bool
    is_reprint_allowed: bool
    is_collaboration: bool
    is_360: bool
    is_paid_video: bool
    pages_info: list[dict[str, Any]]
    rights: dict[str, Any]
    subtitle: dict[str, Any]
    uploader_profile: dict[str, Any]
    uploader_relation: dict[str, Any]
    uploader_overview: dict[str, Any]
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bvid": self.bvid,
            "aid": self.aid,
            "title": self.title,
            "desc": self.desc,
            "pic": self.pic,
            "dynamic": self.dynamic,
            "tags": self.tags,
            "tag_details": self.tag_details,
            "videos": self.videos,
            "tid": self.tid,
            "tid_v2": self.tid_v2,
            "tname": self.tname,
            "tname_v2": self.tname_v2,
            "copyright": self.copyright,
            "owner_mid": self.owner_mid,
            "owner_name": self.owner_name,
            "owner_face": self.owner_face,
            "owner_sign": self.owner_sign,
            "owner_gender": self.owner_gender,
            "owner_level": self.owner_level,
            "owner_verified": self.owner_verified,
            "owner_verified_title": self.owner_verified_title,
            "owner_vip_type": self.owner_vip_type,
            "owner_follower_count": self.owner_follower_count,
            "owner_following_count": self.owner_following_count,
            "owner_video_count": self.owner_video_count,
            "is_activity_participant": self.is_activity_participant,
            "duration": self.duration,
            "state": self.state,
            "pubdate": _dt(self.pubdate),
            "cid": self.cid,
            "resolution_width": self.resolution_width,
            "resolution_height": self.resolution_height,
            "resolution_rotate": self.resolution_rotate,
            "is_story": self.is_story,
            "is_interactive_video": self.is_interactive_video,
            "is_downloadable": self.is_downloadable,
            "is_reprint_allowed": self.is_reprint_allowed,
            "is_collaboration": self.is_collaboration,
            "is_360": self.is_360,
            "is_paid_video": self.is_paid_video,
            "pages_info": self.pages_info,
            "rights": self.rights,
            "subtitle": self.subtitle,
            "uploader_profile": self.uploader_profile,
            "uploader_relation": self.uploader_relation,
            "uploader_overview": self.uploader_overview,
        }


@dataclass(slots=True)
class CommentSnapshot:
    bvid: str
    snapshot_time: datetime
    limit: int
    items: list[CommentItem]
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bvid": self.bvid,
            "snapshot_time": _dt(self.snapshot_time),
            "limit": self.limit,
            "comment_top10": [item.to_dict() for item in self.items],
        }


@dataclass(slots=True)
class GCPStorageConfig:
    project_id: str = ""
    bigquery_dataset: str = "bili_video_data_crawler"
    gcs_bucket_name: str = ""
    gcp_region: str = ""
    credentials_path: str = ""
    object_prefix: str = "bilibili-media"
    public_base_url: str = ""

    def is_enabled(self) -> bool:
        return bool(self.bigquery_dataset.strip() and self.gcs_bucket_name.strip())

    def normalized_prefix(self) -> str:
        return self.object_prefix.strip().strip("/")

    def endpoint_with_scheme(self) -> str:
        return "https://storage.googleapis.com"

    @property
    def bucket_name(self) -> str:
        return self.gcs_bucket_name.strip()

    def build_gcs_uri(self, object_key: str) -> str:
        key = object_key.lstrip("/")
        return f"gs://{self.bucket_name}/{key}" if key else f"gs://{self.bucket_name}"

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id.strip(),
            "bigquery_dataset": self.bigquery_dataset.strip(),
            "gcs_bucket_name": self.bucket_name,
            "gcp_region": self.gcp_region.strip(),
            "object_prefix": self.normalized_prefix(),
            "public_base_url": self.public_base_url.strip(),
            "has_credentials_path": bool(self.credentials_path.strip()),
        }


@dataclass(slots=True)
class MediaDownloadStrategy:
    max_height: int = 1080
    chunk_size_mb: int = 4
    sqlite_path: str = "outputs/bili_video_data_crawler.db"
    request_timeout_seconds: int = 120
    storage_backend: str = "gcs"
    gcp_config: GCPStorageConfig | None = None
    truncate_seconds: int = 0
    stop_checker: Callable[[], bool] | None = field(default=None, repr=False, compare=False)

    def chunk_size_bytes(self) -> int:
        return max(1, self.chunk_size_mb) * 1024 * 1024

    def sqlite_file(self) -> Path:
        return Path(self.sqlite_path)

    @property
    def oss_config(self) -> GCPStorageConfig | None:
        return self.gcp_config

    def use_oss_media(self) -> bool:
        return self.use_gcs_media()

    def use_gcs_media(self) -> bool:
        return self.storage_backend == "gcs" and self.gcp_config is not None and self.gcp_config.is_enabled()

    def build_object_key(self, bvid: str, cid: int | None, asset_type: str) -> str:
        prefix = self.gcp_config.normalized_prefix() if self.gcp_config else ""
        extension = "mp4" if int(self.truncate_seconds or 0) > 0 else "m4s"
        parts = [part for part in [prefix, bvid, str(cid or "na"), f"{asset_type}.{extension}"] if part]
        return "/".join(parts)

    def with_sqlite_path(self, sqlite_path: str | Path) -> MediaDownloadStrategy:
        return MediaDownloadStrategy(
            max_height=self.max_height,
            chunk_size_mb=self.chunk_size_mb,
            sqlite_path=str(sqlite_path),
            request_timeout_seconds=self.request_timeout_seconds,
            storage_backend=self.storage_backend,
            gcp_config=self.gcp_config,
            truncate_seconds=self.truncate_seconds,
            stop_checker=self.stop_checker,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_height": self.max_height,
            "chunk_size_mb": self.chunk_size_mb,
            "sqlite_path": self.sqlite_path,
            "request_timeout_seconds": self.request_timeout_seconds,
            "storage_backend": self.storage_backend,
            "gcp_config": self.gcp_config.to_safe_dict() if self.gcp_config else None,
            "truncate_seconds": int(self.truncate_seconds or 0),
        }


OSSStorageConfig = GCPStorageConfig


@dataclass(slots=True)
class MediaAssetRef:
    asset_type: str
    storage_backend: str
    object_key: str
    format_selected: str
    mime_type: str
    file_size: int
    sha256: str
    chunk_count: int
    bucket_name: str | None = None
    storage_endpoint: str | None = None
    object_url: str | None = None
    etag: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_type": self.asset_type,
            "storage_backend": self.storage_backend,
            "object_key": self.object_key,
            "format_selected": self.format_selected,
            "mime_type": self.mime_type,
            "file_size": self.file_size,
            "sha256": self.sha256,
            "chunk_count": self.chunk_count,
            "bucket_name": self.bucket_name,
            "storage_endpoint": self.storage_endpoint,
            "object_url": self.object_url,
            "etag": self.etag,
        }


@dataclass(slots=True)
class MediaResult:
    bvid: str
    cid: int | None
    video_asset: MediaAssetRef | None
    audio_asset: MediaAssetRef | None
    upload_session_id: str
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bvid": self.bvid,
            "cid": self.cid,
            "video_object_key": self.video_asset.object_key if self.video_asset else None,
            "audio_object_key": self.audio_asset.object_key if self.audio_asset else None,
            "video_format_selected": self.video_asset.format_selected if self.video_asset else None,
            "audio_format_selected": self.audio_asset.format_selected if self.audio_asset else None,
            "upload_session_id": self.upload_session_id,
            "video_asset": self.video_asset.to_dict() if self.video_asset else None,
            "audio_asset": self.audio_asset.to_dict() if self.audio_asset else None,
        }


@dataclass(slots=True)
class FullCrawlSummary:
    bvid: str
    meta_ok: bool
    stat_ok: bool
    comment_ok: bool
    media_ok: bool
    snapshot_time: datetime | None
    task_mode: str = CrawlTaskMode.FULL_BUNDLE.value
    errors: list[str] = field(default_factory=list)
    meta_result: MetaResult | None = None
    stat_snapshot: StatSnapshot | None = None
    comment_snapshot: CommentSnapshot | None = None
    media_result: MediaResult | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "bvid": self.bvid,
            "meta_ok": self.meta_ok,
            "stat_ok": self.stat_ok,
            "comment_ok": self.comment_ok,
            "media_ok": self.media_ok,
            "snapshot_time": _dt(self.snapshot_time),
            "task_mode": self.task_mode,
            "errors": self.errors,
            "meta_result": self.meta_result.to_dict() if self.meta_result else None,
            "stat_snapshot": self.stat_snapshot.to_dict() if self.stat_snapshot else None,
            "comment_snapshot": self.comment_snapshot.to_dict() if self.comment_snapshot else None,
            "media_result": self.media_result.to_dict() if self.media_result else None,
        }


@dataclass(slots=True)
class BatchCrawlReport:
    run_id: str
    total_bvids: int
    processed_count: int
    success_count: int
    failed_count: int
    remaining_count: int
    started_at: datetime
    finished_at: datetime
    task_mode: str = CrawlTaskMode.FULL_BUNDLE.value
    summaries: list[FullCrawlSummary] = field(default_factory=list)
    part_number: int = 1
    effective_parallelism: int = 1
    consecutive_failure_limit: int = 0
    stopped_due_to_consecutive_failures: bool = False
    completed_all: bool = False
    stop_reason: str = ""
    session_dir: str = ""
    logs_dir: str = ""
    remaining_csv_path: str | None = None
    task_log_path: str | None = None
    session_state_path: str | None = None
    session_summary_log_path: str | None = None
    successful_bvids: list[str] = field(default_factory=list)
    failed_bvids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "total_bvids": self.total_bvids,
            "processed_count": self.processed_count,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "remaining_count": self.remaining_count,
            "started_at": _dt(self.started_at),
            "finished_at": _dt(self.finished_at),
            "task_mode": self.task_mode,
            "part_number": self.part_number,
            "effective_parallelism": self.effective_parallelism,
            "consecutive_failure_limit": self.consecutive_failure_limit,
            "stopped_due_to_consecutive_failures": self.stopped_due_to_consecutive_failures,
            "completed_all": self.completed_all,
            "stop_reason": self.stop_reason,
            "session_dir": self.session_dir,
            "logs_dir": self.logs_dir,
            "remaining_csv_path": self.remaining_csv_path,
            "task_log_path": self.task_log_path,
            "session_state_path": self.session_state_path,
            "session_summary_log_path": self.session_summary_log_path,
            "successful_bvids": self.successful_bvids,
            "failed_bvids": self.failed_bvids,
            "per_bvid_summaries": [summary.to_dict() for summary in self.summaries],
        }
