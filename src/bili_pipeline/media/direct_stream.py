from __future__ import annotations

import hashlib
import uuid
from typing import Any

import aiohttp
from bilibili_api.video import AudioStreamDownloadURL, Video, VideoDownloadURLDataDetecter, VideoStreamDownloadURL

from bili_pipeline.models import MediaAssetRef, MediaDownloadStrategy, MediaResult
from bili_pipeline.storage import OSSMediaStore, SQLiteCrawlerStore
from bili_pipeline.utils import run_async


def _quality_height(name: str | None) -> int:
    if not name:
        return 0
    digits = "".join(ch for ch in name if ch.isdigit())
    return int(digits) if digits else 0


def _pick_streams(
    download_payload: dict[str, Any],
    max_height: int,
) -> tuple[VideoStreamDownloadURL | None, AudioStreamDownloadURL | None]:
    streams = VideoDownloadURLDataDetecter(download_payload).detect_all()
    video_candidates = [stream for stream in streams if isinstance(stream, VideoStreamDownloadURL)]
    audio_candidates = [stream for stream in streams if isinstance(stream, AudioStreamDownloadURL)]

    def _video_key(stream: VideoStreamDownloadURL) -> tuple[int, str]:
        quality_name = _quality_name(stream)
        height = _quality_height(quality_name)
        height = height if height <= max_height else -1
        return (height, quality_name)

    allowed_video_candidates = [stream for stream in video_candidates if _quality_height(_quality_name(stream)) <= max_height]
    video_pool = allowed_video_candidates or video_candidates
    video_stream = max(video_pool, key=_video_key, default=None)
    audio_stream = max(audio_candidates, key=lambda item: _quality_height(_quality_name(item)), default=None)
    return video_stream, audio_stream


def _quality_name(stream: Any) -> str:
    data = getattr(stream, "__dict__", {})
    quality = data.get("video_quality") or data.get("audio_quality")
    return getattr(quality, "name", "unknown")


def _mime_type(asset_type: str) -> str:
    return "video/mp4" if asset_type == "video" else "audio/mp4"


async def _stream_one_asset(
    *,
    store: SQLiteCrawlerStore,
    oss_store: OSSMediaStore | None,
    bvid: str,
    cid: int | None,
    asset_type: str,
    object_key: str,
    format_selected: str,
    url: str,
    upload_session_id: str,
    strategy: MediaDownloadStrategy,
) -> MediaAssetRef:
    timeout = aiohttp.ClientTimeout(total=strategy.request_timeout_seconds * 10)
    sha = hashlib.sha256()
    file_size = 0
    chunk_count = 0
    mime_type = _mime_type(asset_type)
    storage_backend = "oss" if oss_store is not None else "sqlite"
    bucket_name = oss_store.bucket_name if oss_store is not None else None
    storage_endpoint = oss_store.endpoint if oss_store is not None else None
    object_url = oss_store.build_object_url(object_key) if oss_store is not None else None
    asset_id = store.begin_media_asset(
        bvid=bvid,
        cid=cid,
        asset_type=asset_type,
        storage_backend=storage_backend,
        object_key=object_key,
        bucket_name=bucket_name,
        storage_endpoint=storage_endpoint,
        object_url=object_url,
        format_selected=format_selected,
        mime_type=mime_type,
        upload_session_id=upload_session_id,
        raw_payload={"url": url},
    )
    multipart_session = None
    if oss_store is not None:
        multipart_session = oss_store.create_multipart_upload(
            object_key=object_key,
            mime_type=mime_type,
            metadata={"bvid": bvid, "asset_type": asset_type},
        )

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"Referer": "https://www.bilibili.com/"}) as response:
                response.raise_for_status()
                async for chunk in response.content.iter_chunked(strategy.chunk_size_bytes()):
                    if not chunk:
                        continue
                    sha.update(chunk)
                    file_size += len(chunk)
                    if multipart_session is None:
                        store.append_media_chunk(asset_id, chunk_count, chunk)
                    else:
                        multipart_session.upload_part(chunk)
                    chunk_count += 1
    except Exception:
        if multipart_session is not None:
            try:
                multipart_session.abort()
            except Exception:  # noqa: BLE001
                pass
        raise

    etag = ""
    if multipart_session is not None:
        commit_result = multipart_session.complete()
        object_url = commit_result.object_url
        etag = commit_result.etag
    store.finalize_media_asset(
        asset_id,
        file_size=file_size,
        sha256=sha.hexdigest(),
        chunk_count=chunk_count,
        etag=etag,
    )
    return MediaAssetRef(
        asset_type=asset_type,
        storage_backend=storage_backend,
        object_key=object_key,
        format_selected=format_selected,
        mime_type=mime_type,
        file_size=file_size,
        sha256=sha.hexdigest(),
        chunk_count=chunk_count,
        bucket_name=bucket_name,
        storage_endpoint=storage_endpoint,
        object_url=object_url,
        etag=etag or None,
    )


async def _stream_media_async(
    bvid: str,
    strategy: MediaDownloadStrategy,
    store: SQLiteCrawlerStore,
    credential: Any | None = None,
) -> MediaResult:
    video = Video(bvid=bvid) if credential is None else Video(bvid=bvid, credential=credential)
    info = await video.get_info()
    pages = info.get("pages", [])
    cid = pages[0].get("cid") if pages else None
    download_payload = await video.get_download_url(page_index=0)
    video_stream, audio_stream = _pick_streams(download_payload, strategy.max_height)
    oss_store = OSSMediaStore(strategy.oss_config) if strategy.use_oss_media() and strategy.oss_config else None

    if video_stream is None and audio_stream is None:
        raise RuntimeError("No downloadable media stream found for this bvid.")

    upload_session_id = uuid.uuid4().hex
    video_asset = None
    audio_asset = None

    if video_stream is not None:
        video_asset = await _stream_one_asset(
            store=store,
            oss_store=oss_store,
            bvid=bvid,
            cid=cid,
            asset_type="video",
            object_key=strategy.build_object_key(bvid, cid, "video"),
            format_selected=_quality_name(video_stream),
            url=video_stream.url,
            upload_session_id=upload_session_id,
            strategy=strategy,
        )

    if audio_stream is not None:
        audio_asset = await _stream_one_asset(
            store=store,
            oss_store=oss_store,
            bvid=bvid,
            cid=cid,
            asset_type="audio",
            object_key=strategy.build_object_key(bvid, cid, "audio"),
            format_selected=_quality_name(audio_stream),
            url=audio_stream.url,
            upload_session_id=upload_session_id,
            strategy=strategy,
        )

    return MediaResult(
        bvid=bvid,
        cid=cid,
        video_asset=video_asset,
        audio_asset=audio_asset,
        upload_session_id=upload_session_id,
        raw_payload=download_payload,
    )


def stream_media_to_store(
    bvid: str,
    strategy: MediaDownloadStrategy,
    store: SQLiteCrawlerStore | None = None,
    credential: Any | None = None,
) -> MediaResult:
    if strategy.storage_backend == "oss" and not strategy.use_oss_media():
        raise ValueError("阿里云 OSS 存储已启用，但配置不完整。请检查 Endpoint、Bucket 和 AccessKey。")
    active_store = store or SQLiteCrawlerStore(strategy.sqlite_file())
    return run_async(_stream_media_async(bvid=bvid, strategy=strategy, store=active_store, credential=credential))
