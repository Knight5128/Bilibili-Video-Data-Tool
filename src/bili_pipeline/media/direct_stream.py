from __future__ import annotations

import hashlib
import subprocess
import uuid
from typing import Any

import aiohttp
from bilibili_api.video import AudioStreamDownloadURL, Video, VideoDownloadURLDataDetecter, VideoStreamDownloadURL

from bili_pipeline.models import MediaAssetRef, MediaDownloadStrategy, MediaResult
from bili_pipeline.storage import BigQueryCrawlerStore, GcsMediaStore
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


class StopRequestedError(RuntimeError):
    """Raised when a cooperative stop request interrupts media transfer."""


def _stop_requested(strategy: MediaDownloadStrategy) -> bool:
    return bool(strategy.stop_checker is not None and strategy.stop_checker())


def _iter_ffmpeg_truncated_chunks(
    *,
    url: str,
    strategy: MediaDownloadStrategy,
) -> tuple[bytes, bytes]:
    from imageio_ffmpeg import get_ffmpeg_exe

    ffmpeg_path = get_ffmpeg_exe()
    command = [
        ffmpeg_path,
        "-v",
        "error",
        "-headers",
        "Referer: https://www.bilibili.com/\r\nUser-Agent: Mozilla/5.0\r\n",
        "-i",
        url,
        "-t",
        str(int(strategy.truncate_seconds)),
        "-c",
        "copy",
        "-movflags",
        "frag_keyframe+empty_moov",
        "-f",
        "mp4",
        "pipe:1",
    ]
    process = subprocess.Popen(  # noqa: S603
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        while True:
            if _stop_requested(strategy):
                process.kill()
                raise StopRequestedError("用户请求停止媒体下载任务。")
            chunk = process.stdout.read(strategy.chunk_size_bytes()) if process.stdout is not None else b""
            if not chunk:
                break
            yield chunk, b""
        stderr_output = process.stderr.read() if process.stderr is not None else b""
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(stderr_output.decode("utf-8", errors="ignore").strip() or f"ffmpeg exited with code {return_code}")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


async def _stream_one_asset(
    *,
    store: BigQueryCrawlerStore,
    gcs_store: GcsMediaStore,
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
    storage_backend = "gcs"
    bucket_name = gcs_store.bucket_name
    storage_endpoint = gcs_store.endpoint
    object_url = gcs_store.build_object_url(object_key)
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
    multipart_session = gcs_store.create_multipart_upload(
        object_key=object_key,
        mime_type=mime_type,
        metadata={"bvid": bvid, "asset_type": asset_type},
        chunk_size=strategy.chunk_size_bytes(),
    )

    try:
        if int(strategy.truncate_seconds or 0) > 0:
            for chunk, _ in _iter_ffmpeg_truncated_chunks(url=url, strategy=strategy):
                if not chunk:
                    continue
                sha.update(chunk)
                file_size += len(chunk)
                multipart_session.upload_part(chunk)
                chunk_count += 1
        else:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers={"Referer": "https://www.bilibili.com/"}) as response:
                    response.raise_for_status()
                    async for chunk in response.content.iter_chunked(strategy.chunk_size_bytes()):
                        if _stop_requested(strategy):
                            raise StopRequestedError("用户请求停止媒体下载任务。")
                        if not chunk:
                            continue
                        sha.update(chunk)
                        file_size += len(chunk)
                        multipart_session.upload_part(chunk)
                        chunk_count += 1
    except Exception:
        try:
            multipart_session.abort()
        except Exception:  # noqa: BLE001
            pass
        raise

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
    store: BigQueryCrawlerStore,
    credential: Any | None = None,
) -> MediaResult:
    video = Video(bvid=bvid) if credential is None else Video(bvid=bvid, credential=credential)
    info = await video.get_info()
    pages = info.get("pages", [])
    cid = pages[0].get("cid") if pages else None
    download_payload = await video.get_download_url(page_index=0)
    video_stream, audio_stream = _pick_streams(download_payload, strategy.max_height)
    if not strategy.use_gcs_media() or strategy.gcp_config is None:
        raise ValueError("Google Cloud Storage 存储已启用，但 GCP 配置不完整。")
    gcs_store = GcsMediaStore(strategy.gcp_config)

    if video_stream is None and audio_stream is None:
        raise RuntimeError("No downloadable media stream found for this bvid.")

    upload_session_id = uuid.uuid4().hex
    video_asset = None
    audio_asset = None

    if video_stream is not None:
        video_asset = await _stream_one_asset(
            store=store,
            gcs_store=gcs_store,
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
            gcs_store=gcs_store,
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
    store: BigQueryCrawlerStore | None = None,
    credential: Any | None = None,
) -> MediaResult:
    if strategy.storage_backend == "gcs" and not strategy.use_gcs_media():
        raise ValueError("Google Cloud Storage 存储已启用，但 GCP 配置不完整。请检查 Project、Dataset 和 Bucket。")
    if strategy.gcp_config is None:
        raise ValueError("缺少 GCP 配置，无法写入 GCS / BigQuery。")
    active_store = store or BigQueryCrawlerStore(strategy.gcp_config)
    return run_async(_stream_media_async(bvid=bvid, strategy=strategy, store=active_store, credential=credential))
