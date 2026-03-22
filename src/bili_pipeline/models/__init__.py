"""Core discovery models."""

from .crawl import (
    BatchCrawlReport,
    CommentItem,
    CommentSnapshot,
    FullCrawlSummary,
    MediaAssetRef,
    MediaDownloadStrategy,
    MediaResult,
    MetaResult,
    OSSStorageConfig,
    StatSnapshot,
)
from .video_pool import CandidateVideo, DiscoverResult, VideoPoolEntry

__all__ = [
    "BatchCrawlReport",
    "CandidateVideo",
    "CommentItem",
    "CommentSnapshot",
    "DiscoverResult",
    "FullCrawlSummary",
    "MediaAssetRef",
    "MediaDownloadStrategy",
    "MediaResult",
    "MetaResult",
    "OSSStorageConfig",
    "StatSnapshot",
    "VideoPoolEntry",
]
