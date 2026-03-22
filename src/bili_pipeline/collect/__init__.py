"""Structured data collectors."""

from .video_collectors import crawl_latest_comments, crawl_stat_snapshot, crawl_video_meta

__all__ = ["crawl_latest_comments", "crawl_stat_snapshot", "crawl_video_meta"]
