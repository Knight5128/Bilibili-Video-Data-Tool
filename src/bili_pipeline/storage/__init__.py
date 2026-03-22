"""Storage backends for crawler outputs."""

from .oss_store import OSSMediaStore
from .sqlite_store import SQLiteCrawlerStore

__all__ = ["OSSMediaStore", "SQLiteCrawlerStore"]
