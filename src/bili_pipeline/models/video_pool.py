from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


SourceType = Literal["hot", "weekly_hot", "partition", "author_expand", "search_supplement"]


@dataclass(slots=True)
class CandidateVideo:
    """Lightweight discovery payload before the full metadata crawl."""

    bvid: str
    source_type: SourceType
    source_ref: str
    discovered_at: datetime
    owner_mid: int | None = None
    tid: int | None = None
    pubdate: datetime | None = None
    duration_seconds: int | None = None
    seed_score: float | None = None


@dataclass(slots=True)
class VideoPoolEntry:
    """Merged entry stored in video_pool."""

    bvid: str
    source_type: SourceType
    source_ref: str
    discovered_at: datetime
    last_seen_at: datetime
    owner_mid: int | None = None
    tid: int | None = None
    pubdate: datetime | None = None
    duration_seconds: int | None = None
    seed_score: float | None = None
    source_refs: list[str] = field(default_factory=list)

    def merge(self, candidate: CandidateVideo) -> None:
        self.discovered_at = min(self.discovered_at, candidate.discovered_at)
        self.last_seen_at = max(self.last_seen_at, candidate.discovered_at)
        self.owner_mid = self.owner_mid or candidate.owner_mid
        self.tid = self.tid if self.tid is not None else candidate.tid
        self.pubdate = self.pubdate or candidate.pubdate
        self.duration_seconds = self.duration_seconds or candidate.duration_seconds
        if candidate.seed_score is not None:
            self.seed_score = max(self.seed_score or candidate.seed_score, candidate.seed_score)
        if candidate.source_ref not in self.source_refs:
            self.source_refs.append(candidate.source_ref)


@dataclass(slots=True)
class DiscoverResult:
    entries: list[VideoPoolEntry]
    owner_mids: list[int]
