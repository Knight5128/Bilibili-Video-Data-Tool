from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover.interfaces import AuthorVideoSource, SeedSource
from bili_pipeline.models import CandidateVideo, DiscoverResult, VideoPoolEntry

SOURCE_PRIORITY = {
    "weekly_hot": 5,
    "hot": 4,
    "partition": 3,
    "author_expand": 2,
    "search_supplement": 1,
}


@dataclass(slots=True)
class VideoPoolBuilder:
    """Builds video_pool from seeds and author backfill."""

    config: DiscoverConfig
    hot_sources: list[SeedSource]
    partition_sources: list[SeedSource]
    author_source: AuthorVideoSource

    def build(self, now: datetime | None = None) -> DiscoverResult:
        candidates = self._collect_seed_candidates()
        return self.build_from_seed_candidates(candidates, now=now)

    def build_from_seed_candidates(
        self,
        candidates: list[CandidateVideo],
        now: datetime | None = None,
    ) -> DiscoverResult:
        current_time = now or datetime.now()
        since = current_time - timedelta(days=self.config.lookback_days)
        merged = self._merge_candidates(
            [candidate for candidate in candidates if self._allow_candidate(candidate, enforce_tid=True)]
        )

        owner_mids = sorted(
            {
                entry.owner_mid
                for entry in merged.values()
                if entry.owner_mid is not None and entry.source_type in {"hot", "weekly_hot", "partition"}
            }
        )
        if not self.config.enable_author_backfill:
            entries = sorted(merged.values(), key=lambda item: item.discovered_at)
            return DiscoverResult(entries=entries, owner_mids=owner_mids)

        backfill_candidates: list[CandidateVideo] = []
        for owner_mid in owner_mids:
            for candidate in self.author_source.fetch_recent_videos(owner_mid, since):
                if self._allow_candidate(candidate, enforce_tid=False):
                    backfill_candidates.append(candidate)

        merged_backfill = self._merge_candidates(backfill_candidates, merged)
        entries = sorted(merged_backfill.values(), key=lambda item: item.discovered_at)
        return DiscoverResult(entries=entries, owner_mids=owner_mids)

    def build_from_owner_mids(
        self,
        owner_mids: list[int],
        now: datetime | None = None,
        *,
        progress_callback: Callable[[int, int, int, int], None] | None = None,
        error_callback: Callable[[int, int, int, Exception], None] | None = None,
    ) -> DiscoverResult:
        current_time = now or datetime.now()
        since = current_time - timedelta(days=self.config.lookback_days)
        normalized_owner_mids = sorted({int(owner_mid) for owner_mid in owner_mids})
        total_owners = len(normalized_owner_mids)

        candidates: list[CandidateVideo] = []
        for index, owner_mid in enumerate(normalized_owner_mids, start=1):
            try:
                author_candidates = self.author_source.fetch_recent_videos(owner_mid, since)
            except Exception as exc:  # noqa: BLE001
                if error_callback is not None:
                    error_callback(owner_mid, index, total_owners, exc)
                continue

            for candidate in author_candidates:
                if self._allow_candidate(candidate, enforce_tid=False):
                    candidates.append(candidate)
            if progress_callback is not None:
                progress_callback(owner_mid, index, total_owners, len(author_candidates))

        merged = self._merge_candidates(candidates)
        entries = sorted(merged.values(), key=lambda item: item.discovered_at)
        return DiscoverResult(entries=entries, owner_mids=normalized_owner_mids)

    def _collect_seed_candidates(self) -> list[CandidateVideo]:
        candidates: list[CandidateVideo] = []
        for source in [*self.hot_sources, *self.partition_sources]:
            for candidate in source.fetch():
                if self._allow_candidate(candidate, enforce_tid=True):
                    candidates.append(candidate)
        return candidates

    def _allow_candidate(self, candidate: CandidateVideo, *, enforce_tid: bool) -> bool:
        if enforce_tid and not self.config.allows_tid(candidate.tid):
            return False
        if not self.config.allows_duration(candidate.duration_seconds):
            return False
        return True

    def _merge_candidates(
        self,
        candidates: list[CandidateVideo],
        existing: dict[str, VideoPoolEntry] | None = None,
    ) -> dict[str, VideoPoolEntry]:
        merged = dict(existing or {})
        for candidate in candidates:
            entry = merged.get(candidate.bvid)
            if entry is None:
                merged[candidate.bvid] = VideoPoolEntry(
                    bvid=candidate.bvid,
                    source_type=candidate.source_type,
                    source_ref=candidate.source_ref,
                    discovered_at=candidate.discovered_at,
                    last_seen_at=candidate.discovered_at,
                    owner_mid=candidate.owner_mid,
                    tid=candidate.tid,
                    pubdate=candidate.pubdate,
                    duration_seconds=candidate.duration_seconds,
                    seed_score=candidate.seed_score,
                    source_refs=[candidate.source_ref],
                )
                continue

            if self._should_replace_source(entry.source_type, candidate.source_type):
                entry.source_type = candidate.source_type
                entry.source_ref = candidate.source_ref
            entry.merge(candidate)
        return merged

    @staticmethod
    def _should_replace_source(old_type: str, new_type: str) -> bool:
        return SOURCE_PRIORITY.get(new_type, 0) > SOURCE_PRIORITY.get(old_type, 0)
