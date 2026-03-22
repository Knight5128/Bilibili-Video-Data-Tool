from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from bili_pipeline.discover.interfaces import AuthorVideoSource, SeedSource
from bili_pipeline.models import CandidateVideo


@dataclass(slots=True)
class StaticSeedSource(SeedSource):
    items: list[CandidateVideo]

    def fetch(self) -> list[CandidateVideo]:
        return list(self.items)


@dataclass(slots=True)
class StaticAuthorVideoSource(AuthorVideoSource):
    mapping: dict[int, list[CandidateVideo]]

    def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list[CandidateVideo]:
        results: list[CandidateVideo] = []
        for candidate in self.mapping.get(owner_mid, []):
            if candidate.pubdate is None or candidate.pubdate >= since:
                results.append(candidate)
        return results
