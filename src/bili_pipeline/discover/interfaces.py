from __future__ import annotations

from datetime import datetime
from typing import Protocol

from bili_pipeline.models import CandidateVideo


class SeedSource(Protocol):
    """Provides seed videos from hot lists or partition lists."""

    def fetch(self) -> list[CandidateVideo]:
        """Return discovery candidates."""


class AuthorVideoSource(Protocol):
    """Provides videos uploaded by the author in a time window."""

    def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list[CandidateVideo]:
        """Return recent author videos within the lookback window."""
