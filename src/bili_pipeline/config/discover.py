from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class DiscoverConfig:
    """Runtime parameters for building the video pool."""

    lookback_days: int = 90
    partition_tid_whitelist: set[int] = field(default_factory=set)
    partition_tid_blacklist: set[int] = field(default_factory=set)
    enable_author_backfill: bool = True
    enable_duration_filter: bool = False
    min_duration_seconds: int = 15

    def allows_tid(self, tid: int | None) -> bool:
        if tid is None:
            return True
        if tid in self.partition_tid_blacklist:
            return False
        if self.partition_tid_whitelist and tid not in self.partition_tid_whitelist:
            return False
        return True

    def allows_duration(self, duration_seconds: int | None) -> bool:
        if not self.enable_duration_filter or duration_seconds is None:
            return True
        return duration_seconds >= self.min_duration_seconds
