from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

from bilibili_api import hot, video_zone
from bilibili_api.user import User, VideoOrder
from bilibili_api.video import Video

from bili_pipeline.discover.interfaces import AuthorVideoSource, SeedSource
from bili_pipeline.models import CandidateVideo
from bili_pipeline.utils import run_async


def _run_async(awaitable):
    return asyncio.run(awaitable)


def _from_timestamp(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value)


def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _from_timestamp(value)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def _score_from_stat(stat: dict) -> float:
    return float(
        stat.get("view", 0)
        + stat.get("like", 0) * 2
        + stat.get("reply", 0) * 3
        + stat.get("coin", 0) * 2
    )


def _parse_seed_item(
    item: dict,
    source_type: str,
    source_ref: str,
    discovered_at: datetime,
) -> CandidateVideo:
    owner = item.get("owner", {})
    stat = item.get("stat", {})
    return CandidateVideo(
        bvid=item["bvid"],
        source_type=source_type,
        source_ref=source_ref,
        discovered_at=discovered_at,
        owner_mid=owner.get("mid"),
        tid=item.get("tid"),
        pubdate=_from_timestamp(item.get("pubdate")),
        duration_seconds=item.get("duration"),
        seed_score=_score_from_stat(stat),
    )


def _parse_zone_item(item: dict, source_ref: str, discovered_at: datetime, fallback_tid: int | None = None) -> CandidateVideo:
    return CandidateVideo(
        bvid=item["bvid"],
        source_type="partition",
        source_ref=source_ref,
        discovered_at=discovered_at,
        owner_mid=item.get("mid"),
        tid=item.get("typeid") or item.get("tid") or fallback_tid,
        pubdate=_parse_datetime(item.get("created") or item.get("create") or item.get("pubdate")),
        duration_seconds=_parse_length_to_seconds(item.get("length") or item.get("duration")),
        seed_score=float(item.get("play", 0)) + float(item.get("favorites", 0)) * 2,
    )


def _parse_user_video(item: dict, source_ref: str, discovered_at: datetime) -> CandidateVideo:
    return CandidateVideo(
        bvid=item["bvid"],
        source_type="author_expand",
        source_ref=source_ref,
        discovered_at=discovered_at,
        owner_mid=item.get("mid"),
        tid=item.get("typeid") or item.get("tid"),
        pubdate=_parse_datetime(item.get("created") or item.get("create") or item.get("pubdate")),
        duration_seconds=_parse_length_to_seconds(item.get("length") or item.get("duration")),
        seed_score=float(item.get("play", 0)) + float(item.get("favorites", 0)) * 2,
    )


def _parse_length_to_seconds(length: str | None) -> int | None:
    if not length:
        return None
    parts = [int(part) for part in length.split(":")]
    total = 0
    for part in parts:
        total = total * 60 + part
    return total


def fetch_owner_mid_by_bvid(bvid: str) -> int | None:
    info = run_async(Video(bvid=bvid).get_info())
    owner = info.get("owner", {})
    owner_mid = owner.get("mid")
    return int(owner_mid) if owner_mid is not None else None


def resolve_owner_mids_from_bvids(bvids: list[str]) -> tuple[list[int], list[str]]:
    owner_mids: list[int] = []
    failed_bvids: list[str] = []

    for bvid in dict.fromkeys(bvids):
        try:
            owner_mid = fetch_owner_mid_by_bvid(bvid)
        except Exception:  # noqa: BLE001
            failed_bvids.append(bvid)
            continue

        if owner_mid is None:
            failed_bvids.append(bvid)
            continue

        if owner_mid not in owner_mids:
            owner_mids.append(owner_mid)

    return owner_mids, failed_bvids


@dataclass(slots=True)
class BilibiliHotSource(SeedSource):
    pn: int = 1
    ps: int = 20
    fetch_all_pages: bool = False
    max_pages: int = 20

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        if not self.fetch_all_pages:
            payload = _run_async(hot.get_hot_videos(pn=self.pn, ps=self.ps))
            source_ref = f"hot:pn={self.pn}:ps={self.ps}"
            return [_parse_seed_item(item, "hot", source_ref, discovered_at) for item in payload.get("list", [])]

        candidates: list[CandidateVideo] = []
        for page_num in range(self.pn, self.pn + self.max_pages):
            payload = _run_async(hot.get_hot_videos(pn=page_num, ps=self.ps))
            items = payload.get("list", [])
            if not items:
                break
            source_ref = f"hot:pn={page_num}:ps={self.ps}"
            candidates.extend(_parse_seed_item(item, "hot", source_ref, discovered_at) for item in items)
            if payload.get("no_more"):
                break
        return candidates


@dataclass(slots=True)
class BilibiliWeeklyHotSource(SeedSource):
    week: int = 1

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        payload = _run_async(hot.get_weekly_hot_videos(week=self.week))
        source_ref = f"weekly_hot:week={self.week}"
        return [_parse_seed_item(item, "weekly_hot", source_ref, discovered_at) for item in payload.get("list", [])]


@dataclass(slots=True)
class BilibiliZoneTop10Source(SeedSource):
    tid: int
    day: int = 7

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        payload = _run_async(video_zone.get_zone_top10(tid=self.tid, day=self.day))
        source_ref = f"partition_top10:tid={self.tid}:day={self.day}"
        return [_parse_zone_item(item, source_ref, discovered_at, fallback_tid=self.tid) for item in payload]


@dataclass(slots=True)
class BilibiliZoneNewVideosSource(SeedSource):
    tid: int
    page_num: int = 1
    page_size: int = 10

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        payload = _run_async(
            video_zone.get_zone_new_videos(tid=self.tid, page_num=self.page_num, page_size=self.page_size)
        )
        source_ref = f"partition_new:tid={self.tid}:pn={self.page_num}:ps={self.page_size}"
        items = payload.get("archives", payload.get("items", []))
        return [_parse_seed_item(item, "partition", source_ref, discovered_at) for item in items]


@dataclass(slots=True)
class BilibiliZoneRecentVideosSource(SeedSource):
    tid: int
    since: datetime
    page_size: int = 30
    max_pages: int = 200

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        source_ref = f"partition_recent:tid={self.tid}:since={self.since.date().isoformat()}"
        candidates: list[CandidateVideo] = []

        for page_num in range(1, self.max_pages + 1):
            payload = _run_async(video_zone.get_zone_new_videos(tid=self.tid, page_num=page_num, page_size=self.page_size))
            items = payload.get("archives", payload.get("items", []))
            if not items:
                break

            stop_paging = False
            for item in items:
                candidate = _parse_seed_item(item, "partition", source_ref, discovered_at)
                if candidate.pubdate is not None and candidate.pubdate < self.since:
                    stop_paging = True
                    continue
                candidates.append(candidate)

            if stop_paging:
                break

        return candidates


@dataclass(slots=True)
class BilibiliUserRecentVideoSource(AuthorVideoSource):
    page_size: int = 30
    max_pages: int = 20

    def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        user = User(owner_mid)
        candidates: list[CandidateVideo] = []

        for page_num in range(1, self.max_pages + 1):
            payload = _run_async(user.get_videos(pn=page_num, ps=self.page_size, order=VideoOrder.PUBDATE))
            items = payload.get("list", {}).get("vlist", [])
            if not items:
                break

            stop_paging = False
            for item in items:
                candidate = _parse_user_video(item, f"owner:{owner_mid}", discovered_at)
                if candidate.pubdate is None:
                    continue
                if candidate.pubdate < since:
                    stop_paging = True
                    continue
                candidates.append(candidate)

            if stop_paging:
                break

        return candidates
