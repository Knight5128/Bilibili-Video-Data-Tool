from __future__ import annotations

import asyncio
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from bilibili_api import hot, video_zone
from bilibili_api.user import User, VideoOrder
from bilibili_api.video import Video

from bili_pipeline.discover.interfaces import AuthorVideoSource, SeedSource
from bili_pipeline.models import CandidateVideo, RankboardEntry
from bili_pipeline.utils import run_async


def _run_async(awaitable):
    return asyncio.run(awaitable)


def _fetch_json_response(url: str, headers: dict[str, str] | None = None) -> dict:
    request = Request(url, headers=headers or {})
    with urlopen(request, timeout=30) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


async def _get_json_response(url: str, headers: dict[str, str] | None = None) -> dict:
    return await asyncio.to_thread(_fetch_json_response, url, headers)


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


def _parse_rankboard_item(
    item: dict,
    *,
    board_rid: int,
    board_name: str,
    board_rank: int,
    board_url: str,
    discovered_at: datetime,
) -> RankboardEntry:
    owner = item.get("owner", {})
    stat = item.get("stat", {})
    source_ref = f"rankboard:rid={board_rid}:rank={board_rank}"
    return RankboardEntry(
        board_rid=board_rid,
        board_name=board_name,
        board_rank=board_rank,
        bvid=item["bvid"],
        source_type="rankboard",
        source_ref=source_ref,
        discovered_at=discovered_at,
        owner_mid=owner.get("mid"),
        tid=item.get("tid"),
        pubdate=_from_timestamp(item.get("pubdate") or item.get("ctime")),
        duration_seconds=item.get("duration"),
        seed_score=_score_from_stat(stat),
        source_refs=[source_ref, board_url],
    )


def _parse_length_to_seconds(length: str | None) -> int | None:
    if not length:
        return None
    parts = [int(part) for part in length.split(":")]
    total = 0
    for part in parts:
        total = total * 60 + part
    return total


def _sleep_with_jitter(base_seconds: float, jitter_seconds: float) -> None:
    delay = max(0.0, base_seconds) + (random.uniform(0.0, max(0.0, jitter_seconds)) if jitter_seconds > 0 else 0.0)
    if delay > 0:
        time.sleep(delay)


def _is_retryable_request_error(exc: Exception) -> bool:
    message = " ".join(str(exc).split())
    lowered = message.lower()
    retry_markers = (
        "412",
        "429",
        "network error",
        "timeout",
        "timed out",
        "temporarily",
        "connection reset",
        "server disconnected",
        "service unavailable",
        "too many requests",
    )
    return any(marker in lowered for marker in retry_markers)


def _run_async_with_retry(
    awaitable_factory: Callable[[], object],
    *,
    request_interval_seconds: float = 0.0,
    request_jitter_seconds: float = 0.0,
    max_retries: int = 0,
    retry_backoff_seconds: float = 3.0,
) -> object:
    attempt = 0
    while True:
        _sleep_with_jitter(request_interval_seconds, request_jitter_seconds)
        try:
            return run_async(awaitable_factory())
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries or not _is_retryable_request_error(exc):
                raise
            backoff_seconds = max(0.0, retry_backoff_seconds) * (2**attempt)
            _sleep_with_jitter(backoff_seconds, request_jitter_seconds)
            attempt += 1


def fetch_owner_mid_by_bvid(
    bvid: str,
    *,
    request_interval_seconds: float = 0.0,
    request_jitter_seconds: float = 0.0,
    max_retries: int = 0,
    retry_backoff_seconds: float = 3.0,
) -> int | None:
    info = _run_async_with_retry(
        lambda: Video(bvid=bvid).get_info(),
        request_interval_seconds=request_interval_seconds,
        request_jitter_seconds=request_jitter_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    owner = info.get("owner", {})
    owner_mid = owner.get("mid")
    return int(owner_mid) if owner_mid is not None else None


def resolve_owner_mids_from_bvids(
    bvids: list[str],
    *,
    request_interval_seconds: float = 0.0,
    request_jitter_seconds: float = 0.0,
    max_retries: int = 0,
    retry_backoff_seconds: float = 3.0,
    progress_callback: Callable[[str, int, int, int | None], None] | None = None,
    error_callback: Callable[[str, int, int, Exception], None] | None = None,
) -> tuple[list[int], list[str]]:
    owner_mids: list[int] = []
    failed_bvids: list[str] = []
    unique_bvids = list(dict.fromkeys(bvids))
    total = len(unique_bvids)

    for index, bvid in enumerate(unique_bvids, start=1):
        try:
            owner_mid = fetch_owner_mid_by_bvid(
                bvid,
                request_interval_seconds=request_interval_seconds,
                request_jitter_seconds=request_jitter_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            failed_bvids.append(bvid)
            if error_callback is not None:
                error_callback(bvid, index, total, exc)
            continue

        if owner_mid is None:
            failed_bvids.append(bvid)
            continue

        if owner_mid not in owner_mids:
            owner_mids.append(owner_mid)
        if progress_callback is not None:
            progress_callback(bvid, index, total, owner_mid)

    return owner_mids, failed_bvids


@dataclass(slots=True)
class BilibiliHotSource(SeedSource):
    pn: int = 1
    ps: int = 20
    fetch_all_pages: bool = False
    max_pages: int = 20
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        if not self.fetch_all_pages:
            payload = _run_async_with_retry(
                lambda: hot.get_hot_videos(pn=self.pn, ps=self.ps),
                request_interval_seconds=self.request_interval_seconds,
                request_jitter_seconds=self.request_jitter_seconds,
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
            source_ref = f"hot:pn={self.pn}:ps={self.ps}"
            return [_parse_seed_item(item, "hot", source_ref, discovered_at) for item in payload.get("list", [])]

        candidates: list[CandidateVideo] = []
        for page_num in range(self.pn, self.pn + self.max_pages):
            payload = _run_async_with_retry(
                lambda page_num=page_num: hot.get_hot_videos(pn=page_num, ps=self.ps),
                request_interval_seconds=self.request_interval_seconds,
                request_jitter_seconds=self.request_jitter_seconds,
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
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
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        payload = _run_async_with_retry(
            lambda: hot.get_weekly_hot_videos(week=self.week),
            request_interval_seconds=self.request_interval_seconds,
            request_jitter_seconds=self.request_jitter_seconds,
            max_retries=self.max_retries,
            retry_backoff_seconds=self.retry_backoff_seconds,
        )
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
class BilibiliRankboardSource:
    board_rid: int
    board_name: str
    board_url: str
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch(self) -> list[RankboardEntry]:
        discovered_at = datetime.now()
        payload = _run_async_with_retry(
            lambda: _get_json_response(
                "https://api.bilibili.com/x/web-interface/ranking/v2?"
                + urlencode(
                    {
                        "rid": self.board_rid,
                        "type": "all",
                        "web_location": "333.934",
                    }
                ),
                headers={
                    "Referer": self.board_url,
                    "Origin": "https://www.bilibili.com",
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                },
            ),
            request_interval_seconds=self.request_interval_seconds,
            request_jitter_seconds=self.request_jitter_seconds,
            max_retries=self.max_retries,
            retry_backoff_seconds=self.retry_backoff_seconds,
        )
        if payload.get("code") != 0:
            raise ValueError(f"排行榜接口返回异常: {payload.get('message') or payload.get('msg') or payload.get('code')}")
        items = payload.get("data", {}).get("list", [])
        return [
            _parse_rankboard_item(
                item,
                board_rid=self.board_rid,
                board_name=self.board_name,
                board_rank=index,
                board_url=self.board_url,
                discovered_at=discovered_at,
            )
            for index, item in enumerate(items, start=1)
        ]


@dataclass(slots=True)
class BilibiliZoneNewVideosSource(SeedSource):
    tid: int
    page_num: int = 1
    page_size: int = 10
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        payload = _run_async_with_retry(
            lambda: video_zone.get_zone_new_videos(tid=self.tid, page_num=self.page_num, page_size=self.page_size),
            request_interval_seconds=self.request_interval_seconds,
            request_jitter_seconds=self.request_jitter_seconds,
            max_retries=self.max_retries,
            retry_backoff_seconds=self.retry_backoff_seconds,
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
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch(self) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        source_ref = f"partition_recent:tid={self.tid}:since={self.since.date().isoformat()}"
        candidates: list[CandidateVideo] = []

        for page_num in range(1, self.max_pages + 1):
            payload = _run_async_with_retry(
                lambda page_num=page_num: video_zone.get_zone_new_videos(
                    tid=self.tid,
                    page_num=page_num,
                    page_size=self.page_size,
                ),
                request_interval_seconds=self.request_interval_seconds,
                request_jitter_seconds=self.request_jitter_seconds,
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
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
    request_interval_seconds: float = 0.0
    request_jitter_seconds: float = 0.0
    max_retries: int = 0
    retry_backoff_seconds: float = 3.0

    def fetch_recent_videos(
        self,
        owner_mid: int,
        since: datetime,
        until: datetime | None = None,
    ) -> list[CandidateVideo]:
        discovered_at = datetime.now()
        user = User(owner_mid)
        candidates: list[CandidateVideo] = []
        upper_bound = until or datetime.max

        for page_num in range(1, self.max_pages + 1):
            payload = _run_async_with_retry(
                lambda page_num=page_num: user.get_videos(pn=page_num, ps=self.page_size, order=VideoOrder.PUBDATE),
                request_interval_seconds=self.request_interval_seconds,
                request_jitter_seconds=self.request_jitter_seconds,
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
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
                if candidate.pubdate > upper_bound:
                    continue
                candidates.append(candidate)

            if stop_paging:
                break

        return candidates
