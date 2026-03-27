from __future__ import annotations

from datetime import datetime
from typing import Any

from bilibili_api import comment
from bilibili_api.user import User
from bilibili_api.video import Video

from bili_pipeline.models import CommentItem, CommentSnapshot, MetaResult, StatSnapshot
from bili_pipeline.utils import run_async


def _from_timestamp(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value)


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _video_with_credential(bvid: str, credential: Any | None = None) -> Video:
    if credential is None:
        return Video(bvid=bvid)
    return Video(bvid=bvid, credential=credential)


def crawl_video_meta(bvid: str, credential: Any | None = None) -> MetaResult:
    video = _video_with_credential(bvid, credential)
    info = run_async(video.get_info())
    tags_payload = run_async(video.get_tags())

    owner = info.get("owner", {})
    owner_mid = owner.get("mid")
    rights = info.get("rights", {})
    subtitle = info.get("subtitle", {})
    dimension = info.get("dimension", {})

    follower_count = None
    following_count = None
    video_count = None
    user_info: dict[str, Any] = {}
    relation_info: dict[str, Any] = {}
    overview_stat: dict[str, Any] = {}
    if owner_mid:
        user = User(owner_mid, credential=credential) if credential is not None else User(owner_mid)
        user_info = run_async(user.get_user_info())
        relation_info = run_async(user.get_relation_info())
        overview_stat = run_async(user.get_overview_stat())
        follower_count = relation_info.get("follower")
        following_count = relation_info.get("following")
        video_count = overview_stat.get("video")

    pages_info = info.get("pages", [])
    cid = pages_info[0].get("cid") if pages_info else None
    tags = [item.get("tag_name", "") for item in tags_payload if item.get("tag_name")]
    is_activity_participant = bool(info.get("mission_id") or info.get("season_id") or info.get("honor_reply"))
    official_info = user_info.get("official", {})
    vip_info = user_info.get("vip", {})
    owner_verified_title = official_info.get("title") or official_info.get("desc")
    owner_verified = bool(
        owner_verified_title
        or official_info.get("role")
        or official_info.get("type", -1) not in (-1, None)
    )
    is_interactive_video = bool(rights.get("is_stein_gate"))
    is_downloadable = bool(rights.get("download"))
    is_reprint_allowed = not bool(rights.get("no_reprint"))
    is_paid_video = bool(
        rights.get("pay")
        or rights.get("arc_pay")
        or rights.get("ugc_pay")
        or rights.get("free_watch")
        or info.get("is_chargeable_season")
        or info.get("is_upower_exclusive")
        or info.get("is_upower_play")
    )

    return MetaResult(
        bvid=bvid,
        aid=_to_int(info.get("aid")),
        title=info.get("title", ""),
        desc=info.get("desc", ""),
        pic=info.get("pic", ""),
        dynamic=info.get("dynamic", ""),
        tags=tags,
        tag_details=tags_payload,
        videos=_to_int(info.get("videos")),
        tid=_to_int(info.get("tid")),
        tid_v2=_to_int(info.get("tid_v2")),
        tname=info.get("tname", ""),
        tname_v2=info.get("tname_v2", ""),
        copyright=_to_int(info.get("copyright")),
        owner_mid=owner_mid,
        owner_name=owner.get("name"),
        owner_face=owner.get("face") or user_info.get("face"),
        owner_sign=user_info.get("sign"),
        owner_gender=user_info.get("sex"),
        owner_level=_to_int(user_info.get("level")),
        owner_verified=owner_verified,
        owner_verified_title=owner_verified_title,
        owner_vip_type=_to_int(vip_info.get("type")),
        owner_follower_count=follower_count,
        owner_following_count=following_count,
        owner_video_count=video_count,
        is_activity_participant=is_activity_participant,
        duration=_to_int(info.get("duration")),
        state=_to_int(info.get("state")),
        pubdate=_from_timestamp(info.get("pubdate")),
        cid=cid,
        resolution_width=_to_int(dimension.get("width")),
        resolution_height=_to_int(dimension.get("height")),
        resolution_rotate=_to_int(dimension.get("rotate")),
        is_story=bool(info.get("is_story")),
        is_interactive_video=is_interactive_video,
        is_downloadable=is_downloadable,
        is_reprint_allowed=is_reprint_allowed,
        is_collaboration=bool(rights.get("is_cooperation")),
        is_360=bool(rights.get("is_360")),
        is_paid_video=is_paid_video,
        pages_info=pages_info,
        rights=rights,
        subtitle=subtitle,
        uploader_profile=user_info,
        uploader_relation=relation_info,
        uploader_overview=overview_stat,
        raw_payload={
            "info": info,
            "tags": tags_payload,
            "uploader_profile": user_info,
            "uploader_relation": relation_info,
            "uploader_overview": overview_stat,
        },
    )


def crawl_stat_snapshot(bvid: str, credential: Any | None = None) -> StatSnapshot:
    info = run_async(_video_with_credential(bvid, credential).get_info())
    stat = info.get("stat", {})

    return StatSnapshot(
        bvid=bvid,
        snapshot_time=datetime.now(),
        stat_view=int(stat.get("view", 0)),
        stat_like=int(stat.get("like", 0)),
        stat_coin=int(stat.get("coin", 0)),
        stat_favorite=int(stat.get("favorite", 0)),
        stat_share=int(stat.get("share", 0)),
        stat_reply=int(stat.get("reply", 0)),
        stat_danmu=int(stat.get("danmaku", 0)),
        stat_dislike=int(stat.get("dislike", 0)),
        stat_his_rank=int(stat.get("his_rank", 0)),
        stat_now_rank=int(stat.get("now_rank", 0)),
        stat_evaluation=str(stat.get("evaluation") or ""),
        raw_payload=stat,
    )


def crawl_latest_comments(bvid: str, limit: int = 10, credential: Any | None = None) -> CommentSnapshot:
    info = run_async(_video_with_credential(bvid, credential).get_info())
    kwargs: dict[str, Any] = {}
    if credential is not None:
        kwargs["credential"] = credential
    payload = run_async(
        comment.get_comments_lazy(
            oid=int(info["aid"]),
            type_=comment.CommentResourceType.VIDEO,
            order=comment.OrderType.LIKE,
            **kwargs,
        )
    )
    replies = payload.get("replies") or []

    items: list[CommentItem] = []
    for reply in replies[:limit]:
        member = reply.get("member", {})
        items.append(
            CommentItem(
                rpid=int(reply.get("rpid", 0)),
                message=reply.get("content", {}).get("message", ""),
                like=int(reply.get("like", 0)),
                ctime=_from_timestamp(reply.get("ctime")),
                mid=int(member["mid"]) if member.get("mid") else None,
                uname=member.get("uname"),
            )
        )

    return CommentSnapshot(
        bvid=bvid,
        snapshot_time=datetime.now(),
        limit=limit,
        items=items,
        raw_payload=payload,
    )
