from __future__ import annotations

import re
import webbrowser


_BVID_PATTERN = re.compile(r"(BV[0-9A-Za-z]{10})", re.IGNORECASE)
_OWNER_MID_PATTERN = re.compile(r"space\.bilibili\.com/(\d+)|(\d+)")


def normalize_bvid(raw_value: str) -> str | None:
    text = (raw_value or "").strip()
    if not text:
        return None
    match = _BVID_PATTERN.search(text)
    if match is None:
        return None
    matched = match.group(1)
    return f"BV{matched[2:]}"


def normalize_owner_mid(raw_value: str) -> str | None:
    text = (raw_value or "").strip().rstrip("/")
    if not text:
        return None
    match = _OWNER_MID_PATTERN.search(text)
    if match is None:
        return None
    return match.group(1) or match.group(2)


def build_video_url(bvid: str) -> str:
    return f"https://www.bilibili.com/video/{bvid}"


def build_owner_space_url(owner_mid: str) -> str:
    return f"https://space.bilibili.com/{owner_mid}"


def open_in_default_browser(url: str) -> bool:
    return bool(webbrowser.open_new_tab(url))
