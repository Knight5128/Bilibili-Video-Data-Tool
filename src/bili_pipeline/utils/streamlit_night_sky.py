"""Shared night-sky background for Streamlit apps."""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st


@dataclass(frozen=True)
class NightSkyConfig:
    """Animation parameters for the shared Streamlit background."""

    meteor_count: int = 18
    meteor_min_duration_s: float = 4.8
    meteor_max_duration_s: float = 9.6
    meteor_min_offset_s: float = 0.0
    meteor_max_offset_s: float = 7.0
    meteor_min_length_px: int = 120
    meteor_max_length_px: int = 220
    meteor_head_size_px: int = 8


DEFAULT_NIGHT_SKY_CONFIG = NightSkyConfig()


def _lerp(start: float, end: float, ratio: float) -> float:
    return start + (end - start) * ratio


def _build_meteor_markup(config: NightSkyConfig) -> str:
    total = max(1, config.meteor_count)
    meteors: list[str] = []
    for idx in range(total):
        ratio = idx / max(1, total - 1)
        top = 4 + ((idx * 17) % 62)
        left = 56 + ((idx * 13) % 38)
        duration_ratio = ((idx * 7) % (total + 5)) / max(1, total + 4)
        offset_ratio = ((idx * 11) % (total + 3)) / max(1, total + 2)
        length_ratio = ((idx * 5) % (total + 7)) / max(1, total + 6)
        duration = _lerp(config.meteor_min_duration_s, config.meteor_max_duration_s, duration_ratio)
        offset = _lerp(config.meteor_min_offset_s, config.meteor_max_offset_s, offset_ratio)
        length = round(_lerp(config.meteor_min_length_px, config.meteor_max_length_px, length_ratio))
        opacity = _lerp(0.65, 0.98, ratio)
        thickness = 1 + (idx % 2)
        meteors.append(
            (
                '<span class="night-sky-meteor" '
                f'style="--top:{top}%; --left:{left}%; --duration:{duration:.2f}s; '
                f'--delay:-{offset:.2f}s; --length:{length}px; --opacity:{opacity:.2f}; '
                f'--thickness:{thickness}px; --head-size:{config.meteor_head_size_px}px;"></span>'
            )
        )
    return "".join(meteors)


def render_night_sky_background(config: NightSkyConfig = DEFAULT_NIGHT_SKY_CONFIG) -> None:
    """Render a black night sky with animated white meteors."""

    meteor_markup = _build_meteor_markup(config)
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: transparent;
        }}

        div[data-testid="stAppViewContainer"] {{
            background:
                radial-gradient(circle at 20% 15%, rgba(77, 95, 160, 0.18) 0%, rgba(77, 95, 160, 0) 30%),
                radial-gradient(circle at 80% 0%, rgba(121, 141, 214, 0.12) 0%, rgba(121, 141, 214, 0) 22%),
                linear-gradient(180deg, #02030a 0%, #040714 55%, #02030a 100%);
        }}

        div[data-testid="stHeader"] {{
            background: rgba(2, 3, 10, 0);
        }}

        div[data-testid="stAppViewContainer"] > .main .block-container,
        section[data-testid="stSidebar"] {{
            position: relative;
            z-index: 1;
        }}

        section[data-testid="stSidebar"] > div {{
            background: linear-gradient(180deg, rgba(8, 11, 24, 0.92) 0%, rgba(5, 8, 18, 0.88) 100%);
            backdrop-filter: blur(10px);
        }}

        .night-sky-layer {{
            position: fixed;
            inset: 0;
            z-index: 0;
            overflow: hidden;
            pointer-events: none;
        }}

        .night-sky-layer::before {{
            content: "";
            position: absolute;
            inset: 0;
            background:
                radial-gradient(2px 2px at 8% 18%, rgba(255, 255, 255, 0.95) 0 45%, transparent 55%),
                radial-gradient(1.5px 1.5px at 16% 62%, rgba(255, 255, 255, 0.92) 0 42%, transparent 54%),
                radial-gradient(1.6px 1.6px at 26% 34%, rgba(255, 255, 255, 0.88) 0 42%, transparent 54%),
                radial-gradient(2px 2px at 34% 72%, rgba(255, 255, 255, 0.98) 0 42%, transparent 55%),
                radial-gradient(1.3px 1.3px at 42% 28%, rgba(255, 255, 255, 0.92) 0 40%, transparent 54%),
                radial-gradient(1.7px 1.7px at 48% 14%, rgba(255, 255, 255, 0.9) 0 42%, transparent 54%),
                radial-gradient(1.6px 1.6px at 58% 58%, rgba(255, 255, 255, 0.9) 0 40%, transparent 54%),
                radial-gradient(1.8px 1.8px at 66% 26%, rgba(255, 255, 255, 0.95) 0 42%, transparent 55%),
                radial-gradient(1.4px 1.4px at 74% 70%, rgba(255, 255, 255, 0.88) 0 40%, transparent 54%),
                radial-gradient(1.7px 1.7px at 82% 44%, rgba(255, 255, 255, 0.92) 0 42%, transparent 54%),
                radial-gradient(2px 2px at 90% 20%, rgba(255, 255, 255, 0.96) 0 45%, transparent 55%),
                radial-gradient(1.3px 1.3px at 94% 62%, rgba(255, 255, 255, 0.88) 0 40%, transparent 54%);
            opacity: 0.88;
        }}

        .night-sky-layer::after {{
            content: "";
            position: absolute;
            inset: 0;
            background:
                radial-gradient(circle at center, rgba(255, 255, 255, 0.03) 0%, rgba(255, 255, 255, 0) 48%),
                radial-gradient(circle at 50% -5%, rgba(111, 127, 212, 0.12) 0%, rgba(111, 127, 212, 0) 34%);
        }}

        .night-sky-meteor {{
            position: absolute;
            top: var(--top);
            left: var(--left);
            width: var(--length);
            height: var(--thickness);
            border-radius: 999px;
            opacity: 0;
            transform: rotate(-35deg);
            transform-origin: left center;
            background: linear-gradient(90deg, rgba(255, 255, 255, 0.96) 0%, rgba(255, 255, 255, 0) 100%);
            box-shadow:
                0 0 8px rgba(255, 255, 255, 0.42),
                0 0 16px rgba(255, 255, 255, 0.2);
            animation: night-sky-meteor var(--duration) linear infinite;
            animation-delay: var(--delay);
        }}

        .night-sky-meteor::before {{
            content: "";
            position: absolute;
            left: calc(var(--head-size) * -0.25);
            top: 50%;
            width: var(--head-size);
            height: var(--head-size);
            border-radius: 50%;
            transform: translateY(-50%);
            background: radial-gradient(circle, rgba(255, 255, 255, 0.98) 0%, rgba(255, 255, 255, 0.28) 58%, rgba(255, 255, 255, 0) 100%);
        }}

        @keyframes night-sky-meteor {{
            0% {{
                transform: translate3d(160px, -160px, 0) rotate(-35deg);
                opacity: 0;
            }}
            10% {{
                opacity: var(--opacity);
            }}
            72% {{
                opacity: calc(var(--opacity) * 0.92);
            }}
            100% {{
                transform: translate3d(-540px, 540px, 0) rotate(-35deg);
                opacity: 0;
            }}
        }}
        </style>
        <div class="night-sky-layer" aria-hidden="true">{meteor_markup}</div>
        """,
        unsafe_allow_html=True,
    )
