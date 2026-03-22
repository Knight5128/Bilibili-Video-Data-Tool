from __future__ import annotations

import base64
import json
from html import escape
from pathlib import Path

from bilibili_api import Credential
import pandas as pd
import streamlit as st

from bili_pipeline.crawl_api import (
    crawl_bvid_list_from_csv,
    crawl_full_video_bundle,
    crawl_latest_comments,
    crawl_media_assets,
    crawl_stat_snapshot,
    crawl_video_meta,
    stream_media_to_store,
)
from bili_pipeline.field_reference import (
    FIELD_REFERENCE_DOC_NAME,
    SECTION_TITLES,
    build_field_reference_rows,
    sync_field_reference_markdown,
)
from bili_pipeline.models import MediaDownloadStrategy, OSSStorageConfig
from bili_pipeline.storage import OSSMediaStore, SQLiteCrawlerStore
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvd-crawler.png"
FIELD_REFERENCE_DOC_PATH = APP_DIR / FIELD_REFERENCE_DOC_NAME


def _build_logo_data_uri(logo_path: Path) -> str | None:
    if not logo_path.exists():
        return None
    mime_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }
    mime_type = mime_types.get(logo_path.suffix.lower())
    if mime_type is None:
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _render_centered_header(title: str, logo_path: Path) -> None:
    safe_title = escape(title)
    logo_uri = _build_logo_data_uri(logo_path)
    if logo_uri is None:
        st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.25rem;'>{safe_title}</h1>", unsafe_allow_html=True)
        return
    st.markdown(
        f"""
        <div style="display: flex; justify-content: center; align-items: center; gap: 0.75rem; margin-bottom: 0.25rem;">
            <img src="{logo_uri}" alt="{safe_title} logo" style="height: 3.5rem; width: 3.5rem; object-fit: contain;" />
            <h1 style="margin: 0;">{safe_title}</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _inject_field_reference_panel_styles() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="column"]:has(#field-reference-sticky-anchor) {
            position: sticky;
            top: 1rem;
            align-self: flex-start;
        }

        div[data-testid="column"]:has(#field-reference-sticky-anchor) > div[data-testid="stVerticalBlock"] {
            max-height: calc(100vh - 2rem);
            overflow-y: auto;
            padding-right: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _build_credential_from_cookie(cookie_text: str) -> Credential | None:
    text = (cookie_text or "").strip()
    if not text:
        return None
    parts = [item.strip() for item in text.split(";") if item.strip()]
    cookies: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        cookies[key.strip()] = value.strip()
    sessdata = cookies.get("SESSDATA")
    bili_jct = cookies.get("bili_jct")
    buvid3 = cookies.get("buvid3")
    if not (sessdata and bili_jct):
        return None
    return Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3 or "")


def _build_media_strategy(
    *,
    db_path: str,
    max_height: int,
    chunk_size_mb: int,
    storage_backend: str,
    oss_endpoint: str,
    oss_bucket_name: str,
    oss_bucket_region: str,
    oss_access_key_id: str,
    oss_access_key_secret: str,
    oss_security_token: str,
    oss_object_prefix: str,
    oss_public_base_url: str,
) -> MediaDownloadStrategy:
    oss_config = None
    if storage_backend == "oss":
        oss_config = OSSStorageConfig(
            endpoint=oss_endpoint,
            bucket_name=oss_bucket_name,
            bucket_region=oss_bucket_region,
            access_key_id=oss_access_key_id,
            access_key_secret=oss_access_key_secret,
            security_token=oss_security_token,
            object_prefix=oss_object_prefix,
            public_base_url=oss_public_base_url,
        )
    return MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        sqlite_path=db_path,
        storage_backend=storage_backend,
        oss_config=oss_config,
    )


@st.cache_data(show_spinner=False)
def _get_field_reference_df() -> pd.DataFrame:
    return pd.DataFrame(build_field_reference_rows())


def _sync_field_reference_doc_once() -> None:
    if st.session_state.get("field_reference_doc_synced"):
        return
    sync_field_reference_markdown(FIELD_REFERENCE_DOC_PATH)
    st.session_state["field_reference_doc_synced"] = True


def _render_field_reference_panel(field_reference_df: pd.DataFrame) -> None:
    st.subheader("字段定义速查")
    st.caption("启动后常驻右侧，调试接口时可随时展开对照。")

    with st.expander("展开 / 收起字段定义", expanded=True):
        st.caption("该面板与独立 Markdown 字段说明文档共用同一份字段字典。")
        st.code(str(FIELD_REFERENCE_DOC_PATH), language="text")

        selected_section = st.selectbox(
            "选择要查看的数据模块",
            options=["全部"] + list(SECTION_TITLES.keys()),
            format_func=lambda item: "全部模块" if item == "全部" else SECTION_TITLES[item],
            key="field_reference_section",
        )

        active_df = (
            field_reference_df
            if selected_section == "全部"
            else field_reference_df[field_reference_df["数据模块"] == selected_section]
        )
        st.dataframe(active_df, width="stretch", hide_index=True, height=360)

        with st.expander("分模块展开查看", expanded=False):
            for section_key, section_title in SECTION_TITLES.items():
                with st.expander(section_title, expanded=False):
                    section_df = field_reference_df[field_reference_df["数据模块"] == section_key]
                    st.dataframe(section_df, width="stretch", hide_index=True, height=240)


def _load_asset_bytes(
    *,
    store: SQLiteCrawlerStore,
    asset_row: dict,
    target_bvid: str,
    strategy: MediaDownloadStrategy,
) -> bytes:
    cid = asset_row.get("cid")
    asset_type = asset_row.get("asset_type")
    if asset_row.get("storage_backend") != "oss":
        return store.read_asset_bytes(target_bvid, cid, asset_type)

    if not strategy.use_oss_media() or strategy.oss_config is None:
        raise ValueError("当前未启用可用的阿里云 OSS 配置，无法回读远程媒体文件。")

    active_bucket = strategy.oss_config.bucket_name.strip()
    row_bucket = str(asset_row.get("bucket_name") or "").strip()
    if row_bucket and active_bucket and row_bucket != active_bucket:
        raise ValueError("当前 OSS Bucket 与该资产记录不一致，请切换为正确的 Bucket 后再导出。")

    active_endpoint = strategy.oss_config.endpoint_with_scheme().rstrip("/")
    row_endpoint = str(asset_row.get("storage_endpoint") or "").strip().rstrip("/")
    if row_endpoint and active_endpoint and row_endpoint != active_endpoint:
        raise ValueError("当前 OSS Endpoint 与该资产记录不一致，请切换为正确的 Endpoint 后再导出。")

    return OSSMediaStore(strategy.oss_config).download_object_bytes(str(asset_row.get("object_key") or ""))


page_config = {"page_title": "Bilibili Video Data Crawler", "layout": "wide"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
_inject_field_reference_panel_styles()
render_night_sky_background()
_render_centered_header("Bilibili Video Data Crawler", LOGO_PATH)
st.markdown(
    "<p style='text-align: center; margin-bottom: 1rem;'>基于 bilibili-api 的B站视频数据（元数据/动态互动/评论/多媒体）采集工具，支持单视频抓取、批量 CSV 抓取、四类数据接口单独调试与 SQLite 元数据 / 阿里云 OSS 媒体存储查看。</p>",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.subheader("全局设置")
    db_path = st.text_input("SQLite 存储路径", value=str(Path("outputs") / "bili_video_data_crawler.db"))
    comment_limit_default = st.number_input("默认评论条数", min_value=1, max_value=100, value=10, step=1)
    chunk_size_mb = st.number_input("媒体分块大小（MB）", min_value=1, max_value=64, value=4, step=1)
    max_height = st.number_input("媒体最大分辨率（高度）", min_value=360, max_value=2160, value=1080, step=120)
    storage_backend = st.radio(
        "媒体文件存储方式",
        options=["sqlite", "oss"],
        format_func=lambda item: "SQLite 分块存储（原模式）" if item == "sqlite" else "阿里云 OSS + SQLite 元数据",
        help="结构化数据和运行记录始终写入本地 SQLite；此选项只控制视频/音频媒体文件存到哪里。",
    )
    cookie_text = st.text_area(
        "可选：B 站 Cookie（提升评论与媒体抓取稳定性，仅在本地内存中使用）",
        value="",
        height=80,
        help="建议只粘贴包含 SESSDATA、bili_jct、buvid3 的子串，例如：SESSDATA=...; bili_jct=...; buvid3=...",
    )
    st.caption("Cookie 和 OSS 密钥仅保存在当前 Streamlit 会话内存中，不会写入 SQLite。")

    oss_endpoint = ""
    oss_bucket_name = ""
    oss_bucket_region = ""
    oss_access_key_id = ""
    oss_access_key_secret = ""
    oss_security_token = ""
    oss_object_prefix = "bilibili-media"
    oss_public_base_url = ""
    trigger_oss_test = False

    if storage_backend == "oss":
        st.divider()
        st.subheader("阿里云 OSS 配置")
        oss_endpoint = st.text_input(
            "OSS Endpoint",
            value="oss-cn-hangzhou.aliyuncs.com",
            help="支持直接填写地域 Endpoint，例如 oss-cn-shanghai.aliyuncs.com，也可带 https:// 前缀。",
        )
        oss_bucket_name = st.text_input("Bucket 名称", value="")
        oss_bucket_region = st.text_input("Bucket 所在地域（可选）", value="", help="例如 cn-shanghai。若 SDK 已能从 Endpoint 推断，可留空。")
        oss_object_prefix = st.text_input("对象前缀", value="bilibili-media", help="上传到 OSS 的对象 key 前缀。")
        oss_public_base_url = st.text_input(
            "公共访问基础 URL（可选）",
            value="",
            help="若 Bucket 配置了 CDN 或公网自定义域名，可填入形如 https://cdn.example.com 的基础地址。",
        )
        oss_access_key_id = st.text_input("AccessKey ID", value="")
        oss_access_key_secret = st.text_input("AccessKey Secret", value="", type="password")
        oss_security_token = st.text_input("Security Token（可选）", value="", type="password")
        trigger_oss_test = st.button("测试 OSS 连接", width="stretch")

active_credential = _build_credential_from_cookie(cookie_text)
active_media_strategy = _build_media_strategy(
    db_path=db_path,
    max_height=int(max_height),
    chunk_size_mb=int(chunk_size_mb),
    storage_backend=storage_backend,
    oss_endpoint=oss_endpoint,
    oss_bucket_name=oss_bucket_name,
    oss_bucket_region=oss_bucket_region,
    oss_access_key_id=oss_access_key_id,
    oss_access_key_secret=oss_access_key_secret,
    oss_security_token=oss_security_token,
    oss_object_prefix=oss_object_prefix,
    oss_public_base_url=oss_public_base_url,
)
_sync_field_reference_doc_once()

if trigger_oss_test:
    if not active_media_strategy.use_oss_media() or active_media_strategy.oss_config is None:
        st.sidebar.warning("请先完整填写 OSS Endpoint、Bucket、AccessKey 等配置后再测试连接。")
    else:
        try:
            oss_info = OSSMediaStore(active_media_strategy.oss_config).test_connection()
        except Exception as exc:  # noqa: BLE001
            st.sidebar.error(f"OSS 连接测试失败：{exc}")
        else:
            st.sidebar.success("OSS 连接成功。")
            st.sidebar.json(oss_info)

store = SQLiteCrawlerStore(db_path)
field_reference_df = _get_field_reference_df()

main_col, reference_col = st.columns([3.4, 1.6], gap="large")

with reference_col:
    st.markdown("<div id='field-reference-sticky-anchor'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        _render_field_reference_panel(field_reference_df)

with main_col:
    tab_single, tab_batch, tab_interfaces, tab_db, tab_quick_jump = st.tabs(
        ["单 bvid 全流程", "CSV 批量抓取", "四类接口调试", "SQLite 数据查看", "快捷跳转"]
    )


def _show_json(title: str, payload: dict) -> None:
    st.write(title)
    st.json(payload)


def _show_meta_result(result) -> None:
    _show_json("MetaResult", result.to_dict())

    with st.expander("查看标签详情", expanded=False):
        st.json(result.tag_details)
    with st.expander("查看视频权限对象", expanded=False):
        st.json(result.rights)
    with st.expander("查看上传用户资料", expanded=False):
        st.json(result.uploader_profile)
    with st.expander("查看上传用户关系快照", expanded=False):
        st.json(result.uploader_relation)
    with st.expander("查看上传用户空间概览", expanded=False):
        st.json(result.uploader_overview)
    with st.expander("查看 Meta 原始聚合 payload", expanded=False):
        st.json(result.raw_payload)


with tab_single:
    st.subheader("单 bvid 全流程")
    col_a, col_b = st.columns([2, 1])
    with col_a:
        single_bvid = st.text_input("输入 bvid", value="")
    with col_b:
        enable_media_single = st.checkbox("抓取媒体", value=True)

    if st.button("开始单视频顺序抓取", width="stretch"):
        if not single_bvid.strip():
            st.warning("请先输入 bvid。")
        else:
            with st.spinner("正在顺序执行 Meta -> Stat -> Comment -> Media ..."):
                try:
                    summary = crawl_full_video_bundle(
                        single_bvid.strip(),
                        enable_media=enable_media_single,
                        comment_limit=int(comment_limit_default),
                        db_path=db_path,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"抓取失败：{exc}")
                else:
                    if summary.errors:
                        st.warning("流程已结束，但有部分阶段失败。")
                    else:
                        st.success("单视频全流程抓取成功。")
                    _show_json("流程摘要", summary.to_dict())


with tab_batch:
    st.subheader("CSV 批量抓取")
    uploaded = st.file_uploader("上传包含 `bvid` 列的 CSV 文件", type=["csv"], accept_multiple_files=False)
    parallelism = st.number_input("并发度", min_value=1, max_value=16, value=2, step=1)
    enable_media_batch = st.checkbox("批量任务抓取媒体", value=False)

    if uploaded is not None:
        try:
            preview_df = pd.read_csv(uploaded)
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取 CSV 失败：{exc}")
            preview_df = None
        else:
            st.caption(f"预览前 20 行，检测到列：{', '.join(preview_df.columns)}")
            st.dataframe(preview_df.head(20), width="stretch", hide_index=True)

    if st.button("开始批量抓取", width="stretch"):
        if uploaded is None:
            st.warning("请先上传 CSV 文件。")
        else:
            tmp_path = Path(db_path).parent / "_uploaded_bvid_batch.csv"
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(uploaded.getbuffer())

            with st.spinner("正在批量抓取，请等待..."):
                try:
                    report = crawl_bvid_list_from_csv(
                        tmp_path,
                        parallelism=int(parallelism),
                        enable_media=enable_media_batch,
                        comment_limit=int(comment_limit_default),
                        db_path=db_path,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"批量抓取失败：{exc}")
                else:
                    st.success("批量抓取已完成。")
                    _show_json("批量运行报告", report.to_dict())


with tab_interfaces:
    st.subheader("四类接口调试")
    meta_tab, stat_tab, comment_tab, media_tab = st.tabs(["Meta", "Stat", "Comment", "Media"])

    with meta_tab:
        meta_bvid = st.text_input("Meta: bvid", key="meta_bvid")
        if st.button("调用 Meta 接口"):
            try:
                result = crawl_video_meta(meta_bvid.strip(), db_path=db_path, credential=active_credential)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Meta 接口调用失败：{exc}")
            else:
                st.success("Meta 接口调用成功。")
                _show_meta_result(result)

    with stat_tab:
        stat_bvid = st.text_input("Stat: bvid", key="stat_bvid")
        if st.button("调用 Stat 接口"):
            try:
                result = crawl_stat_snapshot(stat_bvid.strip(), db_path=db_path, credential=active_credential)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Stat 接口调用失败：{exc}")
            else:
                st.success("Stat 接口调用成功。")
                _show_json("StatSnapshot", result.to_dict())

    with comment_tab:
        comment_bvid = st.text_input("Comment: bvid", key="comment_bvid")
        comment_limit = st.number_input("Comment: limit", min_value=1, max_value=100, value=int(comment_limit_default), step=1)
        if st.button("调用 Comment 接口"):
            try:
                result = crawl_latest_comments(
                    comment_bvid.strip(),
                    limit=int(comment_limit),
                    db_path=db_path,
                    credential=active_credential,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Comment 接口调用失败：{exc}")
            else:
                st.success("Comment 接口调用成功。")
                _show_json("CommentSnapshot", result.to_dict())

    with media_tab:
        media_bvid = st.text_input("Media: bvid", key="media_bvid")
        use_streaming_api = st.checkbox("使用 stream_media_to_store", value=True)
        if st.button("调用 Media 接口"):
            try:
                if use_streaming_api:
                    result = stream_media_to_store(
                        media_bvid.strip(),
                        strategy=active_media_strategy,
                        credential=active_credential,
                    )
                else:
                    result = crawl_media_assets(
                        media_bvid.strip(),
                        strategy=active_media_strategy,
                        db_path=db_path,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        credential=active_credential,
                    )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Media 接口调用失败：{exc}")
            else:
                st.success("Media 接口调用成功。")
                _show_json("MediaResult", result.to_dict())


with tab_db:
    st.subheader("SQLite 数据查看")
    st.caption("结构化数据、评论快照、统计快照与运行记录始终保存在 SQLite；若媒体选择 OSS，SQLite 中保存的是对象元数据与定位信息。")
    inspect_bvid = st.text_input("输入要查看的 bvid", value="")
    db_view_col, asset_view_col = st.columns(2)

    if db_view_col.button("查看该 bvid 的结构化数据", width="stretch"):
        target_bvid = inspect_bvid.strip()
        if not target_bvid:
            st.warning("请先输入 bvid。")
        else:
            try:
                video_row = store.fetch_video_row(target_bvid)
                stat_row = store.fetch_latest_stat_snapshot_row(target_bvid)
                comment_row = store.fetch_latest_comment_snapshot_row(target_bvid)
            except Exception as exc:  # noqa: BLE001
                st.error(f"读取结构化数据失败：{exc}")
            else:
                if video_row is None and stat_row is None and comment_row is None:
                    st.info("当前数据库中没有这个 bvid 的结构化记录。")
                else:
                    if video_row is not None:
                        with st.expander("视频元数据（videos 表）", expanded=True):
                            st.json(video_row)
                    if stat_row is not None:
                        with st.expander("最新统计快照（video_stat_snapshots 表）", expanded=False):
                            st.json(stat_row)
                    if comment_row is not None:
                        with st.expander("最新评论快照（topn_comment_snapshots 表）", expanded=False):
                            st.json(comment_row)

    if asset_view_col.button("查看该 bvid 的媒体资产", width="stretch"):
        try:
            target_bvid = inspect_bvid.strip()
            asset_rows = store.fetch_all_asset_rows(target_bvid)
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取数据库失败：{exc}")
        else:
            if not asset_rows:
                st.info("当前数据库中没有这个 bvid 的媒体资产记录。")
            else:
                st.dataframe(pd.DataFrame(asset_rows), width="stretch", hide_index=True)

                with st.expander("导出该 bvid 的媒体文件为本地文件"):
                    video_row = next((row for row in asset_rows if row.get("asset_type") == "video"), None)
                    audio_row = next((row for row in asset_rows if row.get("asset_type") == "audio"), None)

                    if not video_row and not audio_row:
                        st.info("当前 bvid 暂无可导出的媒体资产。")
                    else:
                        if video_row:
                            try:
                                video_bytes = _load_asset_bytes(
                                    store=store,
                                    asset_row=video_row,
                                    target_bvid=target_bvid,
                                    strategy=active_media_strategy,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.warning(f"视频文件导出失败：{exc}")
                            else:
                                if video_bytes:
                                    st.download_button(
                                        "下载视频文件（.m4s，按当前最高允许清晰度）",
                                        data=video_bytes,
                                        file_name=f"{target_bvid}_{video_row.get('cid') or 'na'}_video.m4s",
                                        mime="video/mp4",
                                    )
                        if audio_row:
                            try:
                                audio_bytes = _load_asset_bytes(
                                    store=store,
                                    asset_row=audio_row,
                                    target_bvid=target_bvid,
                                    strategy=active_media_strategy,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.warning(f"音频文件导出失败：{exc}")
                            else:
                                if audio_bytes:
                                    st.download_button(
                                        "下载音频文件（.m4s）",
                                        data=audio_bytes,
                                        file_name=f"{target_bvid}_{audio_row.get('cid') or 'na'}_audio.m4s",
                                        mime="audio/mp4",
                                    )

    st.caption("提示：当前界面显示的是 SQLite 中的媒体元数据记录；若媒体已存入 OSS，可在侧边栏填入同一 Bucket 的凭证后直接回读导出。")
    st.code(
        json.dumps(
            {
                "db_path": db_path,
                "chunk_size_mb": int(chunk_size_mb),
                "max_height": int(max_height),
                "storage_backend": active_media_strategy.storage_backend,
                "oss_config": active_media_strategy.oss_config.to_safe_dict() if active_media_strategy.oss_config else None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        language="json",
    )


with tab_quick_jump:
    st.subheader("快捷跳转")
    st.caption("输入单个 BVID 或作者 ID，点击后自动使用系统默认浏览器打开对应页面。")

    video_col, owner_col = st.columns(2)

    with video_col:
        quick_jump_bvid = st.text_input(
            "视频 BVID",
            value="",
            key="quick_jump_bvid",
            help="支持直接输入 BVID，也支持粘贴视频链接后自动提取。",
        )
        if st.button("跳转到视频页", key="quick_jump_bvid_submit", width="stretch"):
            normalized_bvid = normalize_bvid(quick_jump_bvid)
            if normalized_bvid is None:
                st.warning("请输入有效的 BVID。")
            else:
                video_url = build_video_url(normalized_bvid)
                if open_in_default_browser(video_url):
                    st.success(f"已打开视频页：{video_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")

    with owner_col:
        quick_jump_owner_mid = st.text_input(
            "作者 ID",
            value="",
            key="quick_jump_owner_mid",
            help="支持直接输入作者 ID，也支持粘贴作者主页链接后自动提取。",
        )
        if st.button("跳转到作者主页", key="quick_jump_owner_mid_submit", width="stretch"):
            normalized_owner_mid = normalize_owner_mid(quick_jump_owner_mid)
            if normalized_owner_mid is None:
                st.warning("请输入有效的作者 ID。")
            else:
                owner_url = build_owner_space_url(normalized_owner_mid)
                if open_in_default_browser(owner_url):
                    st.success(f"已打开作者主页：{owner_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")
