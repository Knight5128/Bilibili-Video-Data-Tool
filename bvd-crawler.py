from __future__ import annotations

import base64
import json
from datetime import datetime
from html import escape
from pathlib import Path

from bilibili_api import Credential
import pandas as pd
import streamlit as st

from bili_pipeline.crawl_api import (
    DEFAULT_VIDEO_DATA_OUTPUT_DIR,
    TEST_CRAWLS_OUTPUT_DIR,
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
from bili_pipeline.models import GCPStorageConfig, MediaDownloadStrategy
from bili_pipeline.storage import BigQueryCrawlerStore, GcsMediaStore
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.log_files import wrap_log_text
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvd-crawler.png"
FIELD_REFERENCE_DOC_PATH = APP_DIR / FIELD_REFERENCE_DOC_NAME
LOCAL_GCP_CONFIG_PATH = APP_DIR / ".local" / "bvd-crawler.gcp.config.json"
DEFAULT_GCP_CONFIG = {
    "gcp_project_id": "",
    "bigquery_dataset": "bili_video_data_crawler",
    "gcs_bucket_name": "",
    "gcp_region": "",
    "credentials_path": "",
    "gcs_object_prefix": "bilibili-media",
    "gcs_public_base_url": "",
}


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


def _load_persisted_gcp_config() -> dict[str, str]:
    config = DEFAULT_GCP_CONFIG.copy()
    if not LOCAL_GCP_CONFIG_PATH.exists():
        return config
    try:
        payload = json.loads(LOCAL_GCP_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return config
    if not isinstance(payload, dict):
        return config
    for key in DEFAULT_GCP_CONFIG:
        value = payload.get(key, config[key])
        config[key] = value if isinstance(value, str) else str(value)
    return config


def _save_persisted_gcp_config(config: dict[str, str]) -> Path:
    LOCAL_GCP_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_GCP_CONFIG_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return LOCAL_GCP_CONFIG_PATH


def _init_gcp_config_state() -> None:
    if st.session_state.get("gcp_config_state_initialized"):
        return
    persisted_config = _load_persisted_gcp_config()
    for key, default_value in persisted_config.items():
        st.session_state.setdefault(key, default_value)
    st.session_state["gcp_config_state_initialized"] = True


def _collect_gcp_config_from_state() -> dict[str, str]:
    return {
        key: str(st.session_state.get(key, DEFAULT_GCP_CONFIG[key])).strip()
        for key in DEFAULT_GCP_CONFIG
    }


def _build_media_strategy(
    *,
    max_height: int,
    chunk_size_mb: int,
    gcp_project_id: str,
    bigquery_dataset: str,
    gcs_bucket_name: str,
    gcp_region: str,
    credentials_path: str,
    gcs_object_prefix: str,
    gcs_public_base_url: str,
) -> MediaDownloadStrategy:
    gcp_config = GCPStorageConfig(
        project_id=gcp_project_id,
        bigquery_dataset=bigquery_dataset,
        gcs_bucket_name=gcs_bucket_name,
        gcp_region=gcp_region,
        credentials_path=credentials_path,
        object_prefix=gcs_object_prefix,
        public_base_url=gcs_public_base_url,
    )
    return MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        storage_backend="gcs",
        gcp_config=gcp_config,
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
    asset_row: dict,
    strategy: MediaDownloadStrategy,
) -> bytes:
    if not strategy.use_gcs_media() or strategy.gcp_config is None:
        raise ValueError("当前未启用可用的 Google Cloud Storage 配置，无法回读远程媒体文件。")

    active_bucket = strategy.gcp_config.bucket_name.strip()
    row_bucket = str(asset_row.get("bucket_name") or "").strip()
    if row_bucket and active_bucket and row_bucket != active_bucket:
        raise ValueError("当前 GCS Bucket 与该资产记录不一致，请切换为正确的 Bucket 后再导出。")

    active_endpoint = strategy.gcp_config.endpoint_with_scheme().rstrip("/")
    row_endpoint = str(asset_row.get("storage_endpoint") or "").strip().rstrip("/")
    if row_endpoint and active_endpoint and row_endpoint != active_endpoint:
        raise ValueError("当前 GCS Endpoint 与该资产记录不一致，请切换为正确的 Endpoint 后再导出。")

    return GcsMediaStore(strategy.gcp_config).download_object_bytes(str(asset_row.get("object_key") or ""))


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(APP_DIR).as_posix()
    except ValueError:
        return path.as_posix()


def _make_safe_log_prefix(prefix: str) -> str:
    safe_chars = []
    for ch in prefix.strip():
        safe_chars.append(ch if ch.isalnum() or ch in {"_", "-", "."} else "_")
    return "".join(safe_chars).strip("_") or "task"


def _save_text_log(
    log_dir: Path,
    prefix: str,
    content: str,
    finished_at: datetime,
    *,
    started_at: datetime | None = None,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{_make_safe_log_prefix(prefix)}_{finished_at.strftime('%Y%m%d_%H%M%S')}.log"
    log_path.write_text(
        wrap_log_text(
            content,
            started_at=started_at or finished_at,
            finished_at=finished_at,
        ),
        encoding="utf-8",
    )
    return log_path


def _build_single_crawl_log_dir(started_at: datetime) -> Path:
    return TEST_CRAWLS_OUTPUT_DIR / f"single_bvid_{started_at.strftime('%Y%m%d_%H%M%S')}"


def _build_api_debug_log_dir(api_name: str, started_at: datetime) -> Path:
    return TEST_CRAWLS_OUTPUT_DIR / f"{api_name}_api_{started_at.strftime('%Y%m%d_%H%M%S')}"


def _build_media_uri_lines(media_result) -> list[str]:
    if media_result is None:
        return []
    lines: list[str] = []
    if media_result.video_asset and media_result.video_asset.bucket_name and media_result.video_asset.object_key:
        lines.append(f"video_gcs_uri: gs://{media_result.video_asset.bucket_name}/{media_result.video_asset.object_key}")
    if media_result.audio_asset and media_result.audio_asset.bucket_name and media_result.audio_asset.object_key:
        lines.append(f"audio_gcs_uri: gs://{media_result.audio_asset.bucket_name}/{media_result.audio_asset.object_key}")
    return lines


def _build_single_crawl_log_content(
    *,
    bvid: str,
    started_at: datetime,
    finished_at: datetime,
    enable_media: bool,
    comment_limit: int,
    summary=None,
    error: Exception | None = None,
) -> str:
    lines = [
        "Bilibili Video Data Crawler - Single BVID Crawl",
        f"bvid: {bvid}",
        f"started_at: {started_at.isoformat()}",
        f"finished_at: {finished_at.isoformat()}",
        f"enable_media: {enable_media}",
        f"comment_limit: {comment_limit}",
    ]
    if error is not None:
        lines.extend(
            [
                "status: failed",
                f"error: {error}",
            ]
        )
        return "\n".join(lines)
    lines.append("status: success" if summary is not None and not summary.errors else "status: partial_success")
    if summary is not None:
        lines.extend(_build_media_uri_lines(summary.media_result))
        lines.append("")
        lines.append("summary_json:")
        lines.append(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
    return "\n".join(lines)


def _build_api_log_content(
    *,
    api_name: str,
    bvid: str,
    started_at: datetime,
    finished_at: datetime,
    payload: dict | None = None,
    media_result=None,
    error: Exception | None = None,
    extra_lines: list[str] | None = None,
) -> str:
    lines = [
        f"Bilibili Video Data Crawler - {api_name.upper()} API Debug",
        f"api_name: {api_name}",
        f"bvid: {bvid}",
        f"started_at: {started_at.isoformat()}",
        f"finished_at: {finished_at.isoformat()}",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    if error is not None:
        lines.extend(
            [
                "status: failed",
                f"error: {error}",
            ]
        )
        return "\n".join(lines)
    lines.append("status: success")
    lines.extend(_build_media_uri_lines(media_result))
    if payload is not None:
        lines.append("")
        lines.append("payload_json:")
        lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
    return "\n".join(lines)


page_config = {"page_title": "Bilibili Video Data Crawler", "layout": "wide"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
_inject_field_reference_panel_styles()
render_night_sky_background()
_render_centered_header("Bilibili Video Data Crawler", LOGO_PATH)
_init_gcp_config_state()
st.markdown(
    "<p style='text-align: center; margin-bottom: 1rem;'>基于 bilibili-api 的B站视频数据（元数据/动态互动/评论/多媒体）采集工具，当前采用 BigQuery 存储结构化结果，并使用 Google Cloud Storage 沉淀视频/音频媒体文件，便于后续在 Google Colab 中直接加载与训练。</p>",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.subheader("全局设置")
    comment_limit_default = st.number_input("默认评论条数", min_value=1, max_value=100, value=10, step=1)
    chunk_size_mb = st.number_input("媒体分块大小（MB）", min_value=1, max_value=64, value=4, step=1)
    max_height = st.number_input("媒体最大分辨率（高度）", min_value=360, max_value=2160, value=1080, step=120)
    consecutive_failure_limit = st.number_input(
        "CSV 批量抓取连续失败暂停阈值",
        min_value=1,
        max_value=100,
        value=10,
        step=1,
        help="当 CSV 批量抓取中连续有这么多条视频抓取失败时，程序会暂停本轮任务，并导出剔除已成功条目的 remaining CSV。",
    )
    cookie_text = st.text_area(
        "可选：B 站 Cookie（提升评论与媒体抓取稳定性，仅在本地内存中使用）",
        value="",
        height=80,
        help="建议只粘贴包含 SESSDATA、bili_jct、buvid3 的子串，例如：SESSDATA=...; bili_jct=...; buvid3=...",
    )
    st.caption("B 站 Cookie 与 Google 凭证路径仅保存在当前 Streamlit 会话内存中，不会写入 BigQuery。")
    st.caption("当前版本固定采用 BigQuery + Google Cloud Storage，不再维护本地 SQLite / 阿里云 OSS 双存储。")
    st.caption(f"默认导出文件根目录：`{DEFAULT_VIDEO_DATA_OUTPUT_DIR.as_posix()}`")

    st.divider()
    st.subheader("Google Cloud 配置")
    gcp_project_id = st.text_input("GCP Project ID", key="gcp_project_id", help="例如 your-project-id；若本机已配置默认项目，也可留空让客户端自动推断。")
    bigquery_dataset = st.text_input("BigQuery Dataset", key="bigquery_dataset", help="用于存放结构化表，如 videos、video_stat_snapshots、assets。")
    gcs_bucket_name = st.text_input("GCS Bucket 名称", key="gcs_bucket_name", help="用于存放视频/音频媒体文件。")
    gcp_region = st.text_input("GCP Region（可选）", key="gcp_region", help="例如 asia-east1、us-central1。新建 BigQuery Dataset 时会使用该区域。")
    credentials_path = st.text_input(
        "服务账号 JSON 路径（可选）",
        key="credentials_path",
        help="若留空，则使用 Application Default Credentials（ADC）。填写时请提供本机可访问的服务账号 JSON 文件路径。",
    )
    gcs_object_prefix = st.text_input("GCS 对象前缀", key="gcs_object_prefix", help="上传到 GCS 的对象 key 前缀。")
    gcs_public_base_url = st.text_input(
        "公共访问基础 URL（可选）",
        key="gcs_public_base_url",
        help="若 Bucket 配置了 CDN 或公开域名，可填入形如 https://storage.googleapis.com/your-bucket 的基础地址。",
    )
    trigger_save_config = st.button("保存配置", width="stretch")
    trigger_gcp_test = st.button("测试 GCP 连接", width="stretch")

active_credential = _build_credential_from_cookie(cookie_text)
active_media_strategy = _build_media_strategy(
    max_height=int(max_height),
    chunk_size_mb=int(chunk_size_mb),
    gcp_project_id=gcp_project_id,
    bigquery_dataset=bigquery_dataset,
    gcs_bucket_name=gcs_bucket_name,
    gcp_region=gcp_region,
    credentials_path=credentials_path,
    gcs_object_prefix=gcs_object_prefix,
    gcs_public_base_url=gcs_public_base_url,
)
_sync_field_reference_doc_once()

if trigger_save_config:
    try:
        saved_path = _save_persisted_gcp_config(_collect_gcp_config_from_state())
    except Exception as exc:  # noqa: BLE001
        st.sidebar.error(f"保存配置失败：{exc}")
    else:
        st.sidebar.success(f"配置已保存到本地：{saved_path}")

if trigger_gcp_test:
    if not active_media_strategy.use_gcs_media() or active_media_strategy.gcp_config is None:
        st.sidebar.warning("请先完整填写 BigQuery Dataset 与 GCS Bucket；若未配置 ADC，也请提供服务账号 JSON 路径。")
    else:
        try:
            bq_info = BigQueryCrawlerStore(active_media_strategy.gcp_config).test_connection()
            gcs_info = GcsMediaStore(active_media_strategy.gcp_config).test_connection()
        except Exception as exc:  # noqa: BLE001
            st.sidebar.error(f"GCP 连接测试失败：{exc}")
        else:
            st.sidebar.success("GCP 连接成功。")
            st.sidebar.json({"bigquery": bq_info, "gcs": gcs_info})

store = None
store_init_error = None
if active_media_strategy.gcp_config is not None and active_media_strategy.gcp_config.is_enabled():
    try:
        store = BigQueryCrawlerStore(active_media_strategy.gcp_config)
    except Exception as exc:  # noqa: BLE001
        store_init_error = str(exc)
field_reference_df = _get_field_reference_df()

main_col, reference_col = st.columns([3.4, 1.6], gap="large")

with reference_col:
    st.markdown("<div id='field-reference-sticky-anchor'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        _render_field_reference_panel(field_reference_df)

with main_col:
    tab_single, tab_batch, tab_interfaces, tab_db, tab_quick_jump = st.tabs(
        ["单 bvid 全流程", "CSV 批量抓取", "四类接口调试", "BigQuery / GCS 数据查看", "快捷跳转"]
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


def _gcp_ready() -> bool:
    return store is not None and store_init_error is None


with tab_single:
    st.subheader("单 bvid 全流程")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
    col_a, col_b = st.columns([2, 1])
    with col_a:
        single_bvid = st.text_input("输入 bvid", value="")
    with col_b:
        enable_media_single = st.checkbox("抓取媒体", value=True)

    if st.button("开始单视频顺序抓取", width="stretch"):
        if not _gcp_ready():
            st.warning("请先在侧边栏完成 Google Cloud 配置并确保连接可用。")
        elif not single_bvid.strip():
            st.warning("请先输入 bvid。")
        else:
            started_at = datetime.now()
            target_bvid = single_bvid.strip()
            log_dir = _build_single_crawl_log_dir(started_at)
            with st.spinner("正在顺序执行 Meta -> Stat -> Comment -> Media ..."):
                try:
                    summary = crawl_full_video_bundle(
                        target_bvid,
                        enable_media=enable_media_single,
                        comment_limit=int(comment_limit_default),
                        gcp_config=active_media_strategy.gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "single_bvid_crawl",
                        _build_single_crawl_log_content(
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            enable_media=enable_media_single,
                            comment_limit=int(comment_limit_default),
                            error=exc,
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.error(f"抓取失败：{exc}")
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")
                else:
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "single_bvid_crawl",
                        _build_single_crawl_log_content(
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            enable_media=enable_media_single,
                            comment_limit=int(comment_limit_default),
                            summary=summary,
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    if summary.errors:
                        st.warning("流程已结束，但有部分阶段失败。")
                    else:
                        st.success("单视频全流程抓取成功。")
                    _show_json("流程摘要", summary.to_dict())
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")


with tab_batch:
    st.subheader("CSV 批量抓取")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
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
        if not _gcp_ready():
            st.warning("请先在侧边栏完成 Google Cloud 配置并确保连接可用。")
        elif uploaded is None:
            st.warning("请先上传 CSV 文件。")
        else:
            upload_cache_dir = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "_uploaded_batches"
            upload_cache_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = upload_cache_dir / f"batch_input_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(uploaded.getbuffer())

            with st.spinner("正在批量抓取，请等待..."):
                try:
                    report = crawl_bvid_list_from_csv(
                        tmp_path,
                        parallelism=int(parallelism),
                        enable_media=enable_media_batch,
                        comment_limit=int(comment_limit_default),
                        consecutive_failure_limit=int(consecutive_failure_limit),
                        gcp_config=active_media_strategy.gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                        output_root_dir=DEFAULT_VIDEO_DATA_OUTPUT_DIR,
                        source_csv_name=uploaded.name,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"批量抓取失败：{exc}")
                else:
                    if report.completed_all:
                        st.success("批量抓取已完成，所有视频均已成功抓取。")
                    elif report.stopped_due_to_consecutive_failures:
                        st.warning("批量抓取已按连续失败阈值暂停，并导出了剔除成功条目的 remaining CSV。")
                    else:
                        st.warning("批量抓取已结束，但仍有未成功视频，已导出 remaining CSV 供继续执行。")
                    _show_json("批量运行报告", report.to_dict())
                    st.caption(f"本次 batch_crawl 目录：`{_display_path(Path(report.session_dir))}`")
                    if report.task_log_path:
                        st.caption(f"子任务日志：`{_display_path(Path(report.task_log_path))}`")
                    if report.remaining_csv_path:
                        st.caption(f"剩余 bvid CSV：`{_display_path(Path(report.remaining_csv_path))}`")
                    if report.session_summary_log_path:
                        st.caption(f"最终汇总日志：`{_display_path(Path(report.session_summary_log_path))}`")


with tab_interfaces:
    st.subheader("四类接口调试")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
    meta_tab, stat_tab, comment_tab, media_tab = st.tabs(["Meta", "Stat", "Comment", "Media"])

    with meta_tab:
        meta_bvid = st.text_input("Meta: bvid", key="meta_bvid")
        if st.button("调用 Meta 接口"):
            if not _gcp_ready():
                st.warning("请先配置可用的 BigQuery / GCS 连接。")
            else:
                started_at = datetime.now()
                target_bvid = meta_bvid.strip()
                log_dir = _build_api_debug_log_dir("meta", started_at)
                try:
                    result = crawl_video_meta(target_bvid, gcp_config=active_media_strategy.gcp_config, credential=active_credential)
                except Exception as exc:  # noqa: BLE001
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "meta_api",
                        _build_api_log_content(
                            api_name="meta",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            error=exc,
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.error(f"Meta 接口调用失败：{exc}")
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")
                else:
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "meta_api",
                        _build_api_log_content(
                            api_name="meta",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            payload=result.to_dict(),
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.success("Meta 接口调用成功。")
                    _show_meta_result(result)
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")

    with stat_tab:
        stat_bvid = st.text_input("Stat: bvid", key="stat_bvid")
        if st.button("调用 Stat 接口"):
            if not _gcp_ready():
                st.warning("请先配置可用的 BigQuery / GCS 连接。")
            else:
                started_at = datetime.now()
                target_bvid = stat_bvid.strip()
                log_dir = _build_api_debug_log_dir("stat", started_at)
                try:
                    result = crawl_stat_snapshot(target_bvid, gcp_config=active_media_strategy.gcp_config, credential=active_credential)
                except Exception as exc:  # noqa: BLE001
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "stat_api",
                        _build_api_log_content(
                            api_name="stat",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            error=exc,
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.error(f"Stat 接口调用失败：{exc}")
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")
                else:
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "stat_api",
                        _build_api_log_content(
                            api_name="stat",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            payload=result.to_dict(),
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.success("Stat 接口调用成功。")
                    _show_json("StatSnapshot", result.to_dict())
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")

    with comment_tab:
        comment_bvid = st.text_input("Comment: bvid", key="comment_bvid")
        comment_limit = st.number_input("Comment: limit", min_value=1, max_value=100, value=int(comment_limit_default), step=1)
        if st.button("调用 Comment 接口"):
            if not _gcp_ready():
                st.warning("请先配置可用的 BigQuery / GCS 连接。")
            else:
                started_at = datetime.now()
                target_bvid = comment_bvid.strip()
                log_dir = _build_api_debug_log_dir("comment", started_at)
                try:
                    result = crawl_latest_comments(
                        target_bvid,
                        limit=int(comment_limit),
                        gcp_config=active_media_strategy.gcp_config,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "comment_api",
                        _build_api_log_content(
                            api_name="comment",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            error=exc,
                            extra_lines=[f"comment_limit: {int(comment_limit)}"],
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.error(f"Comment 接口调用失败：{exc}")
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")
                else:
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "comment_api",
                        _build_api_log_content(
                            api_name="comment",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            payload=result.to_dict(),
                            extra_lines=[f"comment_limit: {int(comment_limit)}"],
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.success("Comment 接口调用成功。")
                    _show_json("CommentSnapshot", result.to_dict())
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")

    with media_tab:
        media_bvid = st.text_input("Media: bvid", key="media_bvid")
        use_streaming_api = st.checkbox("使用 stream_media_to_store", value=True)
        if st.button("调用 Media 接口"):
            if not _gcp_ready():
                st.warning("请先配置可用的 BigQuery / GCS 连接。")
            else:
                started_at = datetime.now()
                target_bvid = media_bvid.strip()
                log_dir = _build_api_debug_log_dir("media", started_at)
                try:
                    if use_streaming_api:
                        result = stream_media_to_store(
                            target_bvid,
                            strategy=active_media_strategy,
                            credential=active_credential,
                        )
                    else:
                        result = crawl_media_assets(
                            target_bvid,
                            strategy=active_media_strategy,
                            gcp_config=active_media_strategy.gcp_config,
                            max_height=int(max_height),
                            chunk_size_mb=int(chunk_size_mb),
                            credential=active_credential,
                        )
                except Exception as exc:  # noqa: BLE001
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "media_api",
                        _build_api_log_content(
                            api_name="media",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            error=exc,
                            extra_lines=[f"use_streaming_api: {use_streaming_api}"],
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.error(f"Media 接口调用失败：{exc}")
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")
                else:
                    finished_at = datetime.now()
                    log_path = _save_text_log(
                        log_dir,
                        "media_api",
                        _build_api_log_content(
                            api_name="media",
                            bvid=target_bvid,
                            started_at=started_at,
                            finished_at=finished_at,
                            payload=result.to_dict(),
                            media_result=result,
                            extra_lines=[f"use_streaming_api: {use_streaming_api}"],
                        ),
                        finished_at,
                        started_at=started_at,
                    )
                    st.success("Media 接口调用成功。")
                    _show_json("MediaResult", result.to_dict())
                    st.caption(f"日志已保存：`{_display_path(log_path)}`")


with tab_db:
    st.subheader("BigQuery / GCS 数据查看")
    st.caption("结构化数据、评论快照、统计快照与运行记录保存在 BigQuery；媒体资产记录中的对象键指向 GCS 中的真实视频/音频文件。")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
    inspect_bvid = st.text_input("输入要查看的 bvid", value="")
    db_view_col, asset_view_col = st.columns(2)

    if db_view_col.button("查看该 bvid 的结构化数据", width="stretch"):
        target_bvid = inspect_bvid.strip()
        if not _gcp_ready():
            st.warning("请先完成 Google Cloud 配置并建立连接。")
        elif not target_bvid:
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
                        with st.expander("最热评论快照（topn_comment_snapshots 表）", expanded=False):
                            st.json(comment_row)

    if asset_view_col.button("查看该 bvid 的媒体资产", width="stretch"):
        target_bvid = inspect_bvid.strip()
        if not _gcp_ready():
            st.warning("请先完成 Google Cloud 配置并建立连接。")
        elif not target_bvid:
            st.warning("请先输入 bvid。")
        else:
            try:
                asset_rows = store.fetch_all_asset_rows(target_bvid)
            except Exception as exc:  # noqa: BLE001
                st.error(f"读取 BigQuery 失败：{exc}")
            else:
                if not asset_rows:
                    st.info("当前 BigQuery 中没有这个 bvid 的媒体资产记录。")
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
                                        asset_row=video_row,
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
                                        asset_row=audio_row,
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

    st.caption("提示：当前界面展示的是 BigQuery 中的媒体元数据记录；实际视频/音频对象存放在 GCS，可直接按当前配置回读导出。")
    st.code(
        json.dumps(
            {
                "chunk_size_mb": int(chunk_size_mb),
                "max_height": int(max_height),
                "storage_backend": active_media_strategy.storage_backend,
                "gcp_config": active_media_strategy.gcp_config.to_safe_dict() if active_media_strategy.gcp_config else None,
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
