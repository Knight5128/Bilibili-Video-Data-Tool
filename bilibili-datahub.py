from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from bili_pipeline.cloud_tracker.admin import parse_owner_mid_upload
from bili_pipeline.crawl_api import (
    DEFAULT_VIDEO_DATA_OUTPUT_DIR,
    TEST_CRAWLS_OUTPUT_DIR,
    crawl_full_video_bundle,
    crawl_latest_comments,
    crawl_media_assets,
    crawl_stat_snapshot,
    crawl_video_meta,
    stream_media_to_store,
)
from bili_pipeline.datahub.config import (
    DEFAULT_AUTO_CONFIG,
    DEFAULT_GCP_CONFIG,
    LOCAL_AUTO_CONFIG_PATH,
    LOCAL_GCP_CONFIG_PATH,
    build_gcp_config,
)
from bili_pipeline.datahub.author_refinement import (
    AUTHOR_CATEGORY_OPTIONS,
    AUTHOR_REFINEMENT_ACCUMULATED_FILENAME,
    AUTHOR_REFINEMENT_BASE_CHART_FILENAME,
    AUTHOR_REFINEMENT_BIN_SUMMARY_FILENAME,
    AUTHOR_REFINEMENT_COMPARE_CHART_FILENAME,
    AUTHOR_REFINEMENT_COMPLETE_FILENAME,
    AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME,
    AUTHOR_REFINEMENT_REFINED_FULL_FILENAME,
    AUTHOR_REFINEMENT_REFINED_SAMPLED_FILENAME,
    AUTHOR_REFINEMENTS_OUTPUT_DIR,
    build_followup_author_refinement_session,
    build_author_distribution_figure,
    build_session_expanded_author_list,
    build_refinement_comparison_figure,
    crawl_author_metadata_with_guardrails,
    parse_priority_retention_rules,
    prepare_author_refinement_session,
    record_author_refinement_part,
    write_author_refinement_summary,
    save_remaining_author_csv,
    sample_authors_with_priority_rules,
)
from bili_pipeline.datahub.background_tasks import (
    DEFAULT_BACKGROUND_TASKS_ROOT,
    DEFAULT_COOKIE_PATH,
    background_task_is_running,
    create_background_task_dir,
    launch_background_worker,
    load_cookie_text,
    load_registered_background_task_status,
    register_active_background_task,
    save_cookie_text,
    update_background_task_status,
    write_background_task_config,
)
from bili_pipeline.datahub.discover_ops import (
    BVID_TO_UIDS_OUTPUT_DIR,
    DEFAULT_UID_EXPANSION_START_DATE,
    DEFAULT_VIDEO_POOL_OUTPUT_DIR,
    FULL_SITE_FLOORINGS_LOGS_DIR,
    CustomSeedSelection,
    build_custom_seed_result,
    build_owner_since_overrides,
    build_rankboard_result,
    build_result_from_owner_mids_with_guardrails,
    coerce_end_datetime,
    coerce_start_datetime,
    decode_uploaded_text,
    display_path,
    drop_existing_uid_expansion_duplicates,
    extract_bvids,
    extract_failed_owner_mids_from_text,
    extract_owner_mids,
    extract_owner_mids_from_rows,
    load_full_export_tids,
    load_rankboard_boards,
    normalize_output_root,
    owner_mid_dataframe,
    prepare_uid_expansion_session,
    record_uid_expansion_part,
    save_owner_mid_csv,
    summarize_exception,
    write_uid_expansion_summary,
)
from bili_pipeline.datahub.local_cycle_runner import DataHubLocalCycleRunner
from bili_pipeline.datahub.manual_batch_runner import (
    MANUAL_CRAWLS_OUTPUT_DIR,
    run_manual_realtime_batch_crawl,
)
from bili_pipeline.datahub.manual_media_runner import (
    MANUAL_MEDIA_CRAWLS_OUTPUT_DIR,
    build_manual_media_waitlist_path,
    run_manual_media_mode_a,
    run_manual_media_mode_b,
    sync_manual_media_waitlist,
)
from bili_pipeline.datahub.shared import (
    append_live_log,
    build_credential_from_cookie,
    load_json_config,
    persist_live_log_snapshot,
    render_centered_header,
    save_json_config,
    save_timestamped_task_log,
)
from bili_pipeline.discover import resolve_owner_mids_from_bvids
from bili_pipeline.discover.export_csv import (
    discover_entries_to_full_export_rows,
    discover_entries_to_rows,
    export_discover_result_csv,
    export_rows_csv,
    rankboard_entries_to_rows,
)
from bili_pipeline.field_reference import (
    FIELD_REFERENCE_DOC_NAME,
    SECTION_TITLES,
    build_field_reference_rows,
    sync_field_reference_markdown,
)
from bili_pipeline.models import CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy
from bili_pipeline.storage import BigQueryCrawlerStore, GcsMediaStore
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.file_merge import (
    build_deduplicated_output_path,
    deduplicate_dataframe,
    export_dataframe,
    merge_dataframes,
    parse_comma_separated_keys,
    read_uploaded_dataframe,
    resolve_output_path,
    validate_columns,
)
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvd-crawler.png"
FIELD_REFERENCE_DOC_PATH = APP_DIR / FIELD_REFERENCE_DOC_NAME
COOKIE_FILE_PATH = (APP_DIR / DEFAULT_COOKIE_PATH).resolve()
BACKGROUND_TASKS_ROOT = (APP_DIR / DEFAULT_BACKGROUND_TASKS_ROOT).resolve()


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


def _sync_field_reference_doc_once() -> None:
    if st.session_state.get("field_reference_doc_synced"):
        return
    sync_field_reference_markdown(FIELD_REFERENCE_DOC_PATH)
    st.session_state["field_reference_doc_synced"] = True


@st.cache_data(show_spinner=False)
def _get_field_reference_df() -> pd.DataFrame:
    return pd.DataFrame(build_field_reference_rows())


def _render_field_reference_panel(field_reference_df: pd.DataFrame) -> None:
    st.subheader("字段定义速查")
    st.caption("启动后常驻右侧，调试接口时可随时展开对照。")
    with st.expander("展开 / 收起字段定义", expanded=True):
        selected_section = st.selectbox(
            "选择要查看的数据模块",
            options=["全部"] + list(SECTION_TITLES.keys()),
            format_func=lambda item: "全部模块" if item == "全部" else SECTION_TITLES[item],
        )
        active_df = field_reference_df if selected_section == "全部" else field_reference_df[field_reference_df["数据模块"] == selected_section]
        st.dataframe(active_df, width="stretch", hide_index=True, height=360)


def _build_media_strategy(
    *,
    max_height: int,
    chunk_size_mb: int,
    gcp_config: GCPStorageConfig,
) -> MediaDownloadStrategy:
    return MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        storage_backend="gcs",
        gcp_config=gcp_config,
    )


def _load_asset_bytes(*, asset_row: dict, strategy: MediaDownloadStrategy) -> bytes:
    if not strategy.use_gcs_media() or strategy.gcp_config is None:
        raise ValueError("当前未启用可用的 Google Cloud Storage 配置，无法回读远程媒体文件。")
    return GcsMediaStore(strategy.gcp_config).download_object_bytes(str(asset_row.get("object_key") or ""))


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


def _read_uploaded_files(uploaded_files) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    for uploaded_file in uploaded_files:
        dfs.append(read_uploaded_dataframe(uploaded_file))
    return dfs


def _merge_and_deduplicate_by_column(dfs: list[pd.DataFrame], column_name: str) -> pd.DataFrame:
    for df in dfs:
        validate_columns(df, [column_name], column_name)
    merged = pd.concat(dfs, ignore_index=True)
    deduplicated, _ = deduplicate_dataframe(merged, dedupe_keys=[column_name])
    return deduplicated.reset_index(drop=True)


def _render_author_refinement_visualization_panel(
    *,
    expanded_df: pd.DataFrame,
    key_prefix: str,
    default_ratio: float,
    default_seed: int,
    default_log_x: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with st.form(f"{key_prefix}_visual_form"):
        ratio = st.slider(
            "精简保留比例",
            min_value=0.0,
            max_value=1.0,
            value=float(default_ratio),
            step=0.01,
            key=f"{key_prefix}_ratio",
        )
        random_seed = st.number_input(
            "精简随机种子",
            min_value=0,
            max_value=999999,
            value=int(default_seed),
            step=1,
            key=f"{key_prefix}_seed",
        )
        use_log_x = st.checkbox(
            "图表横轴使用对数刻度",
            value=bool(default_log_x),
            key=f"{key_prefix}_log_x",
        )
        selected_overlay_columns = st.multiselect(
            "叠加展示的类别属性",
            options=[item.column for item in AUTHOR_CATEGORY_OPTIONS],
            default=[],
            format_func=lambda item: {option.column: option.label for option in AUTHOR_CATEGORY_OPTIONS}.get(item, item),
            key=f"{key_prefix}_overlay_columns",
        )
        enable_priority_rules = st.checkbox(
            "启用高粉作者分区间保留比例",
            value=False,
            key=f"{key_prefix}_enable_priority_rules",
        )
        priority_rules_text = st.text_area(
            "高粉作者分区间规则（每行：最低粉丝数,保留比例）",
            value="5000000,1.0\n2000000,0.8\n1000000,0.5",
            height=90,
            key=f"{key_prefix}_priority_rules_text",
        )
        st.form_submit_button("应用可视化参数")

    priority_rules = []
    if enable_priority_rules:
        try:
            priority_rules = parse_priority_retention_rules(priority_rules_text)
        except ValueError as exc:
            st.error(f"高粉作者分区间规则格式错误：{exc}")
            priority_rules = []
    refined_full_df, sampled_df, bin_summary_df, priority_summary_df = sample_authors_with_priority_rules(
        expanded_df,
        sample_ratio=float(ratio),
        priority_rules=priority_rules,
        random_state=int(random_seed),
    )

    metric_col_a, metric_col_b, metric_col_c, metric_col_d = st.columns(4)
    metric_col_a.metric("扩充后作者数", len(expanded_df))
    metric_col_b.metric("目标保留数", int(len(expanded_df) * float(ratio)))
    metric_col_c.metric("实际保留数", len(sampled_df))
    metric_col_d.metric("高粉规则保留数", int(priority_summary_df["保留作者数"].sum()) if not priority_summary_df.empty else 0)

    category_label_lookup = {item.column: item.label for item in AUTHOR_CATEGORY_OPTIONS}
    distribution_figure, mapping_summary = build_author_distribution_figure(
        expanded_df,
        selected_category_columns=selected_overlay_columns,
        use_log_x=use_log_x,
    )
    st.plotly_chart(distribution_figure, use_container_width=True)

    if selected_overlay_columns:
        mapping_rows: list[dict[str, object]] = []
        for column_name in selected_overlay_columns:
            for code, raw_value in mapping_summary.get(column_name, {}).items():
                mapping_rows.append(
                    {
                        "属性": category_label_lookup.get(column_name, column_name),
                        "编码值": code,
                        "原始取值": raw_value,
                    }
                )
        if mapping_rows:
            st.dataframe(pd.DataFrame(mapping_rows), width="stretch", hide_index=True)

    if not priority_summary_df.empty:
        st.caption("高粉作者分区间保留结果")
        st.dataframe(priority_summary_df, width="stretch", hide_index=True)

    st.plotly_chart(
        build_refinement_comparison_figure(expanded_df, sampled_df, use_log_x=use_log_x),
        use_container_width=True,
    )
    st.dataframe(bin_summary_df, width="stretch", hide_index=True)
    return refined_full_df, sampled_df, bin_summary_df


def _load_saved_dataframe(path_str: str) -> pd.DataFrame | None:
    raw = (path_str or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.exists():
        return None
    try:
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path)
        return pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return None


def _load_gcp_form_defaults() -> dict[str, str]:
    return load_json_config(LOCAL_GCP_CONFIG_PATH, DEFAULT_GCP_CONFIG)


def _load_auto_form_defaults() -> dict[str, object]:
    return load_json_config(LOCAL_AUTO_CONFIG_PATH, DEFAULT_AUTO_CONFIG)


def _serialize_cache_payload(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@st.cache_resource(show_spinner=False)
def _build_store_cached(gcp_payload_json: str) -> BigQueryCrawlerStore:
    return BigQueryCrawlerStore(build_gcp_config(json.loads(gcp_payload_json)))


@st.cache_resource(show_spinner=False)
def _build_local_runner_cached(
    gcp_payload_json: str,
    auto_payload_json: str,
    cookie_text: str,
) -> DataHubLocalCycleRunner:
    return DataHubLocalCycleRunner(
        gcp_config=build_gcp_config(json.loads(gcp_payload_json)),
        auto_config=json.loads(auto_payload_json),
        credential=build_credential_from_cookie(cookie_text),
    )


def _manual_stat_comment_video_pool_root() -> Path:
    return Path(DEFAULT_VIDEO_POOL_OUTPUT_DIR).resolve()


def _manual_stat_comment_crawls_root() -> Path:
    return Path(MANUAL_CRAWLS_OUTPUT_DIR).resolve()


def _list_uid_expansion_task_dirs(video_pool_root: Path) -> list[Path]:
    uid_root = video_pool_root / "uid_expansions"
    if not uid_root.exists():
        return []
    return sorted(
        [path for path in uid_root.iterdir() if path.is_dir()],
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )


def _list_manual_batch_flooring_csvs(video_pool_root: Path) -> list[Path]:
    flooring_dir = video_pool_root / "full_site_floorings"
    if not flooring_dir.exists():
        return []
    candidates = [
        path
        for path in flooring_dir.glob("*.csv")
        if path.is_file() and not path.name.endswith("_authors.csv")
    ]
    return sorted(
        candidates,
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )


def _build_default_log_dir(prefix: str, started_at: datetime) -> Path:
    return TEST_CRAWLS_OUTPUT_DIR / f"{prefix}_{started_at.strftime('%Y%m%d_%H%M%S')}"


def _load_registered_background_task(scope: str) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    return load_registered_background_task_status(scope, registry_root=BACKGROUND_TASKS_ROOT)


def _build_background_task_status_display_payload(status_payload: dict[str, object]) -> dict[str, object]:
    display_payload: dict[str, object] = {}
    for key in (
        "status",
        "task_kind",
        "scope",
        "pid",
        "started_at",
        "last_progress_at",
        "finished_at",
        "stale",
        "error",
        "recovery",
        "result",
    ):
        if key in status_payload:
            display_payload[key] = status_payload[key]
    result_path = str(status_payload.get("result_path") or "").strip()
    if result_path:
        display_payload["result_path"] = display_path(Path(result_path), APP_DIR)
    return display_payload


def _render_background_task_status(scope: str, title: str) -> None:
    registry_payload, status_payload = _load_registered_background_task(scope)
    st.markdown(f"**{title}**")
    if registry_payload is None:
        st.caption("当前尚无已登记的后台任务。")
        return
    task_dir = Path(str(registry_payload.get("task_dir") or ""))
    st.caption(f"最近任务目录：`{display_path(task_dir, APP_DIR)}`")
    if status_payload:
        st.json(_build_background_task_status_display_payload(status_payload))
    else:
        st.caption("任务状态文件尚未生成。")


def _launch_datahub_background_task(*, scope: str, task_kind: str, gcp_payload: dict[str, object], payload: dict[str, object]) -> Path:
    if background_task_is_running(scope, registry_root=BACKGROUND_TASKS_ROOT):
        raise ValueError("当前已有同类后台任务仍在运行，请先等待其完成。")
    task_dir = create_background_task_dir(task_kind, root_dir=BACKGROUND_TASKS_ROOT)
    write_background_task_config(
        task_dir,
        {
            "scope": scope,
            "task_kind": task_kind,
            "task_dir": str(task_dir),
            "gcp_payload": gcp_payload,
            "cookie_path": str(COOKIE_FILE_PATH),
            "cookie_refresh_batch_size": 100,
            "payload": payload,
        },
    )
    update_background_task_status(
        task_dir,
        {
            "status": "queued",
            "scope": scope,
            "task_kind": task_kind,
            "task_dir": str(task_dir),
            "created_at": datetime.now().isoformat(),
        },
    )
    pid = launch_background_worker(task_dir)
    register_active_background_task(scope, task_dir=task_dir, registry_root=BACKGROUND_TASKS_ROOT, pid=pid)
    update_background_task_status(
        task_dir,
        {
            "status": "queued",
            "scope": scope,
            "task_kind": task_kind,
            "task_dir": str(task_dir),
            "created_at": datetime.now().isoformat(),
            "pid": pid,
        },
    )
    return task_dir


page_config = {"page_title": "Bilibili DataHub", "layout": "wide"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
_inject_field_reference_panel_styles()
render_night_sky_background()
render_centered_header(st, "Bilibili DataHub", LOGO_PATH)
_sync_field_reference_doc_once()
st.markdown(
    "<p style='text-align: center; margin-bottom: 1rem;'>统一整合视频列表发现、批量抓取、接口调试、BigQuery/GCS 查看与本地自动批量抓取的一体化本地应用。</p>",
    unsafe_allow_html=True,
)

gcp_defaults = _load_gcp_form_defaults()
auto_defaults = _load_auto_form_defaults()
if "datahub_cookie_text" not in st.session_state:
    st.session_state["datahub_cookie_text"] = load_cookie_text(COOKIE_FILE_PATH)

with st.sidebar:
    st.subheader("全局设置")
    comment_limit_default = st.number_input("默认评论条数", min_value=1, max_value=100, value=10, step=1)
    chunk_size_mb = st.number_input("媒体分块大小（MB）", min_value=1, max_value=64, value=4, step=1)
    max_height = st.number_input("媒体最大分辨率（高度）", min_value=360, max_value=2160, value=1080, step=120)
    consecutive_failure_limit = st.number_input("手动批量抓取连续失败暂停阈值", min_value=1, max_value=100, value=10, step=1)
    cookie_text = st.text_area("可选：B 站 Cookie（可保存到本地，供后台任务热更新读取）", key="datahub_cookie_text", height=80)
    trigger_save_cookie = st.button("保存 / 更新 Cookie", width="stretch")
    st.caption(f"Cookie 文件：`{display_path(COOKIE_FILE_PATH, APP_DIR)}`")
    st.divider()
    st.subheader("Google Cloud 配置")
    with st.form("sidebar_gcp_config_form"):
        gcp_project_id = st.text_input("GCP Project ID", value=gcp_defaults["gcp_project_id"])
        bigquery_dataset = st.text_input("BigQuery Dataset", value=gcp_defaults["bigquery_dataset"])
        gcs_bucket_name = st.text_input("GCS Bucket 名称", value=gcp_defaults["gcs_bucket_name"])
        gcp_region = st.text_input("GCP Region（可选）", value=gcp_defaults["gcp_region"])
        credentials_path = st.text_input("服务账号 JSON 路径（可选）", value=gcp_defaults["credentials_path"])
        gcs_object_prefix = st.text_input("GCS 对象前缀", value=gcp_defaults["gcs_object_prefix"])
        gcs_public_base_url = st.text_input("公共访问基础 URL（可选）", value=gcp_defaults["gcs_public_base_url"])
        trigger_save_gcp_config = st.form_submit_button("保存 GCP 配置", width="stretch")
    st.divider()
    st.subheader("自动批量抓取设置")
    with st.form("sidebar_auto_config_form"):
        auto_crawl_interval_hours = st.number_input("自动批量抓取触发间隔（小时）", min_value=1, max_value=24, value=int(auto_defaults["crawl_interval_hours"]), step=1)
        auto_tracking_window_days = st.number_input("作者追踪窗口（天）", min_value=1, max_value=90, value=int(auto_defaults["tracking_window_days"]), step=1)
        auto_comment_limit = st.number_input("自动流程评论条数", min_value=1, max_value=100, value=int(auto_defaults["comment_limit"]), step=1)
        auto_max_videos_per_cycle = st.number_input("单轮最大追踪视频数", min_value=1, max_value=5000, value=int(auto_defaults["max_videos_per_cycle"]), step=1)
        auto_request_pause_seconds = st.number_input("自动流程请求延迟（秒）", min_value=0.0, max_value=30.0, value=float(auto_defaults["request_pause_seconds"]), step=0.5)
        auto_risk_pause_minutes = st.number_input("风控暂停起始分钟数", min_value=1, max_value=720, value=int(auto_defaults["risk_pause_minutes"]), step=1)
        auto_risk_pause_max_minutes = st.number_input("风控暂停最大分钟数", min_value=1, max_value=1440, value=int(auto_defaults["risk_pause_max_minutes"]), step=1)
        auto_lock_ttl_minutes = st.number_input("运行锁超时分钟数", min_value=1, max_value=720, value=int(auto_defaults["lock_ttl_minutes"]), step=1)
        auto_author_overlap_minutes = st.number_input("作者增量重叠分钟数", min_value=0, max_value=1440, value=int(auto_defaults["author_overlap_minutes"]), step=30)
        auto_pending_parallelism = st.number_input("待补元数据/媒体抓取并发度", min_value=1, max_value=16, value=int(auto_defaults["pending_once_batch_parallelism"]), step=1)
        trigger_save_auto_config = st.form_submit_button("保存自动批量抓取配置", width="stretch")

gcp_payload = {
    "gcp_project_id": gcp_project_id,
    "bigquery_dataset": bigquery_dataset,
    "gcs_bucket_name": gcs_bucket_name,
    "gcp_region": gcp_region,
    "credentials_path": credentials_path,
    "gcs_object_prefix": gcs_object_prefix,
    "gcs_public_base_url": gcs_public_base_url,
}
auto_payload = {
    **DEFAULT_AUTO_CONFIG,
    "crawl_interval_hours": int(auto_crawl_interval_hours),
    "tracking_window_days": int(auto_tracking_window_days),
    "comment_limit": int(auto_comment_limit),
    "max_videos_per_cycle": int(auto_max_videos_per_cycle),
    "request_pause_seconds": float(auto_request_pause_seconds),
    "risk_pause_minutes": int(auto_risk_pause_minutes),
    "risk_pause_max_minutes": int(auto_risk_pause_max_minutes),
    "lock_ttl_minutes": int(auto_lock_ttl_minutes),
    "author_overlap_minutes": int(auto_author_overlap_minutes),
    "pending_once_batch_parallelism": int(auto_pending_parallelism),
}

if trigger_save_gcp_config:
    saved_path = save_json_config(LOCAL_GCP_CONFIG_PATH, gcp_payload)
    st.session_state.pop("datahub_auto_status_snapshot", None)
    st.sidebar.success(f"GCP 配置已保存：{saved_path}")
if trigger_save_auto_config:
    saved_path = save_json_config(LOCAL_AUTO_CONFIG_PATH, auto_payload)
    st.session_state.pop("datahub_auto_status_snapshot", None)
    st.sidebar.success(f"自动批量抓取配置已保存：{saved_path}")
if trigger_save_cookie:
    saved_path = save_cookie_text(cookie_text, path=COOKIE_FILE_PATH)
    st.sidebar.success(f"Cookie 已保存：{saved_path}")

active_credential = build_credential_from_cookie(cookie_text)
active_gcp_config = build_gcp_config(gcp_payload)
gcp_payload_json = _serialize_cache_payload(gcp_payload)
auto_payload_json = _serialize_cache_payload(auto_payload)
active_media_strategy = _build_media_strategy(
    max_height=int(max_height),
    chunk_size_mb=int(chunk_size_mb),
    gcp_config=active_gcp_config,
)

def _resolve_store() -> BigQueryCrawlerStore:
    if not active_gcp_config.is_enabled():
        raise ValueError("请先完成 GCP 配置。")
    return _build_store_cached(gcp_payload_json)


def _resolve_local_runner() -> DataHubLocalCycleRunner:
    if not active_gcp_config.is_enabled():
        raise ValueError("自动批量抓取尚未就绪，请先完成 GCP 配置。")
    return _build_local_runner_cached(gcp_payload_json, auto_payload_json, cookie_text)


def _load_auto_status_snapshot() -> dict[str, object]:
    local_runner = _resolve_local_runner()
    status_payload = local_runner.status()
    status_payload["author_source_rows"] = local_runner.tracker_store.list_author_sources()
    return status_payload

field_reference_df = _get_field_reference_df()
main_col, reference_col = st.columns([3.4, 1.6], gap="large")

with reference_col:
    st.markdown("<div id='field-reference-sticky-anchor'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        _render_field_reference_panel(field_reference_df)

with main_col:
    tab_discover, tab_author_refinement, tab_merge, tab_debug, tab_auto, tab_manual_batch, tab_manual_media, tab_db, tab_quick_jump, tab_tid = st.tabs(
        [
            "视频列表构建",
            "作者数据洞察&精简",
            "文件拼接及去重",
            "数据抓取调试",
            "自动批量抓取",
            "手动批量抓取-动态数据",
            "手动批量抓取-媒体/元数据",
            "BigQuery / GCS 数据查看",
            "快捷跳转",
            "tid 与分区名称对应",
        ]
    )

with tab_discover:
    st.subheader("视频列表构建")
    full_export_tab, owner_expand_tab, bvid_lookup_tab, failed_uid_tab = st.tabs(
        ["自定义全量导出", "导出作者一段时间内上传视频列表", "BVID 回查作者 UID", "从日志提取失败作者 UID"]
    )

    with full_export_tab:
        with st.form("full_export_form"):
            full_daily_hot = st.checkbox("抓取当日全站热门（默认400条）", value=True)
            full_weekly_enabled = st.checkbox("抓取过去 n 期每周必看", value=True)
            full_weekly_weeks = st.number_input("n（每周必看期数）", min_value=1, max_value=520, value=12, step=1)
            full_column_top = st.checkbox("抓取全部分区的当天主流视频", value=True)
            full_rankboard = st.checkbox("抓取实时排行榜（每个分区实时显示100条）", value=False)
            full_selection = CustomSeedSelection(
                include_daily_hot=bool(full_daily_hot),
                weekly_weeks=int(full_weekly_weeks) if full_weekly_enabled else 0,
                include_column_top=bool(full_column_top),
                include_rankboard=bool(full_rankboard),
            )
            full_default_name = (
                f"{'-'.join(full_selection.active_tokens())}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                if full_selection.active_tokens()
                else f"custom_full_export-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            full_out_path = st.text_input(
                "输出视频列表 CSV 文件路径",
                value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / "full_site_floorings" / full_default_name),
            )
            trigger_full_export = st.form_submit_button("开始执行自定义全量导出")
        full_log_placeholder = st.empty()
        if trigger_full_export:
            logs: list[str] = []

            def _log(message: str) -> None:
                append_live_log(logs, full_log_placeholder, message)

            if not full_selection.active_tokens():
                st.warning("请至少选择一种抓取来源。")
            else:
                try:
                    valid_tids = load_full_export_tids() if full_selection.include_column_top else []
                    rankboard_boards = load_rankboard_boards() if full_selection.include_rankboard else []
                    result = build_custom_seed_result(full_selection, valid_tids, logger=_log) if (full_selection.include_daily_hot or full_selection.weekly_weeks > 0 or full_selection.include_column_top) else None
                    rankboard_result = build_rankboard_result(rankboard_boards, logger=_log) if full_selection.include_rankboard else None
                    rows: list[dict] = []
                    if result is not None:
                        rows.extend(discover_entries_to_full_export_rows(result.entries))
                    if rankboard_result is not None:
                        rows.extend(rankboard_entries_to_rows(rankboard_result.entries))
                    saved = export_rows_csv(rows, full_out_path)
                    owner_mids, missing_bvids = extract_owner_mids_from_rows(rows)
                    if missing_bvids:
                        fallback_owner_mids, _failed_bvids = resolve_owner_mids_from_bvids(missing_bvids)
                        for owner_mid in fallback_owner_mids:
                            if owner_mid not in owner_mids:
                                owner_mids.append(owner_mid)
                    authors_saved = save_owner_mid_csv(owner_mids, saved.with_name(f"{saved.stem}_authors.csv"))
                except Exception as exc:  # noqa: BLE001
                    st.error(f"自定义全量导出失败：{exc}")
                else:
                    st.success(f"已导出视频列表：{saved}")
                    st.caption(f"作者 UID 列表：`{display_path(authors_saved, APP_DIR)}`")
                    st.dataframe(pd.DataFrame(rows).head(200), width="stretch", hide_index=True)
                finally:
                    log_path = save_timestamped_task_log("custom_full_export", logs, log_dir=FULL_SITE_FLOORINGS_LOGS_DIR)
                    if log_path is not None:
                        st.caption(f"运行日志：`{display_path(log_path, APP_DIR)}`")

    with owner_expand_tab:
        with st.form("owner_expand_form"):
            owner_files = st.file_uploader("上传保存作者 ID 的 CSV/XLSX 文件（可多选）", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
            owner_column = st.text_input("作者 ID 所在列名", value="owner_mid")
            owner_start_date = st.date_input("start_date", value=DEFAULT_UID_EXPANSION_START_DATE)
            owner_end_date = st.date_input("end_date", value=datetime.now().date())
            owner_out_path = st.text_input("输出根目录（自动创建 uid_expansions/...）", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR))
            trigger_owner_expand = st.form_submit_button("开始导出作者一段时间内上传视频列表")
        owner_log_placeholder = st.empty()
        if trigger_owner_expand:
            owner_logs: list[str] = []
            live_log_path: Path | None = None
            session = None
            requested_start_at = None
            requested_end_at = None
            owner_since_overrides = None
            reused_owner_count = 0
            owner_mids: list[int] = []
            task_started_at = None
            outcome = None
            saved = None
            remaining_saved = None
            summary_saved = None

            def _owner_log(message: str) -> None:
                append_live_log(owner_logs, owner_log_placeholder, message, mirror_path=live_log_path)

            try:
                if not owner_files:
                    raise ValueError("请先上传至少一个 CSV/XLSX 文件。")
                owner_dfs = _read_uploaded_files(owner_files)
                prepared_owner_df = _merge_and_deduplicate_by_column(owner_dfs, owner_column.strip())
                owner_mids, invalid_count = extract_owner_mids(prepared_owner_df, owner_column.strip())
                if invalid_count:
                    st.warning(f"检测到 {invalid_count} 条无法解析的作者 ID，已忽略。")
                if not owner_mids:
                    raise ValueError("未从上传文件中解析出有效作者 ID。")
                task_started_at = datetime.now().replace(microsecond=0)
                requested_start_at = coerce_start_datetime(owner_start_date)
                requested_end_at = coerce_end_datetime(owner_end_date, reference_now=task_started_at)
                output_root = normalize_output_root(owner_out_path)
                session = prepare_uid_expansion_session(
                    output_root,
                    [uploaded_file.name for uploaded_file in owner_files],
                    owner_mids,
                    requested_start_at,
                    requested_end_at,
                    task_started_at,
                    logger=_owner_log,
                )
                live_log_path = session.logs_dir / f"uid_expansion_part_{session.part_number}_running.log"
                persist_live_log_snapshot(owner_logs, live_log_path)
                owner_since_overrides, reused_owner_count = build_owner_since_overrides(
                    owner_mids,
                    output_root,
                    requested_start_at,
                    requested_end_at,
                    excluded_session_dirs=[session.session_dir],
                    logger=_owner_log,
                )
                if session.is_new_session:
                    session.session_dir.mkdir(parents=True, exist_ok=True)
                    save_owner_mid_csv(owner_mids, session.session_dir / "original_uids.csv")
                outcome = build_result_from_owner_mids_with_guardrails(
                    owner_mids,
                    requested_start_at,
                    requested_end_at,
                    owner_since_overrides=owner_since_overrides,
                    logger=_owner_log,
                )
                outcome.result, _removed_count = drop_existing_uid_expansion_duplicates(outcome.result, output_root, logger=_owner_log)
                saved = export_discover_result_csv(outcome.result, session.session_dir / f"videolist_part_{session.part_number}.csv")
                remaining_saved = None
                if outcome.remaining_owner_mids:
                    remaining_saved = save_owner_mid_csv(outcome.remaining_owner_mids, session.session_dir / f"remaining_uids_part_{session.part_number}.csv")
            except Exception as exc:  # noqa: BLE001
                _owner_log(f"[ERROR]: 作者批量抓取任务失败：{summarize_exception(exc)}")
                st.error(f"作者视频扩展失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                if remaining_saved is not None:
                    st.caption(f"剩余作者 UID：`{display_path(remaining_saved, APP_DIR)}`")
                if summary_saved is not None:
                    st.caption(f"任务总结：`{display_path(summary_saved, APP_DIR)}`")
                st.dataframe(pd.DataFrame(discover_entries_to_rows(outcome.result.entries)).head(200), width="stretch", hide_index=True)
            finally:
                if session is not None:
                    log_path = save_timestamped_task_log(f"uid_expansion_part_{session.part_number}", owner_logs, log_dir=session.logs_dir)
                    if live_log_path is not None:
                        live_log_path.unlink(missing_ok=True)
                    if log_path is not None:
                        st.caption(f"运行日志：`{display_path(log_path, APP_DIR)}`")
                    if log_path is not None and saved is not None and outcome is not None and requested_start_at is not None and requested_end_at is not None and owner_since_overrides is not None and task_started_at is not None:
                        state = record_uid_expansion_part(
                            session,
                            requested_start_at,
                            requested_end_at,
                            owner_since_overrides,
                            reused_owner_count,
                            len(owner_mids),
                            outcome,
                            saved,
                            remaining_saved,
                            log_path,
                            task_started_at.isoformat(timespec="seconds"),
                            datetime.now().isoformat(timespec="seconds"),
                        )
                        if not outcome.remaining_owner_mids:
                            summary_saved = write_uid_expansion_summary(session.session_dir, state)
                            st.caption(f"任务总结：`{display_path(summary_saved, APP_DIR)}`")

    with bvid_lookup_tab:
        with st.form("bvid_lookup_form"):
            bvid_files = st.file_uploader("上传保存 BVID 的 CSV/XLSX 文件（可多选）", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
            bvid_column = st.text_input("BVID 所在列名", value="bvid")
            bvid_out_path = st.text_input("输出 CSV 文件路径", value=str(BVID_TO_UIDS_OUTPUT_DIR / f"bvid_to_uids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
            trigger_bvid_lookup = st.form_submit_button("开始 BVID 回查作者 UID")
        if trigger_bvid_lookup:
            try:
                if not bvid_files:
                    raise ValueError("请先上传文件。")
                bvid_dfs = _read_uploaded_files(bvid_files)
                prepared_bvid_df = _merge_and_deduplicate_by_column(bvid_dfs, bvid_column.strip())
                bvids = extract_bvids(prepared_bvid_df, bvid_column.strip())
                owner_mids, failed_bvids = resolve_owner_mids_from_bvids(bvids)
                saved = save_owner_mid_csv(owner_mids, Path(bvid_out_path))
            except Exception as exc:  # noqa: BLE001
                st.error(f"BVID 回查失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                if failed_bvids:
                    st.warning(f"有 {len(failed_bvids)} 个 BVID 未能解析作者。")
                st.dataframe(owner_mid_dataframe(owner_mids).head(200), width="stretch", hide_index=True)

    with failed_uid_tab:
        with st.form("failed_uid_form"):
            uploaded_logs = st.file_uploader("上传 .log/.txt 抓取日志文件（可多选）", type=["log", "txt"], accept_multiple_files=True)
            manual_log_text = st.text_area("或直接粘贴日志文本", value="", height=180)
            failed_uid_out_path = st.text_input("失败作者 UID 导出路径", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / f"failed_owner_mids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
            trigger_failed_uid = st.form_submit_button("提取失败作者 UID")
        if trigger_failed_uid:
            try:
                texts: list[str] = []
                for uploaded in uploaded_logs or []:
                    texts.append(decode_uploaded_text(uploaded.getvalue()))
                if manual_log_text.strip():
                    texts.append(manual_log_text)
                if not texts:
                    raise ValueError("请上传日志文件或粘贴日志文本。")
                owner_mids = sorted(set(owner_mid for text in texts for owner_mid in extract_failed_owner_mids_from_text(text)))
                saved = save_owner_mid_csv(owner_mids, Path(failed_uid_out_path))
            except Exception as exc:  # noqa: BLE001
                st.error(f"提取失败作者 UID 失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                st.dataframe(owner_mid_dataframe(owner_mids).head(200), width="stretch", hide_index=True)

with tab_author_refinement:
    st.subheader("作者数据洞察&精简")
    st.caption(
        "支持断点续跑：当连续触发风控报错达到阈值时，会先保存当前已扩充作者列表和剩余待爬作者列表；"
        "后续若上传某次任务目录中的 `remaining_authors_part_n.csv`，则会自动续跑原任务而不是新建目录。"
    )
    author_related_fields_df = field_reference_df[
        field_reference_df["数据模块"].eq("MetaResult")
        & field_reference_df["变量名称"].isin(
            [
                "owner_mid",
                "owner_name",
                "owner_face",
                "owner_sign",
                "owner_gender",
                "owner_level",
                "owner_verified",
                "owner_verified_title",
                "owner_vip_type",
                "owner_follower_count",
                "owner_following_count",
                "owner_video_count",
            ]
        )
    ]
    with st.expander("查看本模块会扩展的 MetaResult 级作者字段", expanded=False):
        st.dataframe(author_related_fields_df, width="stretch", hide_index=True, height=320)

    refinement_task_tab, refinement_visual_tab = st.tabs(["抓取 / 续跑任务", "直接可视化扩充后列表"])

    with refinement_task_tab:
        with st.form("author_refinement_task_form"):
            refinement_upload = st.file_uploader(
                "上传作者列表 CSV/XLSX 文件，或上传历史任务里的 `remaining_authors_part_n.csv` 续跑",
                type=["csv", "xlsx", "xls"],
                key="author_refinement_upload",
            )
            refinement_owner_column = st.text_input(
                "作者 ID 所在列名（续跑 remaining 文件时默认仍按 `owner_mid` 识别）",
                value="owner_mid",
                key="author_refinement_owner_column",
            )
            refinement_ratio = st.slider("完成任务后默认保存的精简保留比例", min_value=0.0, max_value=1.0, value=0.3, step=0.01)
            refinement_pause_seconds = st.number_input(
                "作者接口请求间隔（秒）",
                min_value=0.0,
                max_value=10.0,
                value=0.2,
                step=0.1,
                help="作者抓取按顺序执行；适当增加间隔可降低触发风控的概率。",
            )
            refinement_risk_limit = st.number_input(
                "连续风控报错暂停阈值",
                min_value=1,
                max_value=50,
                value=5,
                step=1,
                help="连续命中风控相关报错达到该次数后，本轮先暂停，并导出 remaining 作者列表用于续跑。",
            )
            enable_refinement_sleep_resume = st.checkbox(
                "开启风控后睡眠续跑",
                value=False,
                help="开启后，若某个 part 因连续风控报错暂停，程序会先睡眠一段时间，再自动进入下一个 part。",
            )
            refinement_sleep_minutes = st.number_input(
                "风控后睡眠时长（分钟）",
                min_value=1,
                max_value=1440,
                value=5,
                step=1,
            )
            refinement_random_seed = st.number_input("完成任务后默认保存的精简随机种子", min_value=0, max_value=999999, value=42, step=1)
            trigger_refinement = st.form_submit_button("开始洞察 / 续跑")
        refinement_log_placeholder = st.empty()

        if trigger_refinement:
            st.session_state.pop("author_refinement_result", None)
            st.session_state.pop("author_refinement_generated_outputs", None)
            refinement_logs: list[str] = []
            live_log_path: Path | None = None
            session = None
            task_dir: Path | None = None
            log_path: Path | None = None
            summary_saved: Path | None = None
            original_saved: Path | None = None
            metadata_part_saved: Path | None = None
            accumulated_saved: Path | None = None
            remaining_saved: Path | None = None
            owner_column_name = ""
            invalid_count = 0
            outcome = None
            state = None

            def _refinement_log(message: str) -> None:
                append_live_log(refinement_logs, refinement_log_placeholder, message, mirror_path=live_log_path)

            try:
                owner_column_name = refinement_owner_column.strip()
                if refinement_upload is None:
                    raise ValueError("请先上传作者列表文件。")
                if not owner_column_name:
                    raise ValueError("请填写作者 ID 列名。")

                raw_df = read_uploaded_dataframe(refinement_upload)
                validate_columns(raw_df, [owner_column_name], "作者 ID 列")
                deduplicated_df, _dedupe_time_column = deduplicate_dataframe(raw_df, dedupe_keys=[owner_column_name])
                owner_mids, invalid_count = extract_owner_mids(deduplicated_df, owner_column_name)
                deduplicated_df = deduplicated_df.copy()
                deduplicated_df[owner_column_name] = pd.to_numeric(deduplicated_df[owner_column_name], errors="coerce").astype("Int64")
                if not owner_mids:
                    raise ValueError("未从上传文件中解析出有效作者 ID。")

                task_started_at = datetime.now().replace(microsecond=0)
                session = prepare_author_refinement_session(
                    AUTHOR_REFINEMENTS_OUTPUT_DIR,
                    refinement_upload.name,
                    owner_mids,
                    task_started_at,
                    logger=_refinement_log,
                )
                task_dir = session.session_dir

                if invalid_count:
                    _refinement_log(f"[WARN] 检测到 {invalid_count} 条无法解析的作者 ID，已在后续抓取中忽略。")

                if session.is_new_session:
                    original_saved = export_dataframe(deduplicated_df, task_dir / AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME)
                    _refinement_log(f"[INFO] 已保存任务原始作者列表：{display_path(original_saved, APP_DIR)}")
                else:
                    original_saved = task_dir / AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME

                current_owner_mids = owner_mids
                while True:
                    live_log_path = task_dir / "logs" / f"author_refinement_part_{session.part_number}_running.log"
                    persist_live_log_snapshot(refinement_logs, live_log_path)
                    metadata_part_saved = None
                    accumulated_saved = None
                    remaining_saved = None

                    _refinement_log(f"[INFO] 开始执行 author_refinement part_{session.part_number}，待处理作者数 {len(current_owner_mids)}。")
                    outcome = crawl_author_metadata_with_guardrails(
                        current_owner_mids,
                        credential=active_credential,
                        logger=_refinement_log,
                        request_pause_seconds=float(refinement_pause_seconds),
                        consecutive_risk_limit=int(refinement_risk_limit),
                    )
                    metadata_part_saved = export_dataframe(
                        outcome.metadata_df,
                        task_dir / f"author_metadata_part_{session.part_number}.csv",
                    )
                    accumulated_df = build_session_expanded_author_list(task_dir)
                    accumulated_saved = export_dataframe(accumulated_df, task_dir / AUTHOR_REFINEMENT_ACCUMULATED_FILENAME)
                    _refinement_log(f"[INFO] 已保存当前累计扩充作者列表：{display_path(accumulated_saved, APP_DIR)}")

                    if outcome.remaining_owner_mids:
                        remaining_saved = save_remaining_author_csv(
                            outcome.remaining_owner_mids,
                            task_dir / f"remaining_authors_part_{session.part_number}.csv",
                        )
                        _refinement_log(f"[WARN] 已导出剩余待爬作者列表：{display_path(remaining_saved, APP_DIR)}")

                    part_log_path = save_timestamped_task_log(
                        f"author_refinement_part_{session.part_number}",
                        refinement_logs,
                        log_dir=task_dir / "logs",
                    )
                    state = record_author_refinement_part(
                        session,
                        owner_mid_column=owner_column_name,
                        source_name=refinement_upload.name if refinement_upload is not None else "",
                        input_owner_count=len(current_owner_mids),
                        outcome=outcome,
                        metadata_part_path=metadata_part_saved,
                        accumulated_path=accumulated_saved,
                        remaining_path=remaining_saved,
                        failures_path=None,
                        log_path=part_log_path,
                        run_started_at=task_started_at.isoformat(timespec="seconds"),
                        run_finished_at=datetime.now().isoformat(timespec="seconds"),
                    )
                    summary_saved = write_author_refinement_summary(task_dir, state)

                    if not outcome.remaining_owner_mids:
                        break

                    if not (enable_refinement_sleep_resume and outcome.stopped_due_to_risk):
                        break

                    sleep_seconds = int(refinement_sleep_minutes) * 60
                    _refinement_log(
                        f"[INFO] 当前 part 因连续风控报错达到阈值而暂停；已开启睡眠续跑，"
                        f"将在 {int(refinement_sleep_minutes)} 分钟后自动进入下一个 part。"
                    )
                    time.sleep(sleep_seconds)
                    session = build_followup_author_refinement_session(task_dir)
                    current_owner_mids = outcome.remaining_owner_mids
                    task_started_at = datetime.now().replace(microsecond=0)
            except Exception as exc:  # noqa: BLE001
                _refinement_log(f"[ERROR] 作者数据洞察失败：{exc}")
                st.error(f"作者数据洞察失败：{exc}")
            finally:
                if task_dir is not None:
                    log_path = save_timestamped_task_log("author_refinement", refinement_logs, log_dir=task_dir / "logs")
                    if live_log_path is not None:
                        live_log_path.unlink(missing_ok=True)
                    if outcome is not None and state is not None:
                        st.session_state["author_refinement_result"] = {
                            "status": "completed" if state.get("completed_all") else ("paused" if outcome.stopped_due_to_risk else "partial"),
                            "task_dir": str(task_dir),
                            "original_saved": str(original_saved) if original_saved is not None else "",
                            "metadata_part_saved": str(metadata_part_saved) if metadata_part_saved is not None else "",
                            "accumulated_saved": str(accumulated_saved) if accumulated_saved is not None else "",
                            "remaining_saved": str(remaining_saved) if remaining_saved is not None else "",
                            "summary_saved": str(summary_saved) if summary_saved is not None else "",
                            "owner_column_name": owner_column_name,
                            "invalid_count": invalid_count,
                            "failure_count": len(outcome.failures),
                            "remaining_count": len(outcome.remaining_owner_mids),
                            "sample_ratio": float(refinement_ratio),
                            "random_seed": int(refinement_random_seed),
                            "last_risk_error": outcome.last_risk_error,
                            "sleep_resume_enabled": bool(enable_refinement_sleep_resume),
                            "sleep_minutes": int(refinement_sleep_minutes),
                        }
                    if log_path is not None:
                        st.caption(f"运行日志：`{display_path(log_path, APP_DIR)}`")

            result = st.session_state.get("author_refinement_result")
            if result:
                if result["status"] == "completed":
                    st.success("作者数据洞察任务已完整完成，并已输出完整扩充表、精简结果与图表。")
                elif result["status"] == "paused":
                    st.warning("作者数据洞察任务已按连续风控阈值暂停；当前累计扩充作者表和 remaining 作者列表已保存，可上传 remaining 文件续跑。")

        refinement_result = st.session_state.get("author_refinement_result")
        if refinement_result:
            st.caption(f"任务目录：`{display_path(Path(refinement_result['task_dir']), APP_DIR)}`")
            if refinement_result.get("original_saved"):
                st.caption(f"原始作者列表：`{display_path(Path(refinement_result['original_saved']), APP_DIR)}`")
            if refinement_result.get("metadata_part_saved"):
                st.caption(f"本次产出的作者属性分片：`{display_path(Path(refinement_result['metadata_part_saved']), APP_DIR)}`")
            if refinement_result.get("accumulated_saved"):
                st.caption(f"当前累计扩充作者列表：`{display_path(Path(refinement_result['accumulated_saved']), APP_DIR)}`")
            if refinement_result.get("remaining_saved"):
                st.caption(f"剩余待爬作者列表：`{display_path(Path(refinement_result['remaining_saved']), APP_DIR)}`")
            if refinement_result.get("summary_saved"):
                st.caption(f"任务总结：`{display_path(Path(refinement_result['summary_saved']), APP_DIR)}`")
            if refinement_result.get("last_risk_error"):
                st.caption(f"最近一次风控相关错误：`{refinement_result['last_risk_error']}`")
            accumulated_df = _load_saved_dataframe(refinement_result.get("accumulated_saved", ""))
            if isinstance(accumulated_df, pd.DataFrame) and not accumulated_df.empty:
                with st.expander("查看当前累计扩充作者列表预览", expanded=False):
                    st.dataframe(accumulated_df.head(200), width="stretch", hide_index=True)

            st.divider()
            st.caption("上传完整作者清单后，点击按钮再生成完整汇总、精简结果与图表。这个过程不再自动绑定在抓取任务结束时执行。")
            with st.form("author_refinement_generate_outputs_form"):
                full_author_upload = st.file_uploader(
                    "上传完整的作者列表 CSV/XLSX 文件",
                    type=["csv", "xlsx", "xls"],
                    key="author_refinement_full_author_upload",
                )
                trigger_generate_outputs = st.form_submit_button("根据完整清单生成汇总与图表")
            if trigger_generate_outputs:
                try:
                    if full_author_upload is None:
                        raise ValueError("请先上传完整的作者列表文件。")
                    full_author_df = read_uploaded_dataframe(full_author_upload)
                    validate_columns(full_author_df, ["owner_follower_count"], "完整作者列表字段")
                    task_dir = Path(refinement_result["task_dir"])
                    complete_saved = export_dataframe(full_author_df, task_dir / AUTHOR_REFINEMENT_COMPLETE_FILENAME)
                    refined_full_df, sampled_df, bin_summary_df = stratified_sample_by_followers(
                        full_author_df,
                        sample_ratio=float(refinement_result["sample_ratio"]),
                        random_state=int(refinement_result["random_seed"]),
                    )
                    refined_full_saved = export_dataframe(refined_full_df, task_dir / AUTHOR_REFINEMENT_REFINED_FULL_FILENAME)
                    sampled_saved = export_dataframe(sampled_df, task_dir / AUTHOR_REFINEMENT_REFINED_SAMPLED_FILENAME)
                    bin_summary_saved = export_dataframe(bin_summary_df, task_dir / AUTHOR_REFINEMENT_BIN_SUMMARY_FILENAME)
                    base_distribution_figure, _mapping_summary = build_author_distribution_figure(full_author_df)
                    base_chart_saved = task_dir / AUTHOR_REFINEMENT_BASE_CHART_FILENAME
                    base_distribution_figure.write_html(base_chart_saved, include_plotlyjs="cdn")
                    compare_figure = build_refinement_comparison_figure(full_author_df, sampled_df)
                    compare_chart_saved = task_dir / AUTHOR_REFINEMENT_COMPARE_CHART_FILENAME
                    compare_figure.write_html(compare_chart_saved, include_plotlyjs="cdn")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"根据完整清单生成汇总与图表失败：{exc}")
                else:
                    st.session_state["author_refinement_generated_outputs"] = {
                        "task_dir": refinement_result["task_dir"],
                        "complete_saved": str(complete_saved),
                        "refined_full_saved": str(refined_full_saved),
                        "sampled_saved": str(sampled_saved),
                        "bin_summary_saved": str(bin_summary_saved),
                        "base_chart_saved": str(base_chart_saved),
                        "compare_chart_saved": str(compare_chart_saved),
                    }
                    st.success("已根据完整清单生成汇总、精简结果与图表。")

            generated_outputs = st.session_state.get("author_refinement_generated_outputs")
            if generated_outputs and generated_outputs.get("task_dir") == refinement_result["task_dir"]:
                st.caption(f"完整扩充后的作者列表：`{display_path(Path(generated_outputs['complete_saved']), APP_DIR)}`")
                st.caption(f"精简结果（完整版，含保留标记）：`{display_path(Path(generated_outputs['refined_full_saved']), APP_DIR)}`")
                st.caption(f"精简结果（精简版子集）：`{display_path(Path(generated_outputs['sampled_saved']), APP_DIR)}`")
                st.caption(f"粉丝分布图 HTML：`{display_path(Path(generated_outputs['base_chart_saved']), APP_DIR)}`")
                st.caption(f"精简前后分布对比图 HTML：`{display_path(Path(generated_outputs['compare_chart_saved']), APP_DIR)}`")
                complete_df = _load_saved_dataframe(generated_outputs.get("complete_saved", ""))
                if isinstance(complete_df, pd.DataFrame) and not complete_df.empty:
                    _render_author_refinement_visualization_panel(
                        expanded_df=complete_df,
                        key_prefix="author_refinement_task_visual",
                        default_ratio=float(refinement_result["sample_ratio"]),
                        default_seed=int(refinement_result["random_seed"]),
                    )

    with refinement_visual_tab:
        st.caption("如果你已经有扩充后的作者列表，可直接上传并生成粉丝分布图、类别叠加图和精简前后对比图。")
        with st.form("author_refinement_visual_upload_form"):
            visual_upload = st.file_uploader(
                "上传扩充后的作者列表 CSV/XLSX 文件",
                type=["csv", "xlsx", "xls"],
                key="author_refinement_visual_upload",
            )
            trigger_visual_upload = st.form_submit_button("加载并可视化扩充后作者列表")
        if trigger_visual_upload:
            try:
                if visual_upload is None:
                    raise ValueError("请先上传扩充后的作者列表文件。")
                visual_df = read_uploaded_dataframe(visual_upload)
                validate_columns(visual_df, ["owner_follower_count"], "扩充后的作者列表字段")
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载扩充后作者列表失败：{exc}")
            else:
                visual_saved_path = APP_DIR / ".local" / "author_refinement_visual_preview.csv"
                export_dataframe(visual_df, visual_saved_path)
                st.session_state["author_refinement_visual_path"] = str(visual_saved_path)
                st.session_state["author_refinement_visual_source_name"] = visual_upload.name
                st.success("扩充后的作者列表已加载，可直接查看可视化结果。")

        visual_df = _load_saved_dataframe(st.session_state.get("author_refinement_visual_path", ""))
        if isinstance(visual_df, pd.DataFrame) and not visual_df.empty:
            with st.expander("查看扩充后作者列表预览", expanded=False):
                st.dataframe(visual_df.head(200), width="stretch", hide_index=True)
            _refined_full_df, sampled_df, _bin_summary_df = _render_author_refinement_visualization_panel(
                expanded_df=visual_df,
                key_prefix="author_refinement_visual_only",
                default_ratio=0.3,
                default_seed=42,
            )
            source_name = Path(st.session_state.get("author_refinement_visual_source_name", "expanded_authors.csv")).stem
            sampled_csv = sampled_df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "导出精简作者列表",
                data=sampled_csv,
                file_name=f"{source_name}_refined_sampled.csv",
                mime="text/csv",
            )

with tab_merge:
    st.subheader("文件拼接及去重")
    with st.form("merge_form"):
        uploaded_files = st.file_uploader("上传多个 CSV/XLSX 文件", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
        sort_keys_raw = st.text_input("排序键（逗号分隔，留空则默认按 bvid 降序）", value="")
        dedupe_enabled = st.checkbox("对拼接结果去重", value=False)
        dedupe_keys_raw = st.text_input("去重键（逗号分隔）", value="bvid")
        keep_keys_raw = st.text_input("保留列（逗号分隔，可留空）", value="")
        merge_out_path = st.text_input("输出文件路径", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        trigger_merge = st.form_submit_button("开始拼接及去重")
    if trigger_merge:
        try:
            if not uploaded_files:
                raise ValueError("请先上传至少一个文件。")
            dfs = _read_uploaded_files(uploaded_files)
            sort_keys = parse_comma_separated_keys(sort_keys_raw)
            merged = merge_dataframes(dfs, sort_keys)
            saved = export_dataframe(merged, resolve_output_path(merge_out_path))
            dedup_saved = None
            if dedupe_enabled:
                dedupe_keys = parse_comma_separated_keys(dedupe_keys_raw)
                keep_keys = parse_comma_separated_keys(keep_keys_raw) or None
                deduplicated, _time_col = deduplicate_dataframe(merged, dedupe_keys=dedupe_keys, keep_keys=keep_keys)
                dedup_saved = export_dataframe(deduplicated, build_deduplicated_output_path(saved))
        except Exception as exc:  # noqa: BLE001
            st.error(f"拼接失败：{exc}")
        else:
            st.success(f"拼接结果已导出：{saved}")
            if dedup_saved is not None:
                st.caption(f"去重结果：`{display_path(dedup_saved, APP_DIR)}`")
            st.dataframe(merged.head(200), width="stretch", hide_index=True)

with tab_debug:
    st.subheader("数据抓取调试")
    single_tab, api_tab = st.tabs(["单 bvid 全流程", "四类接口调试"])

    with single_tab:
        if not active_gcp_config.is_enabled():
            st.warning("单视频全流程抓取依赖 GCP 配置；请先完成侧边栏中的 BigQuery / GCS 设置。")
        with st.form("debug_single_form"):
            col_a, col_b = st.columns([2, 1])
            with col_a:
                single_bvid = st.text_input("输入 bvid", value="", key="single_bvid")
            with col_b:
                enable_media_single = st.checkbox("抓取媒体", value=True, key="single_enable_media")
            trigger_single_bundle = st.form_submit_button("开始单视频顺序抓取")
        if trigger_single_bundle:
            if not active_gcp_config.is_enabled():
                st.warning("请先完成 GCP 配置。")
            elif not single_bvid.strip():
                st.warning("请先输入 bvid。")
            else:
                try:
                    summary = crawl_full_video_bundle(
                        single_bvid.strip(),
                        enable_media=enable_media_single,
                        task_mode=CrawlTaskMode.FULL_BUNDLE,
                        comment_limit=int(comment_limit_default),
                        gcp_config=active_gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"抓取失败：{exc}")
                else:
                    if summary.errors:
                        st.warning("流程结束，但部分阶段失败。")
                    else:
                        st.success("单视频全流程抓取成功。")
                    _show_json("流程摘要", summary.to_dict())

    with api_tab:
        meta_tab, stat_tab, comment_tab, media_tab = st.tabs(["Meta", "Stat", "Comment", "Media"])
        with meta_tab:
            with st.form("debug_meta_form"):
                meta_bvid = st.text_input("Meta: bvid", key="meta_bvid")
                trigger_meta_api = st.form_submit_button("调用 Meta 接口")
            if trigger_meta_api:
                result = crawl_video_meta(meta_bvid.strip(), gcp_config=active_gcp_config, credential=active_credential)
                _show_meta_result(result)
        with stat_tab:
            with st.form("debug_stat_form"):
                stat_bvid = st.text_input("Stat: bvid", key="stat_bvid")
                trigger_stat_api = st.form_submit_button("调用 Stat 接口")
            if trigger_stat_api:
                result = crawl_stat_snapshot(stat_bvid.strip(), gcp_config=active_gcp_config, credential=active_credential)
                _show_json("StatSnapshot", result.to_dict())
        with comment_tab:
            with st.form("debug_comment_form"):
                comment_bvid = st.text_input("Comment: bvid", key="comment_bvid")
                comment_limit = st.number_input("Comment: limit", min_value=1, max_value=100, value=int(comment_limit_default), step=1)
                trigger_comment_api = st.form_submit_button("调用 Comment 接口")
            if trigger_comment_api:
                result = crawl_latest_comments(comment_bvid.strip(), limit=int(comment_limit), gcp_config=active_gcp_config, credential=active_credential)
                _show_json("CommentSnapshot", result.to_dict())
        with media_tab:
            with st.form("debug_media_form"):
                media_bvid = st.text_input("Media: bvid", key="media_bvid")
                use_streaming_api = st.checkbox("使用 stream_media_to_store", value=True)
                trigger_media_api = st.form_submit_button("调用 Media 接口")
            if trigger_media_api:
                if use_streaming_api:
                    result = stream_media_to_store(media_bvid.strip(), strategy=active_media_strategy, credential=active_credential)
                else:
                    result = crawl_media_assets(
                        media_bvid.strip(),
                        strategy=active_media_strategy,
                        gcp_config=active_gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        credential=active_credential,
                    )
                _show_json("MediaResult", result.to_dict())

with tab_auto:
    st.subheader("自动批量抓取")
    if not active_gcp_config.is_enabled():
        st.warning("自动批量抓取依赖 GCP 配置；请先完成侧边栏中的 BigQuery / GCS 设置。")
    else:
        st.caption("为缩短启动时间，本页状态改为按需加载；需要时再点击“加载当前自动批量状态”。")
    upload_col, run_col, queue_col = st.columns(3)
    with upload_col:
        with st.form("auto_upload_form"):
            author_upload = st.file_uploader("上传作者列表 CSV（必须包含 owner_mid 列）", type=["csv"], key="auto_author_upload")
            source_name = st.text_input("作者列表来源名称", value="selected_authors")
            trigger_auto_upload = st.form_submit_button("上传 / 替换作者列表", width="stretch")
    if trigger_auto_upload:
        try:
            local_runner = _resolve_local_runner()
            if author_upload is None:
                raise ValueError("请先上传作者 CSV。")
            owner_mids = parse_owner_mid_upload(type("Upload", (), {"read": lambda self=None: author_upload.getvalue()})())
            saved_count = local_runner.replace_author_sources(
                owner_mids=owner_mids,
                source_name=source_name.strip() or "selected_authors",
                payload={"filename": author_upload.name, "uploaded_at": datetime.now().isoformat()},
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"上传作者列表失败：{exc}")
        else:
            st.success(f"已更新作者列表，共 {saved_count} 个作者。")
    with run_col:
        with st.form("auto_run_cycle_form"):
            trigger_auto_run = st.form_submit_button("手动触发一轮自动批量抓取", width="stretch")
    if trigger_auto_run:
        try:
            local_runner = _resolve_local_runner()
            result = local_runner.run_cycle()
        except Exception as exc:  # noqa: BLE001
            st.error(f"自动批量抓取失败：{exc}")
        else:
            if result.tracker_report.get("status") == "success":
                st.success("自动批量抓取本轮执行成功。")
            else:
                st.warning(f"本轮状态：{result.tracker_report.get('status')}")
            st.json(result.to_dict())
    with queue_col:
        with st.form("auto_pending_queue_form"):
            include_media_pending = st.checkbox("处理待补一次性数据时同步抓取媒体", value=True)
            pending_limit = st.number_input("本次最多处理多少条待补视频", min_value=1, max_value=10000, value=100, step=10)
            trigger_auto_queue = st.form_submit_button("抓取待补元数据 / 媒体数据", width="stretch")
    if trigger_auto_queue:
        try:
            local_runner = _resolve_local_runner()
            report = local_runner.crawl_pending_once_data(
                include_media=include_media_pending,
                limit=int(pending_limit),
                parallelism=int(auto_pending_parallelism),
                credential=active_credential,
                media_strategy=active_media_strategy,
                max_height=int(max_height),
                chunk_size_mb=int(chunk_size_mb),
            )
            if report is None:
                st.info("当前没有待补的一次性数据。")
            else:
                _show_json("待补任务报告", report.to_dict())
        except Exception as exc:  # noqa: BLE001
            st.error(f"处理待补数据失败：{exc}")

    status_load_col, status_refresh_col = st.columns(2)
    with status_load_col:
        trigger_auto_status_load = st.button("加载当前自动批量状态", width="stretch")
    with status_refresh_col:
        trigger_auto_status_refresh = st.button("刷新自动批量状态", width="stretch")

    if trigger_auto_status_load or trigger_auto_status_refresh:
        try:
            st.session_state["datahub_auto_status_snapshot"] = _load_auto_status_snapshot()
        except Exception as exc:  # noqa: BLE001
            st.warning(f"读取自动批量抓取状态失败：{exc}")

    auto_status_snapshot = st.session_state.get("datahub_auto_status_snapshot")
    if auto_status_snapshot is None:
        st.caption("当前尚未加载自动批量抓取状态。")
    else:
        metrics = auto_status_snapshot.get("metrics", {})
        st.caption(
            f"活跃作者数 {auto_status_snapshot.get('author_source_count', 0)}，"
            f"待补元数据/媒体总数 {metrics.get('meta_media_queue_total', 0)}，"
            f"仍待处理 {metrics.get('meta_media_queue_pending', 0)}。"
        )
        with st.expander("最近运行记录", expanded=True):
            st.json(auto_status_snapshot.get("recent_runs", []))
        with st.expander("当前作者源", expanded=False):
            st.dataframe(pd.DataFrame(auto_status_snapshot.get("author_source_rows", [])), width="stretch", hide_index=True)
        with st.expander("待补元数据/媒体清单", expanded=False):
            st.dataframe(pd.DataFrame(auto_status_snapshot.get("meta_media_queue_rows", [])).head(200), width="stretch", hide_index=True)

with tab_manual_batch:
    st.subheader("手动批量抓取-动态数据")
    if not active_gcp_config.is_enabled():
        st.warning("手动批量抓取-动态数据依赖 GCP 配置；请先完成侧边栏中的 BigQuery / GCS 设置。")
    st.caption(
        "本页不再维护本地作者清单。请直接选择 `full_site_floorings` 下的一个或多个视频列表 CSV，"
        "以及可选的一个或多个 `uid_expansions` 任务目录，"
        "完成追踪窗口裁剪与去重后，后台任务会在新的 `manual_crawl_stat_comment_*` 任务目录中留存待抓 CSV，再继续抓取评论/互动量数据。"
    )
    _render_background_task_status("manual_dynamic_batch", "当前后台任务状态")

    video_pool_root = _manual_stat_comment_video_pool_root()
    manual_crawls_dir = _manual_stat_comment_crawls_root()
    uid_task_dirs = _list_uid_expansion_task_dirs(video_pool_root)
    flooring_csvs = _list_manual_batch_flooring_csvs(video_pool_root)

    st.markdown("**输入来源**")
    st.caption(f"`full_site_floorings` 根目录：`{display_path(video_pool_root / 'full_site_floorings', APP_DIR)}`")
    st.caption(f"`uid_expansions` 根目录：`{display_path(video_pool_root / 'uid_expansions', APP_DIR)}`")
    st.caption(f"`manual_crawls` 输出目录：`{display_path(manual_crawls_dir, APP_DIR)}`")
    if flooring_csvs:
        st.caption(f"当前发现 {len(flooring_csvs)} 个可选 `full_site_floorings` 视频列表 CSV。")
    else:
        st.warning("尚未发现任何可选的 `full_site_floorings` 视频列表 CSV。")
    if uid_task_dirs:
        st.caption(f"当前发现 {len(uid_task_dirs)} 个可选 `uid_expansion` 任务目录。")
    else:
        st.warning("尚未发现任何 `uid_expansions` 任务目录。")

    st.markdown("**可选的 full_site_floorings 文件**")
    if flooring_csvs:
        preview_names = [display_path(path, APP_DIR) for path in flooring_csvs[:8]]
        st.caption("最近可选文件示例：" + "；".join(f"`{item}`" for item in preview_names))

    with st.form("realtime_watchlist_run_form"):
        selected_flooring_csvs = st.multiselect(
            "选择一个或多个 full_site_floorings 视频列表 CSV",
            options=[path.name for path in flooring_csvs],
            default=[],
            key="manual_stat_comment_selected_flooring_csvs",
            help="系统会读取所选 CSV 中的 `bvid` 列，并与所选 uid_expansion 任务目录中的 `videolist_part_*.csv` 合并后统一筛窗、去重。",
        )
        selected_uid_task_dirs = st.multiselect(
            "可选：选择一个或多个 uid_expansion 任务目录",
            options=[path.name for path in uid_task_dirs],
            default=[],
            key="manual_stat_comment_selected_uid_dirs",
            help="系统会读取所选任务目录下的 `videolist_part_*.csv`，并与上方所选 full_site_floorings 视频列表合并。",
        )
        time_window_hours = st.number_input(
            "追踪窗口（小时）",
            min_value=1,
            max_value=24 * 365,
            value=168,
            step=1,
            key="manual_stat_comment_time_window_hours",
        )
        parallelism = st.number_input(
            "并发度",
            min_value=1,
            max_value=16,
            value=2,
            step=1,
            key="manual_batch_parallelism",
        )
        trigger_manual_batch = st.form_submit_button("启动后台任务：手动批量抓取-动态数据", width="stretch")

    if trigger_manual_batch:
        try:
            if not active_gcp_config.is_enabled():
                raise ValueError("请先完成 GCP 配置。")
            save_cookie_text(cookie_text, path=COOKIE_FILE_PATH)
            task_dir = _launch_datahub_background_task(
                scope="manual_dynamic_batch",
                task_kind="manual_dynamic_batch",
                gcp_payload=gcp_payload,
                payload={
                    "stream_data_time_window_hours": int(time_window_hours),
                    "parallelism": int(parallelism),
                    "comment_limit": int(comment_limit_default),
                    "consecutive_failure_limit": int(consecutive_failure_limit),
                    "max_height": int(max_height),
                    "chunk_size_mb": int(chunk_size_mb),
                    "video_pool_root": str(video_pool_root),
                    "manual_crawls_root_dir": str(manual_crawls_dir),
                    "selected_flooring_csvs": list(selected_flooring_csvs),
                    "selected_uid_task_dirs": list(selected_uid_task_dirs),
                },
            )
        except ValueError as exc:
            st.error(f"手动批量抓取-动态数据失败：{exc}")
        except Exception as exc:  # noqa: BLE001
            st.error(f"手动批量抓取-动态数据失败：{exc}")
        else:
            st.success(f"后台任务已启动：`{display_path(task_dir, APP_DIR)}`")

with tab_manual_media:
    st.subheader("手动批量抓取-媒体/元数据")
    if not active_gcp_config.is_enabled():
        st.warning("本页依赖 GCP 配置；请先完成侧边栏中的 BigQuery / GCS 设置。")
    st.caption(
        "本页用于集中补抓视频元数据与媒体文件（视频轨 + 音频轨）。"
        "模式 A 基于数据集中的缺失条目维护待补清单，模式 B 基于用户手动上传的 `bvid` 清单去重后执行。"
    )
    _render_background_task_status("manual_media", "当前后台任务状态")
    manual_media_waitlist_path = build_manual_media_waitlist_path(
        active_gcp_config.bigquery_dataset,
        manual_crawls_root_dir=MANUAL_MEDIA_CRAWLS_OUTPUT_DIR,
    )
    mode_a_tab, mode_b_tab = st.tabs(["模式A:基于数据集缺失条目", "模式B:基于手动上传清单"])

    with mode_a_tab:
        st.caption(
            f"当前维护中的待补媒体/元数据清单：`{manual_media_waitlist_path.as_posix()}`；"
            f"任务目录根路径：`{MANUAL_MEDIA_CRAWLS_OUTPUT_DIR.as_posix()}`"
        )
        sync_col, run_col = st.columns(2)
        with sync_col:
            with st.form("manual_media_mode_a_sync_form"):
                trigger_manual_media_sync = st.form_submit_button("同步待补媒体/元数据视频清单", width="stretch")
        with run_col:
            with st.form("manual_media_mode_a_run_form"):
                manual_media_mode_a_parallelism = st.number_input(
                    "并发度",
                    min_value=1,
                    max_value=16,
                    value=1,
                    step=1,
                    key="manual_media_mode_a_parallelism",
                )
                manual_media_mode_a_enable_sleep = st.checkbox("启用睡眠机制", value=False, key="manual_media_mode_a_enable_sleep")
                manual_media_mode_a_sleep_minutes = st.number_input(
                    "睡眠时长（分钟）",
                    min_value=1,
                    max_value=240,
                    value=5,
                    step=1,
                    key="manual_media_mode_a_sleep_minutes",
                )
                trigger_manual_media_mode_a = st.form_submit_button("启动后台任务：按清单抓取媒体/元数据", width="stretch")
        if trigger_manual_media_sync:
            try:
                store = _resolve_store()
                sync_result = sync_manual_media_waitlist(
                    store=store,
                    gcp_config=active_gcp_config,
                    manual_crawls_root_dir=MANUAL_MEDIA_CRAWLS_OUTPUT_DIR,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"同步待补媒体/元数据清单失败：{exc}")
            else:
                st.success(f"待补清单同步完成，当前共 {sync_result.pending_count} 条。")
                _show_json("待补媒体/元数据清单同步结果", sync_result.to_dict())
        current_waitlist_df = _load_saved_dataframe(str(manual_media_waitlist_path))
        with st.expander("查看当前待补媒体/元数据清单", expanded=False):
            if current_waitlist_df is None or current_waitlist_df.empty:
                st.info("当前尚未生成待补清单，或清单内容为空。")
            else:
                st.dataframe(current_waitlist_df.head(200), width="stretch", hide_index=True)
        if trigger_manual_media_mode_a:
            try:
                if not active_gcp_config.is_enabled():
                    raise ValueError("请先完成 GCP 配置。")
                save_cookie_text(cookie_text, path=COOKIE_FILE_PATH)
                task_dir = _launch_datahub_background_task(
                    scope="manual_media",
                    task_kind="manual_media_mode_a",
                    gcp_payload=gcp_payload,
                    payload={
                        "manual_crawls_root_dir": str(MANUAL_MEDIA_CRAWLS_OUTPUT_DIR),
                        "enable_sleep_resume": bool(manual_media_mode_a_enable_sleep),
                        "sleep_minutes": int(manual_media_mode_a_sleep_minutes),
                        "parallelism": int(manual_media_mode_a_parallelism),
                        "comment_limit": int(comment_limit_default),
                        "consecutive_failure_limit": int(consecutive_failure_limit),
                        "max_height": int(max_height),
                        "chunk_size_mb": int(chunk_size_mb),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"模式 A 抓取失败：{exc}")
            else:
                st.success(f"模式 A 后台任务已启动：`{display_path(task_dir, APP_DIR)}`")

    with mode_b_tab:
        manual_media_mode_b_uploads = st.file_uploader(
            "上传一份或多份包含 `bvid` 列的 CSV/XLSX 清单",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="manual_media_mode_b_uploads",
        )
        if manual_media_mode_b_uploads:
            st.caption("当前已上传：" + "，".join(upload.name for upload in manual_media_mode_b_uploads))
        with st.form("manual_media_mode_b_form"):
            manual_media_mode_b_parallelism = st.number_input(
                "并发度",
                min_value=1,
                max_value=16,
                value=1,
                step=1,
                key="manual_media_mode_b_parallelism",
            )
            manual_media_mode_b_enable_sleep = st.checkbox("启用睡眠机制", value=False, key="manual_media_mode_b_enable_sleep")
            manual_media_mode_b_sleep_minutes = st.number_input(
                "睡眠时长（分钟）",
                min_value=1,
                max_value=240,
                value=5,
                step=1,
                key="manual_media_mode_b_sleep_minutes",
            )
            trigger_manual_media_mode_b = st.form_submit_button("启动后台任务：去重并抓取", width="stretch")
        if trigger_manual_media_mode_b:
            try:
                if not manual_media_mode_b_uploads:
                    raise ValueError("请先上传至少一份包含 `bvid` 列的清单。")
                if not active_gcp_config.is_enabled():
                    raise ValueError("请先完成 GCP 配置。")
                if background_task_is_running("manual_media", registry_root=BACKGROUND_TASKS_ROOT):
                    raise ValueError("当前已有媒体/元数据后台任务仍在运行，请先等待其完成。")
                save_cookie_text(cookie_text, path=COOKIE_FILE_PATH)
                task_dir = create_background_task_dir("manual_media_mode_b", root_dir=BACKGROUND_TASKS_ROOT)
                upload_dir = task_dir / "uploads"
                upload_dir.mkdir(parents=True, exist_ok=True)
                uploaded_file_paths: list[str] = []
                uploaded_names: list[str] = []
                for index, upload in enumerate(manual_media_mode_b_uploads, start=1):
                    target_path = upload_dir / f"{index:02d}_{upload.name}"
                    target_path.write_bytes(upload.getbuffer())
                    uploaded_file_paths.append(str(target_path))
                    uploaded_names.append(upload.name)
                write_background_task_config(
                    task_dir,
                    {
                        "scope": "manual_media",
                        "task_kind": "manual_media_mode_b",
                        "task_dir": str(task_dir),
                        "gcp_payload": gcp_payload,
                        "cookie_path": str(COOKIE_FILE_PATH),
                        "cookie_refresh_batch_size": 100,
                        "payload": {
                            "manual_crawls_root_dir": str(MANUAL_MEDIA_CRAWLS_OUTPUT_DIR),
                            "enable_sleep_resume": bool(manual_media_mode_b_enable_sleep),
                            "sleep_minutes": int(manual_media_mode_b_sleep_minutes),
                            "parallelism": int(manual_media_mode_b_parallelism),
                            "comment_limit": int(comment_limit_default),
                            "consecutive_failure_limit": int(consecutive_failure_limit),
                            "max_height": int(max_height),
                            "chunk_size_mb": int(chunk_size_mb),
                            "uploaded_file_paths": uploaded_file_paths,
                            "uploaded_names": uploaded_names,
                        },
                    },
                )
                update_background_task_status(
                    task_dir,
                    {
                        "status": "queued",
                        "scope": "manual_media",
                        "task_kind": "manual_media_mode_b",
                        "task_dir": str(task_dir),
                        "created_at": datetime.now().isoformat(),
                    },
                )
                pid = launch_background_worker(task_dir)
                register_active_background_task("manual_media", task_dir=task_dir, registry_root=BACKGROUND_TASKS_ROOT, pid=pid)
                update_background_task_status(
                    task_dir,
                    {
                        "status": "queued",
                        "scope": "manual_media",
                        "task_kind": "manual_media_mode_b",
                        "task_dir": str(task_dir),
                        "created_at": datetime.now().isoformat(),
                        "pid": pid,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"模式 B 抓取失败：{exc}")
            else:
                st.success(f"模式 B 后台任务已启动：`{display_path(task_dir, APP_DIR)}`")

with tab_db:
    st.subheader("BigQuery / GCS 数据查看")
    if not active_gcp_config.is_enabled():
        st.warning("本页依赖 GCP 配置；请先完成侧边栏中的 BigQuery / GCS 设置。")
    db_view_col, asset_view_col = st.columns(2)
    with db_view_col:
        with st.form("db_view_structured_form"):
            inspect_bvid_structured = st.text_input("输入要查看的 bvid", value="", key="inspect_bvid_structured")
            trigger_db_view = st.form_submit_button("查看该 bvid 的结构化数据", width="stretch")
    if trigger_db_view:
        try:
            store = _resolve_store()
            video_row = store.fetch_video_row(inspect_bvid_structured.strip())
            stat_row = store.fetch_latest_stat_snapshot_row(inspect_bvid_structured.strip())
            comment_row = store.fetch_latest_comment_snapshot_row(inspect_bvid_structured.strip())
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取结构化数据失败：{exc}")
        else:
            if video_row is not None:
                st.json(video_row)
            if stat_row is not None:
                st.json(stat_row)
            if comment_row is not None:
                st.json(comment_row)
    with asset_view_col:
        with st.form("db_view_assets_form"):
            inspect_bvid_assets = st.text_input("输入要查看媒体资产的 bvid", value="", key="inspect_bvid_assets")
            trigger_asset_view = st.form_submit_button("查看该 bvid 的媒体资产", width="stretch")
    if trigger_asset_view:
        try:
            store = _resolve_store()
            asset_rows = store.fetch_all_asset_rows(inspect_bvid_assets.strip())
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取媒体资产失败：{exc}")
        else:
            st.dataframe(pd.DataFrame(asset_rows), width="stretch", hide_index=True)
            with st.expander("导出该 bvid 的媒体文件为本地文件", expanded=False):
                video_row = next((row for row in asset_rows if row.get("asset_type") == "video"), None)
                audio_row = next((row for row in asset_rows if row.get("asset_type") == "audio"), None)
                if video_row:
                    video_bytes = _load_asset_bytes(asset_row=video_row, strategy=active_media_strategy)
                    st.download_button("下载视频文件（.m4s）", data=video_bytes, file_name=f"{inspect_bvid_assets.strip()}_video.m4s", mime="video/mp4")
                if audio_row:
                    audio_bytes = _load_asset_bytes(asset_row=audio_row, strategy=active_media_strategy)
                    st.download_button("下载音频文件（.m4s）", data=audio_bytes, file_name=f"{inspect_bvid_assets.strip()}_audio.m4s", mime="audio/mp4")

with tab_quick_jump:
    st.subheader("快捷跳转")
    video_col, owner_col = st.columns(2)
    with video_col:
        with st.form("quick_jump_video_form"):
            quick_jump_bvid = st.text_input("视频 BVID", value="", key="quick_jump_bvid")
            trigger_jump_video = st.form_submit_button("跳转到视频页", width="stretch")
        if trigger_jump_video:
            normalized_bvid = normalize_bvid(quick_jump_bvid)
            if normalized_bvid is None:
                st.warning("请输入有效的 BVID。")
            else:
                video_url = build_video_url(normalized_bvid)
                if open_in_default_browser(video_url):
                    st.success(f"已打开视频页：{video_url}")
    with owner_col:
        with st.form("quick_jump_owner_form"):
            quick_jump_owner_mid = st.text_input("作者 ID", value="", key="quick_jump_owner_mid")
            trigger_jump_owner = st.form_submit_button("跳转到作者主页", width="stretch")
        if trigger_jump_owner:
            normalized_owner_mid = normalize_owner_mid(quick_jump_owner_mid)
            if normalized_owner_mid is None:
                st.warning("请输入有效的作者 ID。")
            else:
                owner_url = build_owner_space_url(normalized_owner_mid)
                if open_in_default_browser(owner_url):
                    st.success(f"已打开作者主页：{owner_url}")

with tab_tid:
    from bili_pipeline.bilibili_zones import find_by_tid, list_zones, unique_mains

    st.subheader("tid 与分区名称对应")
    zones = list(list_zones())
    mains = ["全部"] + unique_mains(zones)
    with st.form("tid_lookup_form"):
        col_a, col_b = st.columns([1, 2])
        with col_a:
            q_tid = st.number_input("按 tid 精确查询", min_value=1, max_value=999999, value=17, step=1)
        with col_b:
            q_text = st.text_input("关键字搜索（主分区/名称/代号）", value="")
        sel_main = st.selectbox("按主分区筛选", options=mains, index=0)
        trigger_tid_query = st.form_submit_button("应用筛选")
    if not st.session_state.get("tid_lookup_initialized"):
        st.session_state["tid_lookup_initialized"] = True
    matches = find_by_tid(int(q_tid))
    if matches:
        st.dataframe([{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in matches], width="stretch", hide_index=True)

    def _hit(z) -> bool:
        if sel_main != "全部" and z.main != sel_main:
            return False
        if not q_text.strip():
            return True
        target = q_text.strip().lower()
        return target in z.main.lower() or target in z.name.lower() or target in z.code.lower()

    filtered = [z for z in zones if _hit(z)]
    st.dataframe([{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in filtered], width="stretch", hide_index=True)
