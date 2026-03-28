# 后续实现范围：模块边界、输入输出、关键数据表（v1）

本文档用于把后续“可运行骨架”拆成清晰模块，保证采集、存储、监控、特征演化可视化解耦，并支持断点续跑与回退。

---

## 1. 建议目录结构（Python 包级别）

> 仅定义模块边界，不强行绑定具体框架。

- `src/bili_pipeline/`
  - `config/`：配置加载（env + yaml/json）
  - `models/`：pydantic/dataclass 数据模型（VideoMeta/CommentItem/…）
  - `discover/`：建池（榜单/分区/作者扩展/搜索补样）
  - `collect/`
    - `meta_collector.py`
    - `stat_collector.py`
    - `comment_collector.py`
    - `danmaku_collector.py`（预留）
  - `media/`
    - `ytdlp_downloader.py`
    - `bbdown_downloader.py`（回退）
  - `storage/`
    - `structured_store.py`（DuckDB/Parquet）
    - `object_store.py`（S3 兼容）
    - `refs.py`（object_key/ref 规范）
  - `scheduler/`：任务队列/调度（两小时 vs 日更）
  - `monitoring/`：指标计算（run-level、dataset-level、field-level、modal-level）
  - `lineage/`：FeatureLineageStage 记录（核心）
  - `utils/`：重试、限速、日志、幂等、hash 等

---

## 2. 输入输出契约（每个模块最小 I/O）

### 2.1 Discover（建池）

- 输入：`DiscoverConfig`（榜单入口、分区 tid 白名单、作者扩展预算、focus 配额等）
- 输出：
  - `video_pool`（`bvid` 列表 + 来源/优先级）
  - `focus_pool`（`bvid` 子集 + 选择理由/配额信息）
- 关键约束：可复现（参数固化到 `run_id` 元数据）

### 2.2 MetaCollector（元数据）

- 输入：`bvid`
- 输出：`VideoMeta`（你在 `todo-v1.md` 中列出的主表字段 + `cid/pages_info`）
- 失败策略：轻量重试；不可访问则标记 `availability=restricted/deleted`

### 2.3 StatCollector（统计快照）

- 输入：`bvid`
- 输出：`VideoStatSnapshot`（带 `snapshot_time` 的累计值快照）
- 失败策略：轻量重试；允许缺失字段（用质量指标统计缺失率）

### 2.4 CommentCollector（评论快照）

- 输入：`bvid, sort_mode, n`
- 输出：
  - `TopNCommentSnapshot`（整体快照元数据）
  - `TopNCommentItem[]`（每条评论的最小字段）
- 关键约束：快照必须记录 `sort_mode`；不要把“前 n 条”解释成永久不变的真值

### 2.5 MediaDownloader（多模态）

- 输入：`bvid` 或 `url` + 下载策略（分辨率上限/格式策略/cookies）
- 输出：
  - `VideoAssetRef`（object_key、sha256、size、mime、duration 等）
  - `AudioAssetRef`（同上）
- 执行策略：`yt-dlp` 主；失败回退 `BBDown`
- 关键约束：严格遵循“临时下载 -> 上传 -> 删除本地缓存”

### 2.6 CrawlOrchestrator（工程入口级接口）

在 collector/downloader 之上，定义一层“编排接口”，用于承接前端应用与批量任务。该层不直接依赖具体 HTTP 实现，只负责：

- 组装输入（单个 `bvid` 或 `bvid` 列表）
- 调用下游 `MetaCollector/StatCollector/CommentCollector/MediaDownloader`
- 写入 `crawl_runs/feature_lineage_stages` 等运行元数据

建议接口与 `crawl-logic-v1.md` 的第 9 节保持一致：

- `crawl_full_video_bundle(bvid: str, *, enable_media: bool = True, comment_limit: int = 10) -> FullCrawlSummary`
- `stream_media_to_store(bvid: str, strategy: MediaDownloadStrategy) -> MediaResult`
- `crawl_bvid_list_from_csv(csv_path: Path | str, *, parallelism: int = 4, enable_media: bool = True, comment_limit: int = 10) -> BatchCrawlReport`

---

## 3. 关键数据表（建议）

### 3.1 `videos`（视频主表）

- 主键：`bvid`
- 字段：对齐 `todo-v1.md` 的 `VideoMeta` 字段（含作者与分区、封面、tags、pages_info、crawl_time、source_*）

### 3.2 `authors`（作者表）

- 主键：`owner_mid`
- 字段：`owner_name`、`follower_count`、`video_count`、作者侧统计等
- 备注：作者字段可能随时间变化，建议允许写入快照表（见下）

### 3.3 `video_stat_snapshots`（统计快照表）

- 主键建议：`(bvid, snapshot_time)`
- 字段：`stat_view/stat_like/...` + `snapshot_time` + `source_pool`

### 3.4 `topn_comment_snapshots`（评论快照头表）

- 主键建议：`(bvid, snapshot_time, sort_mode, n)`
- 字段：快照参数 + 抓取状态 + `items_count`

### 3.5 `topn_comment_items`（评论快照明细表）

- 主键建议：`(bvid, snapshot_time, sort_mode, n, rpid)`
- 字段：`rpid/message/like/ctime/mid/uname` + `crawl_time`

### 3.6 `assets`（对象存储引用表）

- 主键建议：`(bvid, cid, asset_type)`，其中 `asset_type` in `video/audio/subtitle/cover/danmaku`
- 字段：`object_key`、`sha256`、`file_size`、`mime_type`、`uploaded_at`、`source_downloader`

### 3.7 `crawl_runs`（运行表）

- 主键：`run_id`
- 字段：`start_time/end_time`、成功失败数、重试次数、notes、建池参数 JSON

### 3.8 `field_quality_metrics` / `modal_progress_metrics`

- 对齐 `todo-v1.md` 的指标要求，用于看板与数据质量追踪

### 3.9 `feature_lineage_stages`（核心）

完全对齐 `todo-v1.md` 中 `FeatureLineageStage` 的字段要求，用于后续特征工程可视化。

---

## 4. 版本与幂等

- **数据版本**：每个 collector/downloader 都要有 `processor_name` + `processor_version`，写入 lineage 与 run metadata
- **幂等键**：对快照类表，幂等键包含时间戳；对资产类表，幂等键包含 `(bvid,cid,asset_type)`
- **回补策略**：允许“只回补缺失模态/缺失字段”，不要每次全量重跑

---

## 5. 与“新流行度定义”的衔接点

后续你提出的新流行度定义，建议至少支持三类输入特征：

- **面板统计特征**：多指标的短期/长期增量、增长率、加速度、峰值与衰减
- **评论可见性特征**：TopN 评论文本语义（主题/情感）与点赞强度的时间变化
- **内容模态特征**：视频/音频内容特征（在特征工程阶段提取并写入 lineage）

