# 抓取逻辑与调度频率（v1）

本文档把“采集逻辑”写成可直接拆成任务队列的步骤，并明确四类数据的**变量名**、**接口输入输出**与**抓取频率**。你当前的策略是：**对整个 `video_pool` 都采集 2 小时级流行度时间序列**，而元数据、评论快照与多模态数据则按 `bvid` 提供统一接口按需抓取。

---

## 1. 两类运行：Discover Run vs Crawl Run

### 1.1 Discover Run（建池）

- 输入：建池配置（榜单/分区/搜索/作者扩展参数）
- 输出：`video_pool`
- 频率建议：每天 1 次（或每 6–12 小时 1 次，取决于热点敏感度）

### 1.2 Crawl Run（采集）

对已存在的 `video_pool` 做增量采集。

- 日更采集（全量）：统计快照 + 必要元数据补齐
- 2 小时采集（全量）：统计快照 + 前 n 条评论快照

---

## 2. 四类数据与接口定义

### 2.1 流行度时间序列数据接口

接口名建议：`crawl_stat_snapshot(bvid: str) -> StatSnapshot`

**输入**

- `bvid`

**输出变量名**

- `bvid`
- `snapshot_time`
- `stat_view`
- `stat_like`
- `stat_coin`
- `stat_favorite`
- `stat_share`
- `stat_reply`
- `stat_danmu`

**抓取逻辑**

- 对 `video_pool` 中每个 `bvid`，**每 2 小时抓取一次**
- 每次抓取时只记录当前累计值快照，不在采集阶段计算增量
- 后续增量由分析层计算：
  - `delta_view`
  - `delta_like`
  - `delta_coin`
  - `delta_favorite`
  - `delta_share`
  - `delta_reply`
  - `delta_danmu`

### 2.2 元数据接口

接口名建议：`crawl_video_meta(bvid: str) -> MetaResult`

**输入**

- `bvid`

**输出变量名**

- `bvid`
- `title`
- `desc`
- `tags`
- `owner_name`
- `owner_follower_count`
- `owner_video_count`
- `is_activity_participant`
- `pubdate`

**抓取逻辑**

- 该接口是**一次性静态抓取接口**
- 输入 `video_pool` 中某个 `bvid` 后，返回该视频当前的元数据结果
- 默认在视频首次进入采集系统时调用 1 次
- 若后续你希望支持元数据回补，可再增加 `force_refresh` 开关，但第一版先不做

### 2.3 评论接口

接口名建议：`crawl_latest_comments(bvid: str, limit: int = 10) -> CommentLatest10Result`

**输入**

- `bvid`
- `limit=10`

**输出变量名**

- `bvid`
- `snapshot_time`
- `comment_top10`

其中 `comment_top10` 为列表，每个元素包含：

- `rpid`
- `message`
- `like`
- `ctime`
- `mid`
- `uname`

**抓取逻辑**

- 评论数据**单独设置接口**
- 当运行该接口时，抓取该视频**实时的前 10 条最新评论**
- “前 10 条”在这里固定定义为：**按时间序排列的最新评论前 10 条**
- 返回的是某个时刻的快照，不保证未来再次抓取结果一致

### 2.4 多模态数据接口

接口名建议：`crawl_media_assets(bvid: str) -> MediaResult`

**输入**

- `bvid`

**输出变量名**

- `bvid`
- `video_format_selected`
- `audio_format_selected`
- `video_local_path`
- `audio_local_path`
- `video_object_key`
- `audio_object_key`

**抓取逻辑**

- 该接口是**统一多模态抓取接口**
- 输入 `video_pool` 中某个 `bvid` 后，调用 `yt-dlp` 获取该视频对应的完整视频与音频
- 视频分辨率策略固定为：
  - **默认优先 1080p**
  - 若不支持 1080p，则自动降级到该视频可用的最高分辨率
- 音频策略固定为：
  - 下载与所选视频匹配的最佳可用音频流
- 第一版只要求接口返回下载结果与对应引用，不要求额外做复杂转码

---

## 3. 采集总顺序（单视频视角）

对每个 `bvid`，建议固定顺序如下（便于失败回退与断点续跑）：

1. **Resolve 阶段**：把 `bvid` 解析成抓取需要的关键键（如 `aid/cid/pages_info`）  
2. **Meta 阶段**：调用 `crawl_video_meta(bvid)`，返回一次性元数据结果  
3. **Stat 阶段**：调用 `crawl_stat_snapshot(bvid)`，写入时间序列快照  
4. **CommentSnap 阶段**：调用 `crawl_latest_comments(bvid, limit=10)`，抓最新评论前 10 条快照  
5. **Media 阶段**：下载视频与音频（`yt-dlp` 主，失败回退 `BBDown`）  
6. **Upload 阶段**：上传大文件到对象存储，写入 object_key/ref  
7. **Cleanup 阶段**：删除本地临时文件（遵循“临时下载 -> 上传 -> 删除”）

> 原则：越靠前越轻量，越靠后越昂贵；让失败尽量在前面暴露，降低下载浪费。

---

## 4. 调度频率设计

### 3.1 长期（日更）面板：全量 `video_pool`

**目标**：稳定获得每个视频的长期累计值面板数据，用于后续你提出的新流行度定义。

- 频率：每天 1 次
- 任务：可选元数据补齐 + `crawl_stat_snapshot(bvid)`
- 评论接口：不在日更任务里默认执行
- 多模态接口：只在首次抓取或回补时调用 `crawl_media_assets(bvid)`

### 3.2 短期（2 小时）热度趋势：全量 `video_pool`

**目标**：捕捉日内热度变化与短期爆发，支撑“短期热度趋势两小时一次”的设计。

- 频率：每 2 小时 1 次
- 任务：`crawl_stat_snapshot(bvid)` + `crawl_latest_comments(bvid, limit=10)`
- 元数据：通常不需要每 2 小时刷新（除非你关心标题/简介变更）；建议日更即可
- 规模：默认对 `video_pool` 全量执行（成本与风控风险显著更高，建议预留可降级开关：按来源/分区/作者配额抽样，或仅对新增入池后前 7 天做 2 小时级）

---

## 5. 时间序列字段与“快照定义”

### 4.1 StatSnapshot（累计值快照）

每条记录必须包含：

- `bvid`
- `snapshot_time`（抓取时间）
- `stat_view`
- `stat_like`
- `stat_coin`
- `stat_favorite`
- `stat_share`
- `stat_reply`
- `stat_danmu`
- `source_pool`：`video_pool`

衍生指标在后处理计算（不要在抓取阶段强绑定定义）：

- 增量：\( \Delta x_t = x_t - x_{t-1} \)
- 增长率：\( g_t = \\frac{x_t - x_{t-1}}{\\max(1, x_{t-1})} \)
- 加速度：\( a_t = (x_t-x_{t-1})-(x_{t-1}-x_{t-2}) \)

### 4.2 CommentLatest10Result（最新评论前 10 条）

必须把它定义为“**在某个时刻、按时间序、抽取到的最新评论前 10 条集合**”，而不是“永久固定的前 10 条”。

每条快照建议包含：

- `bvid`
- `snapshot_time`
- `limit=10`
- `comment_top10[]`：每项包含 `rpid/message/like/ctime/mid/uname`

> 注意：评论删除/折叠/审核会导致快照漂移，这是预期现象，应在论文方法里说明。

---

## 6. 多模态下载与流式存储（视频+音频）

### 6.1 下载时机

- **首次入池后**：对每个 `bvid` 只下载一次（避免重复占用带宽与存储）
- **回补/重试**：只对 `MediaDownload` 失败或资产缺失的样本再跑

### 6.2 下载策略（`yt-dlp` 主，`BBDown` 回退）

- `yt-dlp`：
  - 默认格式策略为：**优先 1080p 视频 + 最佳可用音频**
  - 若视频不支持 1080p，则自动降级为该视频当前可获取的最高分辨率
  - 若高画质依赖登录，使用 cookies（例如从浏览器导出）
- `BBDown`（回退）：
  - 当 `yt-dlp` 失败或画质受限时，用 BBDown 再尝试
  - 仍遵循“下载 -> 上传对象存储 -> 删除本地缓存”

### 6.3 资产命名与引用

- 资产不建议用标题命名（易变、含非法字符），建议以 `bvid/cid` 为主键构造 object_key
- 结构化表只存：
  - `video_object_key`
  - `audio_object_key`
  - `video_format_selected`
  - `audio_format_selected`
  - `file_size/mime_type/sha256`（如可算）

---

## 7. 失败处理与断点续跑

为保证工程可用性，建议每个阶段都具备：

- **幂等性**：同一 `bvid + stage + version` 重跑不应产生冲突
- **失败记录**：记录 `error_type/error_message/http_status/retry_count`
- **回退**：
  - `Meta/Stat` 失败：可稍后重试（轻量）
  - `Media` 失败：先换下载器/换格式上限/换 cookies，再重试

---

## 8. 与后续“新流行度定义”的接口

本阶段只负责把“足够干净的面板数据 + 最新评论快照 + 多模态资产引用”存下来。

后续你提出的新流行度定义可以在以下三类输入上构建：

- **累计值面板**：播放、点赞、评论、弹幕等的短期与长期变化
- **评论快照语义**：最新评论前 10 条文本、点赞数随时间变化
- **多模态特征**：视频/音频的内容特征（可在特征工程阶段提取）

---

## 9. 单 bvid 全流程与 bvid 列表接口

本节给出三个“工程入口级接口”：单 `bvid` 顺序爬取四类数据、大文件流式存储、`bvid` 列表批量爬取。

### 9.1 单 bvid 全流程接口（对应“接口 1”）

接口名建议：`crawl_full_video_bundle(bvid: str, *, enable_media: bool = True, comment_limit: int = 10) -> FullCrawlSummary`

**输入**

- `bvid`
- `enable_media`：是否在本次流程中执行多模态下载（默认为 `True`，便于按需跳过昂贵阶段）
- `comment_limit`：评论快照条数，默认 `10`，对齐 `crawl_latest_comments` 的 `limit`

**内部执行顺序（与第 3 节一致）**

1. Resolve 阶段：解析 `aid/cid/pages_info` 等前置键  
2. Meta 阶段：`crawl_video_meta(bvid)`  
3. Stat 阶段：`crawl_stat_snapshot(bvid)`  
4. CommentSnap 阶段：`crawl_latest_comments(bvid, limit=comment_limit)`  
5. Media 阶段（可选）：`crawl_media_assets(bvid)` 或更细粒度的 `stream_media_to_store`  
6. Upload 阶段：写入对象存储引用到结构化表  
7. Cleanup 阶段：删除本地临时文件与上传会话中间状态  

**输出 `FullCrawlSummary` 建议内容**

- `bvid`
- `meta_ok` / `stat_ok` / `comment_ok` / `media_ok`
- `snapshot_time`
- `errors[]`：如有失败，记录对应 stage 与错误信息

> 工程上建议为每个 stage 单独落行或落表，`FullCrawlSummary` 只作为“本次调用完成度摘要”，方便前端展示。

---

### 9.2 流式多模态存储接口（对应“接口 2”）

接口名建议：`stream_media_to_store(bvid: str, strategy: MediaDownloadStrategy) -> MediaResult`

**职责边界**

- 只负责 **多模态数据的下载 + 上传 + 清理**，不关心元数据/统计/评论。
- 以 `bvid` 为入口，内部解析到 `cid/pages` 后，按 `strategy` 逐个资产执行下载与流式上传。

**与第 6 节的关系**

- `strategy` 明确：
  - 分辨率上限（如 `max_height=1080`）
  - 音频编码/比特率偏好
  - 本地缓存上限与分块大小
  - 对象存储 bucket/prefix 与访问策略
- 返回的 `MediaResult` 补齐：
  - `video_object_key` / `audio_object_key`
  - `file_size` / `mime_type` / `sha256`
  - `upload_session_id`（若底层使用多段上传）

**流式存储关键点**

- 下载与对象存储多段上传管线化（chunk 写入 → 立即上传 → 成功后删除对应 chunk）
- 控制单个本地临时文件的大小上限，避免长视频长时间占用磁盘
- `MediaResult` 中记录分块元数据，支撑断点续传与幂等

---

### 9.3 bvid 列表批量接口（对应“接口 3”）

接口名建议：`crawl_bvid_list_from_csv(csv_path: Path | str, *, parallelism: int = 4, enable_media: bool = True, comment_limit: int = 10) -> BatchCrawlReport`

**输入**

- `csv_path`：包含 `bvid` 列的 CSV 文件路径（通常来自 `Bilibili_Video_Pool_Builder` 导出的 `video_pool`）
- `parallelism`：并发度（同时进行的 `bvid` 流程数，用于限流和防风控）
- `enable_media` / `comment_limit`：透传给 `crawl_full_video_bundle`

**核心逻辑**

- 读取 CSV，抽取并去重 `bvid` 列表
- 按 `parallelism` 限流，逐个调用 `crawl_full_video_bundle`
- 支持幂等与断点续跑：
  - 已有完整记录的 `bvid` 可按配置跳过或仅做回补（例如只补多模态）
  - 每次调用的 `FullCrawlSummary` 写入 `crawl_runs`，形成 `BatchCrawlReport`

**输出 `BatchCrawlReport` 建议内容**

- `run_id`
- `total_bvids`
- `success_count`
- `failed_count`
- `started_at` / `finished_at`
- `per_bvid_summaries[]`（可选，按需查询）

> 前端应用 Bilibili Video Data Crawler 在“单 bvid 一键抓取”和“bvid 列表批量抓取”菜单下应直接对接本节定义的接口。