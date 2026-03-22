# 项目名称
Bilibili 多模态视频数据采集、存储、监控与特征演化可视化系统

由外部Agent生成，实践时请参照其他结构化文档。

---

# 一、项目目标

请帮我使用 Python 构建一个可维护、可扩展的 Bilibili 数据采集与分析项目，主要服务于我的本科毕业论文研究。系统需要围绕 B 站视频构建一条完整的数据管线，实现以下目标：

1. 基于 `bilibili-api` 获取 B 站视频相关信息。
2. 支持采集多模态原始数据，包括但不限于：
   - 视频元数据
   - 用户统计数据
   - 评论数据
   - 弹幕数据（预留）
   - 字幕数据
   - 音频文件
   - 视频封面
   - 视频标签、分区、发布时间等辅助信息
3. 支持将结构化数据与大文件分开存储：
   - 结构化数据存储到 `Parquet + DuckDB`
   - 音频、封面、字幕 JSON 等文件上传到对象存储（S3 兼容）
4. 支持构建一个监控与分析看板，实时展示：
   - 数据条目数
   - 各字段缺失率
   - 各模态采集成功率
   - 任务运行状态
   - 数据从 raw data 到特征 token 的演化过程
5. 支持后续扩展到特征工程与建模阶段：
   - 文本情感特征
   - 主题特征
   - 音频特征
   - 图像特征
   - 多模态特征融合
6. 项目必须保持模块化，禁止把所有逻辑写在一个脚本里。

---

# 二、核心设计原则

请严格遵循以下原则：

1. **采集、清洗、存储、监控、特征提取必须解耦**
2. **结构化数据与二进制大文件必须分离**
3. **原始数据层、清洗数据层、特征层必须分层管理**
4. **每次运行必须生成唯一 run_id**
5. **必须支持断点续跑**
6. **必须支持失败任务记录与重试**
7. **必须预留后续模型训练接口**
8. **优先考虑可维护性、可扩展性、日志可读性**
9. **尽量使用类型标注、dataclass 或 pydantic model**
10. **任何外部平台（飞书、对象存储）都必须做成可替换的 backend**

---

# 三、功能范围

## 3.1 数据采集功能

系统需要支持以下采集能力：

### A. 视频元数据采集
字段至少包括：
- bvid
- aid
- cid
- title
- desc
- owner_mid
- owner_name
- pubdate
- duration
- tname
- tid
- stat_view
- stat_like
- stat_coin
- stat_favorite
- stat_share
- stat_reply
- stat_danmaku
- stat_vt
- tags
- cover_url
- pages_info
- url（如果可获取则保留）
- crawl_time
- source_keyword / source_uid / source_list_id

### B. 评论采集
字段至少包括：
- rpid
- bvid
- oid / aid
- mid
- uname
- sex（如可获取）
- level_info（如可获取）
- message
- like
- ctime
- reply_count
- parent_id
- root_id
- dialog
- ip_location（如可获取）
- crawl_time

### C. 字幕采集
字段至少包括：
- bvid
- cid
- subtitle_lang
- subtitle_url
- subtitle_json_object_key
- segment_count（如可解析）
- crawl_time

### D. 音频采集
字段至少包括：
- bvid
- cid
- audio_download_url
- audio_object_key
- file_size
- mime_type
- sha256
- duration
- upload_time

### E. 封面采集
字段至少包括：
- bvid
- cover_url
- cover_object_key
- file_size
- mime_type
- sha256
- upload_time

### F. 弹幕采集（预留接口）
先预留数据模型、抓取函数和 sink 接口，不要求第一版完全实现。

---

## 3.2 数据质量监控功能

系统必须自动生成以下监控指标：

### 运行级指标
- run_id
- start_time
- end_time
- duration_seconds
- total_tasks
- success_tasks
- failed_tasks
- skipped_tasks
- success_rate
- avg_latency
- retry_count
- notes

### 数据集级指标
- dataset_name
- total_rows
- new_rows
- duplicate_rows
- null_rows
- updated_at

### 字段级质量指标
对每张表中的每个字段计算：
- field_name
- total_rows
- missing_rows
- missing_ratio
- distinct_count
- sample_value
- zero_ratio（数值字段可选）
- empty_string_ratio（字符串字段可选）
- updated_at

### 模态级指标
- modal_type（metadata / comment / subtitle / audio / image / danmaku）
- discovered_count
- fetched_count
- stored_count
- failed_count
- completion_rate
- updated_at

---

## 3.3 特征工程与特征演化可视化支持

这是一个重点需求。

系统除了保存 raw data 以外，还需要能够保存“从原始模态到最终特征 token 的逐步演化信息”，以供可视化看板展示。

请为每一种模态设计统一的“演化阶段记录表”。

### 统一要求
每个模态的数据处理过程都要被拆成多个 stage，例如：

- raw
- cleaned
- segmented / chunked
- transformed
- embedded
- tokenized
- pooled / aggregated
- final_feature

系统需要记录每个 stage 的：
- stage_id
- run_id
- bvid
- cid（如适用）
- modal_type
- stage_name
- input_ref
- output_ref
- input_shape
- output_shape
- processor_name
- processor_version
- preview_text / preview_value
- metadata_json
- created_at

### 文本模态演化示例
评论文本：
- raw comment text
- cleaned text
- tokenized words
- stopword removed text
- topic distribution
- sentiment score
- embedding vector
- final text feature token

字幕文本：
- raw subtitle json
- merged transcript
- cleaned transcript
- chunked transcript
- embedding
- final subtitle token sequence

### 图像模态演化示例
封面图：
- raw image
- resized image
- normalized tensor
- vision encoder embedding
- projected token
- final image feature token

### 音频模态演化示例
音频：
- raw audio file
- resampled waveform
- mel spectrogram
- frame-level embedding
- pooled audio embedding
- final audio token

### 元数据模态演化示例
结构化视频元数据：
- raw metadata dict
- normalized metadata row
- selected fields
- scaled / encoded features
- final metadata token vector

### 要求
1. 所有 stage 信息都要可以被可视化查询
2. 所有 stage 尽量保留 preview
3. 大对象（音频、图片、中间 tensor）不要直接存数据库，数据库中只保存引用路径、对象键或摘要信息
4. 看板层应能按单个视频查看其各模态特征演化链路

---

# 四、技术栈要求

## 后端
- Python 3.11+
- `bilibili-api`
- `httpx` / `aiohttp`
- `asyncio`
- `tenacity` 或自定义指数退避重试
- `pydantic` 或 `dataclasses`
- `duckdb`
- `pyarrow`
- `pandas`
- `loguru` 或标准 logging
- `orjson`
- `sqlalchemy` 可选
- `boto3` 或兼容 S3 的 SDK 用于对象存储

## 存储
- 结构化数据：DuckDB + Parquet
- 大文件：S3 兼容对象存储
- 临时文件：本地 `tmp/`

## 监控/可视化接口
后端需要暴露统一的数据读取层，方便前端看板读取：
- metrics query service
- lineage query service
- dataset summary service
- feature stage preview service

---

# 五、对象存储要求

请实现抽象接口 `StorageBackend`，用于兼容：
- Cloudflare R2
- Backblaze B2
- MinIO
- 其他 S3 兼容对象存储

接口至少包含：

```python
class StorageBackend(Protocol):
    async def upload_bytes(self, data: bytes, object_key: str, content_type: str | None = None) -> str: ...
    async def upload_file(self, local_path: str, object_key: str, content_type: str | None = None) -> str: ...
    async def exists(self, object_key: str) -> bool: ...
    async def delete(self, object_key: str) -> None: ...
    async def get_public_url(self, object_key: str) -> str | None: ...
```

实现要求：
- 本地下载到 tmp/ 后应立即上传对象存储
- 上传成功后删除本地临时文件
- 数据表中只保存 object_key、etag、size、mime_type、sha256、provider
- object key 命名必须有统一规范，例如：
    - bilibili/video/{bvid}/audio/{cid}.m4a
    - bilibili/video/{bvid}/cover/cover.jpg
    - bilibili/video/{bvid}/subtitle/{cid}_{lang}.json
    - bilibili/video/{bvid}/intermediate/audio/mel/{run_id}.npy
    - bilibili/video/{bvid}/intermediate/image/embed/{run_id}.npy

---

# 六、飞书多维表格同步要求

飞书只作为监控镜像，不作为主数据库。

请实现可选 sink：FeishuBitableSink

功能包括：
- 将运行级指标同步到 crawl_runs 表
- 将字段质量指标同步到 field_quality_snapshot 表
- 将模态级指标同步到 modal_progress_snapshot 表
- 将异常任务同步到 dead_letter_tasks 表

要求：
- 支持 upsert 或幂等写入
- 支持 batch write
- 支持根据配置开关启用或关闭
- 飞书写入失败时不能影响主采集流程

---

# 七、项目目录结构要求

请按下列思想设计项目目录，不要求完全一致，但必须清晰分层：

project_root/
  app/
    config/
      settings.py
      constants.py
    core/
      logger.py
      retry.py
      limiter.py
      utils.py
      ids.py
      exceptions.py
    models/
      video.py
      comment.py
      subtitle.py
      asset.py
      metrics.py
      lineage.py
    fetchers/
      video_fetcher.py
      comment_fetcher.py
      subtitle_fetcher.py
      audio_fetcher.py
      cover_fetcher.py
      danmaku_fetcher.py
    normalizers/
      video_normalizer.py
      comment_normalizer.py
      subtitle_normalizer.py
      asset_normalizer.py
    storage/
      base.py
      s3_backend.py
      local_temp.py
    sinks/
      parquet_sink.py
      duckdb_sink.py
      feishu_bitable_sink.py
      metrics_sink.py
    metrics/
      calculators.py
      profilers.py
      reporters.py
    lineage/
      recorder.py
      serializers.py
      preview.py
    pipeline/
      crawl_pipeline.py
      asset_pipeline.py
      feature_pipeline.py
    services/
      crawl_service.py
      metrics_service.py
      lineage_service.py
      query_service.py
    scheduler/
      task_queue.py
      checkpoint.py
      batch_runner.py
    demos/
      minimal_demo.py
  data/
    bronze/
    silver/
    gold/
    metrics/
    checkpoints/
    tmp/
    logs/
  tests/
  pyproject.toml
  README.md

---

# 八、数据分层要求

请严格实现以下分层：

## bronze 层

原始抓取结果，尽量保持原始字段：
- videos_raw.parquet
- comments_raw.parquet
- subtitles_raw.parquet
- assets_raw.parquet

## silver 层

清洗、标准化后的统一结构：
- videos_cleaned.parquet
- comments_cleaned.parquet
- subtitles_cleaned.parquet
- assets_cleaned.parquet

## gold 层

可直接服务看板和建模：
- video_summary.parquet
- modal_progress.parquet
- feature_lineage.parquet
- field_quality.parquet

---

# 九、断点续跑与任务调度要求

必须支持大规模批量采集，要求：
1. 每个任务有唯一 task_id
2. 支持 checkpoint 持久化
3. 程序异常退出后可以从 checkpoint 恢复
4. 支持已完成任务跳过
5. 支持失败任务重试
6. 支持 dead letter queue
7. 支持限速
8. 支持最大并发数配置
9. 支持按 uid、关键词、bvid 列表三种入口启动任务

---

# 十、日志要求

日志必须便于调试。要求记录：
- run_id
- task_id
- modal_type
- bvid
- stage
- status
- latency
- retry_count
- error_type
- error_message

日志输出：
- 控制台
- 文件日志
- 可选 JSON 日志

# 十一、配置管理要求

请实现集中式配置，放置在 .env 中。

配置项至少包括：

- BILI_SESSDATA
- BILI_BILI_JCT
- BILI_BUVID3
- STORAGE_PROVIDER
- STORAGE_BUCKET
- STORAGE_ENDPOINT
- STORAGE_ACCESS_KEY
- STORAGE_SECRET_KEY
- FEISHU_APP_ID
- FEISHU_APP_SECRET
- FEISHU_APP_TOKEN
- FEISHU_TABLE_RUNS
- FEISHU_TABLE_FIELD_QUALITY
- FEISHU_TABLE_MODAL_PROGRESS
- MAX_CONCURRENCY
- REQUEST_TIMEOUT
- RETRY_TIMES
- ENABLE_FEISHU_SYNC
- ENABLE_AUDIO_DOWNLOAD
- ENABLE_SUBTITLE_DOWNLOAD
- ENABLE_COVER_DOWNLOAD

---

# 十二、数据模型要求

请优先使用 pydantic model 或 dataclass，并给出完整字段定义。

至少包括以下模型：

- VideoMeta
- CommentItem
- SubtitleAsset
- AudioAsset
- CoverAsset
- CrawlRunMetric
- FieldQualityMetric
- ModalProgressMetric
- FeatureLineageStage

其中 FeatureLineageStage 至少包含：

- stage_id
- run_id
- bvid
- cid
- modal_type
- stage_name
- input_ref
- output_ref
- input_shape
- output_shape
- processor_name
- processor_version
- preview_text
- metadata_json
- created_at

---

# 十三、查询服务要求

请设计后端查询服务，方便 UI 直接调用。

至少提供下列 service 或函数：

1. get_run_summary(run_id)
2. get_dataset_profile(dataset_name)
3. get_field_quality(dataset_name)
4. get_modal_progress(run_id)
5. get_video_lineage(bvid)
6. get_video_modal_stage_preview(bvid, modal_type)
7. list_failed_tasks(run_id)
8. list_recent_runs(limit=20)

返回结果尽量采用适合序列化给前端的 dict / pydantic model。



# 补充要求！！！

1. 不要把业务逻辑耦合到某一种存储平台。
2. 不要把飞书写成主存储。
3. 不要默认把所有音频长期保存到本地。
4. 任何大文件都应遵循“临时下载 -> 上传对象存储 -> 删除本地缓存”的流程。
5. 所有中间特征产物都应支持只保存引用，不直接存大对象本体。
6. feature lineage 是核心模块，请认真设计。
7. 代码风格偏工程化，适合后续持续迭代。
8. 先给我可运行骨架，再逐步细化。