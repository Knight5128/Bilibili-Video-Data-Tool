## Bilibili Video Data Crawler 简要说明

**Bilibili Video Data Crawler** 是一个基于 `bilibili-api-python` + `Streamlit` 的视频数据采集工具，用于在已有 `video_pool` 或指定 `bvid` 的基础上，抓取并存储四类数据：**视频元数据、流行度时间序列、评论快照、多模态媒体数据**。当前版本已重构为 **Google Cloud** 存储方案：**结构化结果统一写入 BigQuery**，**视频/音频媒体文件统一写入 Google Cloud Storage（GCS）**，便于后续在 **Google Colab Notebook** 中直接完成加载、抽样、训练与分析；其中视频元数据已扩展覆盖更丰富的**分区、分辨率、权限状态、上传用户资料与空间概览信息**。这些能力现在也已被整合进统一入口 `bilibili-datahub.py`，本说明文档主要对应旧入口 `bvd-crawler.py`，同时说明其在 `Bilibili DataHub` 中的继承关系。

### 核心功能

- **单 bvid 全流程抓取**：对一个 `bvid` 依次执行元数据、统计快照、最热评论快照和媒体数据抓取，并输出阶段级摘要，同时将本次测试日志写入 `outputs/video_data/test_crawls/single_bvid_<date>_<time>/`；日志文件开头与结尾会额外写入 `[TIMESTAMP][BEGIN]` / `[TIMESTAMP][END]`，标记本次记录的精确起止时间。
- **增强元数据抓取**：在 `Meta` 阶段除基础标题、简介、标签外，还补充抓取分区、封面、动态文案、分辨率、权限对象、上传用户资料、粉丝/关注关系和空间概览统计等信息。
- **CSV 批量抓取**：上传包含 `bvid` 列的 CSV 文件后，系统会在 `outputs/video_data/batch_crawls/batch_crawl_<date>_<time>/` 下集中保存该批任务的剩余 CSV、状态文件与日志；当连续失败条数达到侧边栏阈值时，会自动暂停当前子任务，并导出剔除已成功条目的 `remaining_bvids_part_n.csv` 供后续续跑。子任务日志与最终汇总日志同样都会带有 `[TIMESTAMP][BEGIN]` / `[TIMESTAMP][END]` 起止标记。在 `Bilibili DataHub` 中，这一批量抓取已拆分为“评论/互动量实时数据”和“元数据/媒体一次性数据”两类开关，可独立选择。同一入口下的 **手动批量抓取-动态数据** 标签会让用户直接多选 `outputs/video_pool/full_site_floorings/` 下的一个或多个现成视频列表 CSV，并可选叠加 `outputs/video_pool/uid_expansions/` 下的一个或多个任务目录；系统会先合并来源、按追踪窗口裁剪并去重，再在 `outputs/video_data/manual_crawls/manual_crawl_stat_comment_<date>_<time>/` 中留存 `filtered_video_list.csv` 后，继续执行实时评论/互动量抓取。
- **四类接口单独调试**：在前端分别调用 Meta、Stat、Comment、Media 四类接口，便于调试和小规模实验；每次调用都会将调试日志写入 `outputs/video_data/test_crawls/<meta/stat/comment/media>_api_<date>_<time>/`，并在日志首尾自动补充 `[TIMESTAMP][BEGIN]` / `[TIMESTAMP][END]`。
- **Google 云端双层存储**：视频元数据、评论快照、统计快照、批量运行记录，以及媒体对象的 `object_key / bucket / endpoint / hash / size` 等信息统一写入 BigQuery；视频与音频媒体文件通过下载直链分块读取后直接上传到 GCS。
- **媒体流式上传到 GCS**：媒体抓取阶段以流式方式从 B 站下载音视频，并同步写入 GCS Bucket，适合后续在 Google Colab、Vertex AI 或其他 Google 生态工具中直接读取。
- **BigQuery 数据查看**：在前端的数据查看页中，可按 `bvid` 查看当前已入库的视频元数据、最新统计快照、最热评论快照与媒体资产记录，便于核对增强字段是否已成功落库。
- **媒体文件回读导出**：在 BigQuery / GCS 视图中，对已有媒体资产的 `bvid` 可直接根据 BigQuery 中记录的对象元数据回读并导出对应的视频/音频 `.m4s` 文件。
- **GCP 连接测试**：前端提供完整的 Google Cloud 配置区，可在抓取前同时测试 BigQuery Dataset 与 GCS Bucket 的可访问性。
- **Google Cloud 配置保存**：侧边栏支持将 `GCP Project ID / BigQuery Dataset / GCS Bucket / Region / 服务账号 JSON 路径 / 对象前缀 / 公共访问基础 URL` 保存到本地配置文件，下次启动应用时会自动回填，无需重复输入。
- **字段定义展示**：前端右侧常驻一个可收起的“字段定义速查”面板，并同步维护独立文档 `Bilibili_Video_Data_Crawler_Field_Definitions.md`，用于静态展示变量名称、特征名称、特征类型、描述与示例值的对应关系，便于接口调试时随时对照。
- **快捷跳转**：支持在前端直接输入单个视频 `bvid` 或作者 `owner_mid`，一键调用系统默认浏览器跳转到对应的视频播放页或作者主页。

### 典型使用场景

- **学术研究**：围绕指定样本池补充视频的内容属性、互动面板、评论快照和媒体资产。
- **方法验证**：对少量视频快速测试不同评论条数、并发度、媒体分块大小、最大分辨率和媒体存储方式等参数。
- **批量数据准备（旧版 CSV 路径）**：对 `Bilibili_Video_Pool_Builder` 导出的 `video_pool` CSV 进行顺序补抓，适合旧版或一次性全流程补抓。
- **断点续跑**：当批量抓取因连续失败阈值而暂停后，可直接把 `remaining_bvids_part_n.csv` 再上传到“CSV 批量抓取”中继续执行，系统会自动复用第一次任务的会话目录并继续输出 `part_(n+1)` 文件。
- **DataHub 实时补抓**：若只想补抓评论/互动量实时数据，建议使用 `Bilibili DataHub` 中的 `手动批量抓取-动态数据`；该页基于用户所选 `full_site_floorings` 视频列表 CSV 与可选的 `uid_expansion` 任务目录运行，并在新建的 `manual_crawl_stat_comment_*` 任务目录中先留存本轮待抓 CSV。
- **云端训练准备**：将媒体文件直接沉淀到 GCS，并在 BigQuery 中维护结构化索引与样本描述，便于后续在 Google Colab Notebook 中直接读取与训练。

### 当前使用方式（简要）

- 推荐入口：若希望与视频列表发现、作者扩展以及本地自动追踪共用同一前端，建议在 `bilibili-data` 目录下运行统一入口：

```bash
streamlit run bilibili-datahub.py
```

- 旧版独立入口仍可继续使用：

```bash
streamlit run bvd-crawler.py
```

- Logo 资源目录：界面会优先读取 `assets/logos/bvd-crawler.png` 作为浏览器标签页图标，并显示在主界面顶部的居中标题旁；若该文件不存在，则自动回退为纯文本标题。
- 默认背景：主界面默认使用黑色夜空 + 白色流星划过的动态背景；若需要调整流星数量、速度、长度等动画参数，可修改 `src/bili_pipeline/utils/streamlit_night_sky.py` 中的 `NightSkyConfig` 默认值。

- 旧版独立入口前端主要包含五个主菜单，并在页面右侧常驻一个可收起的字段定义面板：
  - **单 bvid 全流程**：输入一个 `bvid`，顺序执行四类抓取逻辑。
  - **CSV 批量抓取**：上传带 `bvid` 列的 CSV，批量执行全流程抓取；这是旧版 `bvd-crawler.py` 中面向显式 `bvid` 清单的入口。
  - **四类接口调试**：分别调用 `crawl_video_meta`、`crawl_stat_snapshot`、`crawl_latest_comments`、`stream_media_to_store/crawl_media_assets`；其中 `Meta` 调试页会额外展开展示标签详情、视频权限对象、上传用户资料、关系快照与空间概览，并将每次调试结果保存为独立日志。
  - **BigQuery / GCS 数据查看**：查看某个 `bvid` 已入库的视频元数据、最新统计快照、最热评论快照和媒体资产信息，并在有媒体资产时通过“导出该 bvid 的媒体文件为本地文件”面板直接从 GCS 回读视频/音频 `.m4s` 文件。
  - **快捷跳转**：输入单个 `bvid` 或作者 ID，也可直接粘贴 B 站视频链接/作者主页链接，界面会自动提取有效标识并在系统默认浏览器中打开目标页面。
  - **右侧字段定义速查面板**：项目启动时即完成初始化并固定在页面右侧，可随时展开 / 收起；页面上下滚动时面板位置保持不变，支持按数据模块筛选字段，也可继续按模块展开查看完整定义。

- 在 `Bilibili DataHub` 中，上述能力主要分散到 `数据抓取调试`、`手动批量抓取-动态数据`、`手动批量抓取-媒体/元数据` 与 `BigQuery / GCS 数据查看` 等标签页中：
  - `手动批量抓取-动态数据`：仅抓取评论/互动量实时数据；用户在页内多选 `full_site_floorings` 视频列表 CSV，并可选叠加 `uid_expansion` 任务目录，系统会合并来源后按追踪窗口裁剪并去重，将结果先保存到 `outputs/video_data/manual_crawls/manual_crawl_stat_comment_<date>_<time>/filtered_video_list.csv`，再启动实时抓取。
  - `手动批量抓取-媒体/元数据`：面向元数据与媒体补抓。
  - `数据抓取调试` 与 `BigQuery / GCS 数据查看`：继续承接旧入口中的调试、查看与导出能力。

- 侧边栏全局设置中可以配置：
  - 默认评论条数、媒体分块大小、最大分辨率等运行参数；
  - `CSV 批量抓取连续失败暂停阈值`：默认 `10`；达到阈值后会暂停当前批量子任务，并导出剔除已成功条目的剩余 CSV；
  - Google Cloud 配置：`GCP Project ID`、`BigQuery Dataset`、`GCS Bucket 名称`、`GCP Region（可选）`、`服务账号 JSON 路径（可选）`、`GCS 对象前缀`、`公共访问基础 URL（可选）`；
  - 可使用“保存配置”按钮将上述 Google Cloud 配置写入本地文件；应用下次启动时会自动读取并回填；
  - 可选的 **B 站 Cookie（SESSDATA、bili_jct、buvid3）**，用于在本地内存中构造 `bilibili-api-python` 的登录态 `Credential`，提升评论抓取和高质量媒体直链获取的成功率与稳定性；
  - **安全说明**：B 站 Cookie 仅保存在当前 Streamlit 会话内存中；Google Cloud 配置会在点击“保存配置”后写入本地文件，但该本地配置文件已默认加入 `.gitignore`，不会被纳入版本控制。若未填写 JSON 路径，则默认使用 Application Default Credentials（ADC）。

- 默认本地导出根目录为 `outputs/video_data/`：
  - `batch_crawls/`：保存每个 CSV 批量任务的会话目录、`remaining_bvids_part_n.csv`、`batch_crawl_state.json`、子任务日志，以及在整轮完成后生成的汇总日志；
  - `test_crawls/`：保存“单 bvid 全流程”和“四类接口调试”的本地日志，便于核对 GCS 路径、接口返回结果与报错信息。
  - 在 `Bilibili DataHub` 的自动追踪页中，还会复用 `tracker_meta_media_queue` 作为“待补元数据/媒体数据”清单来源，用于增量消化只需抓取一次的数据。

### 维护约定

- 当对 **Bilibili Video Data Crawler** 的功能、参数、使用方式或 UI 结构进行实质性修改时：
  - 需要同步检查并更新本说明文档，保证文档与当前实现一致。
  - 特别是：四类接口的输入输出、媒体流式入库策略、BigQuery / GCS 接入方式、批量抓取行为、标签页结构发生变化时，务必更新相关描述。
  - 若新增单视频抓取字段或新的数据模块，应同步更新 `src/bili_pipeline/field_reference.py`，以保持前端“字段定义”页与 `Bilibili_Video_Data_Crawler_Field_Definitions.md` 文档一致。
  - 若这些调整是先在 `bilibili-datahub.py` 中完成的，也应同步检查本说明文档中的旧入口描述与统一入口描述。
