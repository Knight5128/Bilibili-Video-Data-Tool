## Bilibili Video Data Crawler 简要说明

**Bilibili Video Data Crawler** 是一个基于 `bilibili-api-python` + `Streamlit` 的视频数据采集工具，用于在已有 `video_pool` 或指定 `bvid` 的基础上，抓取并存储四类数据：**视频元数据、流行度时间序列、评论快照、多模态媒体数据**。当前版本支持将**结构化元数据保存在本地 SQLite**，并将**视频/音频媒体文件写入本地 SQLite 或阿里云 OSS**；其中视频元数据已扩展覆盖更丰富的**分区、分辨率、权限状态、上传用户资料与空间概览信息**。

### 核心功能

- **单 bvid 全流程抓取**：对一个 `bvid` 依次执行元数据、统计快照、最新评论快照和媒体数据抓取，并输出阶段级摘要。
- **增强元数据抓取**：在 `Meta` 阶段除基础标题、简介、标签外，还补充抓取分区、封面、动态文案、分辨率、权限对象、上传用户资料、粉丝/关注关系和空间概览统计等信息。
- **CSV 批量抓取**：上传包含 `bvid` 列的 CSV 文件，按配置的并发度依次调用单视频全流程接口。
- **四类接口单独调试**：在前端分别调用 Meta、Stat、Comment、Media 四类接口，便于调试和小规模实验。
- **媒体流式存储**：视频与音频通过下载直链分块读取，可按配置选择：
  - 写入 SQLite 分块表，适合单机实验；
  - 上传到阿里云 OSS，适合后续接入阿里云 PAI 等云端训练流程。
- **SQLite 元数据底座**：视频元数据、评论快照、统计快照、批量运行记录，以及媒体对象的 `object_key / bucket / endpoint / hash / size` 等信息统一保存在本地 SQLite 中。
- **结构化数据查看**：在 SQLite 视图中，可按 `bvid` 直接查看当前已入库的视频元数据、最新统计快照、最新评论快照与媒体资产记录，便于核对增强字段是否已成功落库。
- **媒体文件回读导出**：在 SQLite 视图中，对已有媒体资产的 `bvid` 可直接回读并导出对应的视频/音频 `.m4s` 文件；若媒体存于 OSS，只需在侧边栏填入对应 Bucket 凭证即可回读。
- **OSS 连接测试**：前端提供完整的阿里云 OSS 配置区（基于**OSS Python SDK V1**, 文档位于: https://help.aliyun.com/zh/oss/developer-reference/python-sdk-v1），可在抓取前测试连接状态。
- **字段定义展示**：前端右侧常驻一个可收起的“字段定义速查”面板，并同步维护独立文档 `Bilibili_Video_Data_Crawler_Field_Definitions.md`，用于静态展示变量名称、特征名称、特征类型、描述与示例值的对应关系，便于接口调试时随时对照。
- **快捷跳转**：支持在前端直接输入单个视频 `bvid` 或作者 `owner_mid`，一键调用系统默认浏览器跳转到对应的视频播放页或作者主页。

### 典型使用场景

- **学术研究**：围绕指定样本池补充视频的内容属性、互动面板、评论快照和媒体资产。
- **方法验证**：对少量视频快速测试不同评论条数、并发度、媒体分块大小、最大分辨率和媒体存储方式等参数。
- **批量数据准备**：对 `Bilibili_Video_Pool_Builder` 导出的 `video_pool` CSV 进行顺序补抓，形成后续分析所需的完整数据底座。
- **云端训练准备**：将媒体文件直接沉淀到阿里云 OSS，本地只保留轻量元数据库，便于后续在阿里云 PAI 中直接读取与训练。

### 当前使用方式（简要）

- 运行入口：在 `bilibili-data` 目录下运行 Streamlit 应用：

```bash
streamlit run bvd-crawler.py
```

- Logo 资源目录：界面会优先读取 `assets/logos/bvd-crawler.png` 作为浏览器标签页图标，并显示在主界面顶部的居中标题旁；若该文件不存在，则自动回退为纯文本标题。
- 默认背景：主界面默认使用黑色夜空 + 白色流星划过的动态背景；若需要调整流星数量、速度、长度等动画参数，可修改 `src/bili_pipeline/utils/streamlit_night_sky.py` 中的 `NightSkyConfig` 默认值。

- 前端主要包含五个主菜单，并在页面右侧常驻一个可收起的字段定义面板：
  - **单 bvid 全流程**：输入一个 `bvid`，顺序执行四类抓取逻辑。
  - **CSV 批量抓取**：上传带 `bvid` 列的 CSV，批量执行全流程抓取。
  - **四类接口调试**：分别调用 `crawl_video_meta`、`crawl_stat_snapshot`、`crawl_latest_comments`、`stream_media_to_store/crawl_media_assets`；其中 `Meta` 调试页会额外展开展示标签详情、视频权限对象、上传用户资料、关系快照与空间概览。
  - **SQLite 数据查看**：查看某个 `bvid` 已入库的视频元数据、最新统计快照、最新评论快照和媒体资产信息，并在有媒体资产时通过“导出该 bvid 的媒体文件为本地文件”面板直接下载视频/音频 `.m4s` 文件；若资产存于 OSS，则按当前 OSS 配置回读。
  - **快捷跳转**：输入单个 `bvid` 或作者 ID，也可直接粘贴 B 站视频链接/作者主页链接，界面会自动提取有效标识并在系统默认浏览器中打开目标页面。
  - **右侧字段定义速查面板**：项目启动时即完成初始化并固定在页面右侧，可随时展开 / 收起；页面上下滚动时面板位置保持不变，支持按数据模块筛选字段，也可继续按模块展开查看完整定义。

- 侧边栏全局设置中可以配置：
  - SQLite 存储路径、默认评论条数、媒体分块大小、最大分辨率等运行参数；
  - 媒体文件存储方式：`SQLite 分块存储` 或 `阿里云 OSS + SQLite 元数据`；
  - 当选择阿里云 OSS 时，可配置 `Endpoint`、`Bucket 名称`、`Bucket 所在地域（可选）`、`对象前缀`、`公共访问基础 URL（可选）`、`AccessKey ID`、`AccessKey Secret`、`Security Token（可选）`，并可直接测试连接；
  - 可选的 **B 站 Cookie（SESSDATA、bili_jct、buvid3）**，用于在本地内存中构造 `bilibili-api-python` 的登录态 `Credential`，提升评论抓取和高质量媒体直链获取的成功率与稳定性；
  - **安全说明**：B 站 Cookie、OSS AccessKey、Security Token 仅保存在当前 Streamlit 会话内存中，不会写入 SQLite 或其它持久化存储。

### 维护约定

- 当对 **Bilibili Video Data Crawler** 的功能、参数、使用方式或 UI 结构进行实质性修改时：
  - 需要同步检查并更新本说明文档，保证文档与当前实现一致。
  - 特别是：四类接口的输入输出、媒体流式入库策略、阿里云 OSS 接入方式、批量抓取行为、标签页结构发生变化时，务必更新相关描述。
  - 若新增单视频抓取字段或新的数据模块，应同步更新 `src/bili_pipeline/field_reference.py`，以保持前端“字段定义”页与 `Bilibili_Video_Data_Crawler_Field_Definitions.md` 文档一致。
