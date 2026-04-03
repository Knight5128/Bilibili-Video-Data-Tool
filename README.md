# bilibili-data

这是一个面向本地研究与分析场景的 Bilibili 数据工具仓库，当前主要包含一个统一的本地 Streamlit 应用、两个保留的旧版本地入口、一个运行在 `Cloud Run` 上的追踪服务，以及一个运行在 `Cloudflare Workers` 上的公网控制台：

- `Bilibili DataHub`：统一整合视频列表构建、CSV/XLSX 拼接去重、视频四类数据抓取、BigQuery/GCS 查看，以及本地自动追踪。
- `Bilibili Video Data Crawler`：围绕指定 `bvid` 或 `video_pool` 继续抓取视频元数据、统计快照、评论快照与媒体数据。
- `Bilibili_Video_Pool_Builder`：从分区、热门榜单、每周必看等来源构建 `video_pool`，并支持作者历史视频扩展与 CSV/XLSX 拼接去重。
- `Bilibili Cloud Tracker`：运行在 `Cloud Run + Cloud Scheduler` 上的无状态追踪服务，用于周期性发现新视频、维护追踪清单，并每隔固定时间追加互动快照与评论切片。
- `Bilibili Cloud Control Panel`：运行在 `Cloudflare Workers + Cloudflare Access` 上的单租户公网控制台，用于管理 `Cloud Tracker`、`Cloud Run` 与 `Cloud Scheduler`。

## 文档入口

- [Bilibili DataHub 说明文档](./Bilibili_DataHub.md)
- [Bilibili Video Data Crawler 说明文档](./Bilibili_Video_Data_Crawler.md)
- [Bilibili_Video_Pool_Builder 说明文档](./Bilibili_Video_Pool_Builder.md)
- [字段定义速查](./Bilibili_Video_Data_Crawler_Field_Definitions.md)
- [Bilibili Cloud Tracker 部署说明](./Bilibili_Cloud_Tracker_Deployment.md)
- [Cloudflare Control Panel 部署说明](./Cloudflare_Control_Panel_Deployment.md)

## 快速开始

建议使用 Python 3.10+。

如果使用 `uv`：

```bash
uv sync
```

如果使用 `pip`：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## 运行方式

推荐启动统一入口 `Bilibili DataHub`：

```bash
streamlit run bilibili-datahub.py
```

运行本地自动追踪脚本（单次执行一轮）：

```bash
python bilibili-datahub_runner.py
```

旧版 `Bilibili Video Data Crawler` 入口仍可单独启动：

```bash
streamlit run bvd-crawler.py
```

旧版 `Bilibili_Video_Pool_Builder` 入口仍可单独启动：

```bash
streamlit run bvp-builder.py
```

本地运行 `Bilibili Cloud Tracker`：

```bash
python -m bili_pipeline.cloud_tracker
```

本地运行 `Cloudflare Control Panel`：

```bash
cd cloud_panel
npm install
npm run dev
```

## 目录说明

- `src/`：核心业务逻辑与数据处理模块，现已包含 `bili_pipeline.cloud_tracker` 云端追踪服务，以及 `bili_pipeline.datahub` 的本地统一封装。
- `tests/`：基础测试用例。
- `assets/`：界面资源文件。
- `outputs/`：本地运行时导出的结果文件与中间产物。
- `cloud_tracker/`：Cloud Run 部署相关文件，如 `Dockerfile`。
- `cloud_panel/`：Cloudflare Workers 控制台项目。

## 说明

该仓库更适合作为个人研究或多设备同步使用的本地项目仓库。当前推荐把排行榜发现、作者增量发现、评论/互动量快照补抓，以及待补元数据/媒体数据的统一消费，都放到 `Bilibili DataHub` 中完成；旧版两个 Streamlit 入口保留主要是为了兼容之前的使用习惯。

当前 `Bilibili DataHub` 的本地后台任务会周期性写入心跳状态；当遇到 BigQuery 短暂的 SSL / 连接抖动时，也会对流式写入做额外重试，减少页面刷新或代理网络波动带来的误判失败。

建议不要将本地数据库、日志、虚拟环境和敏感配置提交到远程仓库，相关内容已在 `.gitignore` 中排除。
