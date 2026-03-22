# bilibili-data

这是一个面向本地研究与分析场景的 Bilibili 数据工具仓库，当前主要包含两个可独立运行的 Streamlit 应用：

- `Bilibili Video Data Crawler`：围绕指定 `bvid` 或 `video_pool` 继续抓取视频元数据、统计快照、评论快照与媒体数据。
- `Bilibili_Video_Pool_Builder`：从分区、热门榜单、每周必看等来源构建 `video_pool`，并支持作者历史视频扩展与 CSV/XLSX 拼接去重。

## 文档入口

- [Bilibili Video Data Crawler 说明文档](./Bilibili_Video_Data_Crawler.md)
- [Bilibili_Video_Pool_Builder 说明文档](./Bilibili_Video_Pool_Builder.md)
- [字段定义速查](./Bilibili_Video_Data_Crawler_Field_Definitions.md)

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

启动 `Bilibili Video Data Crawler`：

```bash
streamlit run bvd-crawler.py
```

启动 `Bilibili_Video_Pool_Builder`：

```bash
streamlit run bvp-builder.py
```

## 目录说明

- `src/`：核心业务逻辑与数据处理模块。
- `tests/`：基础测试用例。
- `assets/`：界面资源文件。
- `outputs/`：本地运行时导出的结果文件与中间产物。

## 说明

该仓库更适合作为个人研究或多设备同步使用的本地项目仓库。建议不要将本地数据库、日志、虚拟环境和敏感配置提交到远程仓库，相关内容已在 `.gitignore` 中排除。
