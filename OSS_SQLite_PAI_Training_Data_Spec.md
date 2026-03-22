## OSS + SQLite + 阿里云 PAI 训练侧数据组织规范

本文档给出一套适用于当前论文项目的训练侧数据组织规范，目标是让 `Bilibili Video Data Crawler` 采集的数据能够平滑进入阿里云 `OSS` 与阿里云人工智能平台 `PAI` 的训练流程，同时尽量保持本地维护成本低、后续迭代清晰、数据版本可追踪。

### 1. 总体原则

- **媒体文件放 OSS**：视频、音频、封面、字幕、抽帧图、特征文件等大对象统一放在 `OSS`。
- **元数据放 SQLite**：本地 `SQLite` 只保留索引、描述信息、清洗结果、训练切分信息、对象定位信息。
- **训练读取以 manifest 为中心**：`PAI` 训练任务不要直接扫描整个 Bucket，而应优先读取明确版本的清单文件。
- **原始层与训练层分离**：原始抓取数据、预处理结果、训练样本、实验产物分别放在不同路径，避免互相污染。
- **所有可复现实验必须有版本号**：包括样本集版本、特征版本、标签版本、训练任务版本。

### 2. 推荐架构

- `OSS`
  - 保存大文件与训练中间产物
  - 作为 `PAI` 训练任务的主数据源
- `SQLite`
  - 保存视频元数据、评论快照、统计快照、媒体对象索引、训练样本清单、标签和切分结果
  - 作为本地数据管理与版本登记中心
- `PAI`
  - 从 `OSS` 读取训练数据与 manifest
  - 输出模型、日志、评估结果到 `OSS`

建议理解为：

- `SQLite = 控制面`
- `OSS = 数据面`
- `PAI = 计算面`

### 3. OSS Bucket 目录规范

建议在同一个 Bucket 下按层次组织：

```text
oss://<bucket>/bilibili-pipeline/
  raw/
    media/
      <bvid>/<cid>/video.m4s
      <bvid>/<cid>/audio.m4s
    meta/
      crawl_runs/<run_id>.json
      snapshots/<date>/<bvid>.json
  processed/
    remux_mp4/
      <bvid>/<cid>/video.mp4
    audio_wav/
      <bvid>/<cid>/audio.wav
    frames/
      <bvid>/<cid>/<frame_version>/000001.jpg
    asr/
      <bvid>/<cid>/<asr_version>.json
    features/
      <feature_version>/<bvid>/<cid>/video.npy
      <feature_version>/<bvid>/<cid>/audio.npy
      <feature_version>/<bvid>/<cid>/multimodal.npz
  manifests/
    dataset_versions/
      dataset_v001/
        train.jsonl
        val.jsonl
        test.jsonl
        dataset_card.json
    feature_versions/
      feat_v001/
        train.jsonl
        val.jsonl
        test.jsonl
        feature_card.json
  labels/
    label_v001/
      labels.csv
      label_schema.json
  experiments/
    exp_v001/
      config.yaml
      train_log/
      checkpoints/
      metrics.json
      predictions/
```

### 4. OSS 路径命名建议

建议统一采用：

```text
<root>/<layer>/<asset_type>/<bvid>/<cid>/<version_or_filename>
```

具体约定如下：

- 根前缀固定：`bilibili-pipeline/`
- 原始媒体层：`raw/media/<bvid>/<cid>/`
- 预处理层：`processed/<process_name>/<bvid>/<cid>/`
- 清单层：`manifests/dataset_versions/<dataset_version>/`
- 实验层：`experiments/<experiment_version>/`

命名规则建议：

- `dataset_version`：如 `dataset_v001`
- `feature_version`：如 `feat_v001`
- `label_version`：如 `label_v001`
- `experiment_version`：如 `exp_v001`
- 不要直接使用“final”“new”“latest”这类不可追踪命名

### 5. SQLite 的职责边界

SQLite 不建议存放大二进制，只负责保存以下信息：

- 视频基础信息
- 评论快照、统计快照
- 媒体对象在 OSS 中的定位信息
- 数据清洗状态
- 标签信息
- 数据集切分信息
- 特征文件索引
- 训练实验登记

推荐在现有爬虫数据库基础上，新增或维护以下逻辑表：

#### 5.1 `media_assets`

用于登记媒体对象在 OSS 中的位置。

建议字段：

- `bvid`
- `cid`
- `asset_type`，如 `video` / `audio`
- `storage_backend`
- `bucket_name`
- `object_key`
- `object_url`
- `mime_type`
- `file_size`
- `sha256`
- `etag`
- `created_at`
- `updated_at`

#### 5.2 `sample_labels`

用于保存训练标签。

建议字段：

- `sample_id`
- `bvid`
- `cid`
- `task_name`
- `label_name`
- `label_value`
- `label_source`
- `label_version`
- `annotator`
- `updated_at`

#### 5.3 `dataset_splits`

用于记录样本属于哪个数据集版本和切分。

建议字段：

- `dataset_version`
- `sample_id`
- `bvid`
- `cid`
- `split_name`，如 `train` / `val` / `test`
- `task_name`
- `stratify_key`
- `created_at`

#### 5.4 `feature_assets`

用于登记抽帧、ASR、embedding、特征文件。

建议字段：

- `feature_version`
- `sample_id`
- `bvid`
- `cid`
- `feature_type`
- `bucket_name`
- `object_key`
- `file_format`
- `feature_dim`
- `frame_count`
- `duration_seconds`
- `created_at`

#### 5.5 `training_runs`

用于登记训练实验。

建议字段：

- `experiment_version`
- `task_name`
- `dataset_version`
- `feature_version`
- `label_version`
- `train_manifest_path`
- `val_manifest_path`
- `test_manifest_path`
- `pytorch_image`
- `entry_script`
- `status`
- `metrics_json`
- `created_at`

### 6. 训练样本主键规范

建议定义统一样本主键：

```text
sample_id = <bvid>__<cid>
```

原因：

- 对单 P 视频足够稳定
- 便于 SQLite、JSONL、CSV、OSS 路径之间互相映射
- 便于后续扩展到帧级、片段级样本

如果后续做片段级训练，可扩展为：

```text
sample_id = <bvid>__<cid>__<start_ms>__<end_ms>
```

### 7. Manifest 规范

训练时推荐以 `JSONL` 作为主 manifest 格式，每行一个样本。

推荐字段：

```json
{
  "sample_id": "BVxxxxxx__123456",
  "bvid": "BVxxxxxx",
  "cid": 123456,
  "task_name": "video_classification",
  "label": "positive",
  "video_key": "bilibili-pipeline/processed/remux_mp4/BVxxxxxx/123456/video.mp4",
  "audio_key": "bilibili-pipeline/processed/audio_wav/BVxxxxxx/123456/audio.wav",
  "feature_key": "bilibili-pipeline/processed/features/feat_v001/BVxxxxxx/123456/multimodal.npz",
  "text_key": "bilibili-pipeline/processed/asr/BVxxxxxx/123456/asr_v001.json",
  "duration_seconds": 48.6,
  "split": "train",
  "dataset_version": "dataset_v001",
  "label_version": "label_v001",
  "feature_version": "feat_v001"
}
```

建议：

- manifest 中保存 `object_key`，不要只保存公网 URL
- 训练代码内部再根据 `Bucket + key` 拼接访问路径
- 同一版本的数据集必须固化成独立 manifest，不要动态查询 SQLite 生成

### 8. 数据集版本规范

每次进入正式训练前，固定一个数据集版本目录：

```text
manifests/dataset_versions/dataset_v001/
  train.jsonl
  val.jsonl
  test.jsonl
  dataset_card.json
```

其中 `dataset_card.json` 建议至少记录：

- 数据集版本号
- 生成时间
- 来源抓取 run_id
- 样本总数
- 标签分布
- 类别定义
- 切分规则
- 过滤规则
- 依赖的 label 版本
- 依赖的 feature 版本

### 9. 推荐训练前预处理层

为了让 `PAI` 训练更稳定，建议不要直接拿 `.m4s` 原始流进行训练，而是先做一层预处理：

#### 9.1 视频

- 原始：`video.m4s`
- 训练推荐：转为标准 `mp4`
- 若做视觉模型：
  - 抽帧到固定 FPS
  - 或直接生成视觉特征 `npy/npz`

#### 9.2 音频

- 原始：`audio.m4s`
- 训练推荐：转为统一采样率的 `wav`
- 若做音频模型：
  - 生成梅尔频谱
  - 或生成音频 embedding

#### 9.3 文本

- 评论快照和简介可直接来自 SQLite
- 若用字幕/语音文本，建议单独生成 `asr_v001.json`

结论是：

- `raw/` 用于归档
- `processed/` 用于训练
- `PAI` 优先消费 `processed/`

### 10. 阿里云 PAI 训练读取建议

建议 `PAI` 训练代码遵循以下读取顺序：

1. 从固定版本的 `train.jsonl / val.jsonl / test.jsonl` 读取样本列表
2. 通过 `object_key` 从 `OSS` 拉取训练所需文件
3. 尽量读取已经预处理好的 `mp4 / wav / npy / npz`
4. 避免训练时现场做大规模转码

不建议：

- 每次训练都直接遍历整个 Bucket
- 每次训练都从 SQLite 重新动态拼训练集
- 每个 batch 才去做重型转码与特征提取

### 11. 推荐实验目录规范

每个实验版本建议单独目录：

```text
experiments/exp_v001/
  config.yaml
  env.txt
  train_log/
  checkpoints/
  metrics.json
  predictions/
```

建议：

- `config.yaml` 保存训练超参数和数据版本
- `metrics.json` 保存核心评估指标
- `predictions/` 保存验证集或测试集输出
- 一个实验目录只对应一次完整训练配置

### 12. 数据清洗与质检规范

建议在进入 `dataset_version` 之前完成以下校验：

- `SQLite` 中存在对应 `sample_id`
- `OSS object_key` 可访问
- `sha256` 或 `etag` 非空
- 媒体时长非零
- 标签非空
- train/val/test 无交叉泄漏
- 同一 UP 主或同系列视频如需防泄漏，可按规则做组切分

可选增加质检状态字段：

- `is_media_ready`
- `is_label_ready`
- `is_feature_ready`
- `is_split_ready`
- `qa_status`

### 13. 安全与权限建议

- 训练时优先使用 RAM 子账号或临时 STS，而不是长期高权限主账号密钥
- `PAI` 与 `OSS` 尽量放在同地域
- Bucket 权限建议默认私有
- 对外展示或分享时使用单独的公网分发层，不直接暴露训练 Bucket

### 14. 对当前项目的最小落地建议

如果你想先快速跑通，可先采用下面这版最小方案：

- `SQLite`
  - 保留当前爬虫数据库
  - 新增 `sample_labels`、`dataset_splits`、`training_runs`
- `OSS`
  - `raw/media/<bvid>/<cid>/video.m4s`
  - `raw/media/<bvid>/<cid>/audio.m4s`
  - `processed/remux_mp4/<bvid>/<cid>/video.mp4`
  - `processed/audio_wav/<bvid>/<cid>/audio.wav`
  - `manifests/dataset_versions/dataset_v001/{train,val,test}.jsonl`
- `PAI`
  - 只读取 `dataset_v001` 的 manifest
  - 优先训练基于 `mp4/wav` 或特征文件的任务

### 15. 推荐执行顺序

建议后续按以下顺序推进：

1. 先稳定 `Crawler -> OSS + SQLite` 采集链路
2. 再增加预处理脚本，把 `.m4s` 统一转成训练友好的 `mp4/wav`
3. 建立 `sample_labels` 和 `dataset_splits`
4. 固化第一个 `dataset_v001`
5. 在 `PAI` 上以 manifest 方式启动训练
6. 将实验结果回写到 `OSS + SQLite`

### 16. 一句话总结

对当前项目，最稳妥的训练侧组织方式是：

- **OSS 保存原始媒体、预处理文件、manifest、模型产物**
- **SQLite 保存索引、标签、切分、版本和实验记录**
- **PAI 始终围绕固定版本 manifest 进行训练**

这样最有利于后续论文实验的**可复现性、可扩展性和迭代效率**。
