from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


FIELD_REFERENCE_DOC_NAME = "Bilibili_Video_Data_Crawler_Field_Definitions.md"


@dataclass(frozen=True, slots=True)
class FieldDefinition:
    section: str
    variable_name: str
    feature_name: str
    feature_type: str
    description: str
    example_value: str


FIELD_DEFINITIONS: tuple[FieldDefinition, ...] = (
    FieldDefinition("MetaResult", "bvid", "视频唯一标识", "离散", "B 站视频的唯一 BVID。", "BV1xx411c7mD"),
    FieldDefinition("MetaResult", "aid", "视频 AV 号", "离散", "视频对应的 AV 标识。", "243922477"),
    FieldDefinition("MetaResult", "title", "视频标题", "文本", "视频主标题。", "用 Python 分析 B 站视频弹幕"),
    FieldDefinition("MetaResult", "desc", "视频简介", "文本", "视频简介或发布说明。", "本期视频演示如何抓取并清洗弹幕数据。"),
    FieldDefinition("MetaResult", "pic", "视频封面 URL", "文本", "视频封面图片的访问地址。", "https://i0.hdslb.com/bfs/archive/example.jpg"),
    FieldDefinition("MetaResult", "dynamic", "视频动态文案", "文本", "视频发布时填写的话题或动态说明文本。", "#Python##数据分析#"),
    FieldDefinition("MetaResult", "tags", "视频标签列表", "复合", "视频关联的标签名称列表。", '["Python", "数据分析", "Bilibili"]'),
    FieldDefinition("MetaResult", "tag_details", "视频标签详情列表", "复合", "视频标签的结构化详情对象列表。", '[{"tag_id": 123456, "tag_name": "Python"}]'),
    FieldDefinition("MetaResult", "videos", "分 P 数量", "连续", "视频包含的分 P 或片段数量。", "3"),
    FieldDefinition("MetaResult", "tid", "一级分区 ID", "离散", "视频所属的主分区编号。", "17"),
    FieldDefinition("MetaResult", "tid_v2", "细分分区 ID", "离散", "视频所属的细分分区编号。", "2078"),
    FieldDefinition("MetaResult", "tname", "一级分区名称", "文本", "视频所属的主分区名称。", "单机游戏"),
    FieldDefinition("MetaResult", "tname_v2", "细分分区名称", "文本", "视频所属的细分分区名称。", "游戏杂谈"),
    FieldDefinition("MetaResult", "copyright", "原创/转载标记", "离散", "视频是原创还是转载的站内标记。", "1"),
    FieldDefinition("MetaResult", "owner_mid", "作者 ID", "离散", "视频作者的用户 ID。", "12345678"),
    FieldDefinition("MetaResult", "owner_name", "作者昵称", "文本", "视频作者显示昵称。", "数据实验室"),
    FieldDefinition("MetaResult", "owner_face", "作者头像 URL", "文本", "视频作者头像图片的访问地址。", "https://i0.hdslb.com/bfs/face/example.jpg"),
    FieldDefinition("MetaResult", "owner_sign", "作者签名", "文本", "抓取时作者空间展示的个性签名。", "长期分享数据分析实验。"),
    FieldDefinition("MetaResult", "owner_gender", "作者性别", "离散", "抓取时作者账号公开展示的性别。", "男"),
    FieldDefinition("MetaResult", "owner_level", "作者账号等级", "连续", "抓取时作者账号等级。", "6"),
    FieldDefinition("MetaResult", "owner_verified", "作者是否认证", "布尔", "作者账号是否具有官方认证标记。", "True"),
    FieldDefinition("MetaResult", "owner_verified_title", "作者认证类型", "文本", "作者账号的官方认证标题或认证说明。", "bilibili 知名科普 UP 主"),
    FieldDefinition("MetaResult", "owner_vip_type", "作者会员类型", "离散", "抓取时作者账号的会员类型标记。", "2"),
    FieldDefinition("MetaResult", "owner_follower_count", "作者粉丝数", "连续", "抓取时作者账号的粉丝数量。", "245000"),
    FieldDefinition("MetaResult", "owner_following_count", "作者关注数", "连续", "抓取时作者账号关注的用户数量。", "512"),
    FieldDefinition("MetaResult", "owner_video_count", "作者公开视频数", "连续", "抓取时作者账号公开视频数量。", "318"),
    FieldDefinition("MetaResult", "is_activity_participant", "是否活动参与视频", "布尔", "视频是否属于活动、合集或荣誉相关内容。", "False"),
    FieldDefinition("MetaResult", "duration", "视频时长（秒）", "连续", "视频主稿件或默认分 P 的时长，单位为秒。", "314"),
    FieldDefinition("MetaResult", "state", "稿件状态", "离散", "视频稿件当前的站内状态码。", "0"),
    FieldDefinition("MetaResult", "pubdate", "发布时间", "时间", "视频公开发布时间。", "2026-03-10T20:15:00"),
    FieldDefinition("MetaResult", "cid", "分 P / 播放段 ID", "离散", "默认第一页或主播放段对应的 CID。", "1486239001"),
    FieldDefinition("MetaResult", "resolution_width", "视频宽度", "连续", "默认分 P 的画面宽度（像素）。", "1920"),
    FieldDefinition("MetaResult", "resolution_height", "视频高度", "连续", "默认分 P 的画面高度（像素）。", "1080"),
    FieldDefinition("MetaResult", "resolution_rotate", "画面旋转角度", "连续", "默认分 P 画面旋转角度。", "0"),
    FieldDefinition("MetaResult", "is_story", "是否竖屏故事视频", "布尔", "视频是否被标记为故事/短视频形态。", "False"),
    FieldDefinition("MetaResult", "is_interactive_video", "是否互动视频", "布尔", "视频是否包含互动或剧情分支结构。", "False"),
    FieldDefinition("MetaResult", "is_downloadable", "是否可下载", "布尔", "视频是否允许站内下载。", "True"),
    FieldDefinition("MetaResult", "is_reprint_allowed", "是否允许转载", "布尔", "视频是否允许转载或二次搬运。", "False"),
    FieldDefinition("MetaResult", "is_collaboration", "是否联合投稿", "布尔", "视频是否为多人协作投稿。", "True"),
    FieldDefinition("MetaResult", "is_360", "是否全景视频", "布尔", "视频是否支持 360 / 全景播放。", "False"),
    FieldDefinition("MetaResult", "is_paid_video", "是否付费相关视频", "布尔", "视频是否带有付费或充电专属限制。", "False"),
    FieldDefinition("MetaResult", "pages_info", "分 P 信息列表", "复合", "视频各分 P 的结构化信息列表。", '[{"cid": 1486239001, "page": 1, "part": "正片"}]'),
    FieldDefinition("MetaResult", "rights", "视频权限对象", "复合", "视频下载、转载、付费等权限的结构化对象。", '{"download": 1, "no_reprint": 1, "is_cooperation": 1}'),
    FieldDefinition("MetaResult", "subtitle", "字幕信息对象", "复合", "视频字幕开关与字幕列表信息。", '{"allow_submit": false, "list": []}'),
    FieldDefinition("MetaResult", "uploader_profile", "上传用户资料对象", "复合", "视频上传用户的公开资料结构化对象。", '{"mid": 12345678, "name": "数据实验室"}'),
    FieldDefinition("MetaResult", "uploader_relation", "上传用户关系对象", "复合", "上传用户粉丝、关注等关系统计对象。", '{"follower": 245000, "following": 512}'),
    FieldDefinition("MetaResult", "uploader_overview", "上传用户空间概览对象", "复合", "上传用户空间内容数量概览对象。", '{"video": 318, "article": 12}'),
    FieldDefinition("StatSnapshot", "bvid", "视频唯一标识", "离散", "统计快照对应的视频 BVID。", "BV1xx411c7mD"),
    FieldDefinition("StatSnapshot", "snapshot_time", "统计抓取时间", "时间", "本次热度快照的采集时间。", "2026-03-15T14:32:11"),
    FieldDefinition("StatSnapshot", "stat_view", "播放量", "连续", "抓取时视频累计播放量。", "128450"),
    FieldDefinition("StatSnapshot", "stat_like", "点赞量", "连续", "抓取时视频累计点赞量。", "9321"),
    FieldDefinition("StatSnapshot", "stat_coin", "投币量", "连续", "抓取时视频累计投币量。", "2104"),
    FieldDefinition("StatSnapshot", "stat_favorite", "收藏量", "连续", "抓取时视频累计收藏量。", "5430"),
    FieldDefinition("StatSnapshot", "stat_share", "分享量", "连续", "抓取时视频累计分享量。", "812"),
    FieldDefinition("StatSnapshot", "stat_reply", "评论量", "连续", "抓取时视频累计评论量。", "1265"),
    FieldDefinition("StatSnapshot", "stat_danmu", "弹幕量", "连续", "抓取时视频累计弹幕量。", "4089"),
    FieldDefinition("StatSnapshot", "stat_dislike", "点踩量", "连续", "抓取时视频累计点踩或负反馈数量。", "14"),
    FieldDefinition("StatSnapshot", "stat_his_rank", "历史排行", "连续", "视频在历史榜单中的排序编号。", "0"),
    FieldDefinition("StatSnapshot", "stat_now_rank", "当前排行", "连续", "视频在当前榜单中的排序编号。", "0"),
    FieldDefinition("StatSnapshot", "stat_evaluation", "热度评语", "文本", "平台返回的热度评价或榜单说明文本。", "百万播放"),
    FieldDefinition("StatSnapshot", "source_pool", "来源池标记", "离散", "该条统计快照来自哪个任务来源。", "video_pool"),
    FieldDefinition("VideoTagItem", "tag_id", "标签 ID", "离散", "单个视频标签的唯一编号。", "123456"),
    FieldDefinition("VideoTagItem", "tag_name", "标签名称", "文本", "单个视频标签的显示名称。", "Python"),
    FieldDefinition("VideoTagItem", "tag_type", "标签类型", "离散", "标签在平台中的类型编号。", "0"),
    FieldDefinition("VideoTagItem", "jump_url", "标签跳转链接", "文本", "点击标签后跳转的站内链接。", "https://www.bilibili.com/v/topic/detail/?topic_id=123456"),
    FieldDefinition("VideoTagItem", "music_id", "音乐标签 ID", "离散", "若标签与音乐内容绑定，则记录对应音乐 ID。", "0"),
    FieldDefinition("PageInfo", "cid", "分 P CID", "离散", "单个分 P 对应的 CID。", "1486239001"),
    FieldDefinition("PageInfo", "page", "分 P 序号", "连续", "单个分 P 在稿件中的顺序编号。", "1"),
    FieldDefinition("PageInfo", "part", "分 P 标题", "文本", "单个分 P 的标题或片段名称。", "正片"),
    FieldDefinition("PageInfo", "duration", "分 P 时长（秒）", "连续", "单个分 P 的视频时长，单位为秒。", "314"),
    FieldDefinition("PageInfo", "from", "分 P 来源", "离散", "单个分 P 的来源类型标记。", "vupload"),
    FieldDefinition("PageInfo", "dimension", "分 P 分辨率对象", "复合", "单个分 P 的宽高与旋转信息对象。", '{"width": 1920, "height": 1080, "rotate": 0}'),
    FieldDefinition("PageInfo", "weblink", "分 P 网页链接", "文本", "单个分 P 的网页访问链接。", "https://www.bilibili.com/video/BV1xx411c7mD?p=1"),
    FieldDefinition("PageInfo", "vid", "分 P 外部视频 ID", "文本", "分 P 对应的外部视频 ID 或留空值。", ""),
    FieldDefinition("VideoRights", "download", "是否允许下载", "布尔", "视频权限对象中的下载开关。", "True"),
    FieldDefinition("VideoRights", "no_reprint", "是否禁止转载", "布尔", "视频权限对象中的转载限制开关。", "True"),
    FieldDefinition("VideoRights", "is_cooperation", "是否联合投稿", "布尔", "视频权限对象中的联合投稿标记。", "True"),
    FieldDefinition("VideoRights", "is_stein_gate", "是否互动视频", "布尔", "视频权限对象中的互动视频标记。", "False"),
    FieldDefinition("VideoRights", "is_360", "是否全景播放", "布尔", "视频权限对象中的全景视频标记。", "False"),
    FieldDefinition("VideoRights", "pay", "是否付费观看", "布尔", "视频权限对象中的付费观看标记。", "False"),
    FieldDefinition("VideoRights", "arc_pay", "是否支持稿件付费", "布尔", "视频权限对象中的稿件付费标记。", "False"),
    FieldDefinition("VideoRights", "ugc_pay", "是否充电专属", "布尔", "视频权限对象中的 UGC 付费标记。", "False"),
    FieldDefinition("VideoRights", "free_watch", "是否限时免费", "布尔", "视频权限对象中的限时免费标记。", "False"),
    FieldDefinition("VideoRights", "no_share", "是否禁止分享", "布尔", "视频权限对象中的分享限制标记。", "False"),
    FieldDefinition("DownloadSnapshot", "bvid", "视频唯一标识", "离散", "下载信息对应的视频 BVID。", "BV1xx411c7mD"),
    FieldDefinition("DownloadSnapshot", "cid", "下载对应 CID", "离散", "下载信息对应的分 P CID。", "1486239001"),
    FieldDefinition("DownloadSnapshot", "timelength", "媒体时长（毫秒）", "连续", "下载接口返回的媒体总时长，单位为毫秒。", "314000"),
    FieldDefinition("DownloadSnapshot", "quality", "当前清晰度编号", "离散", "当前选中播放流的清晰度编号。", "80"),
    FieldDefinition("DownloadSnapshot", "accept_quality", "可选清晰度列表", "复合", "当前视频支持的清晰度编号列表。", "[16, 32, 64, 80, 112]"),
    FieldDefinition("DownloadSnapshot", "accept_description", "可选清晰度说明", "复合", "当前视频支持的清晰度说明列表。", '["360P", "480P", "720P", "1080P"]'),
    FieldDefinition("DownloadSnapshot", "format", "当前播放格式", "文本", "当前下载接口返回的主格式名称。", "flv720"),
    FieldDefinition("DownloadSnapshot", "video_codecid", "当前视频编码 ID", "离散", "当前播放流使用的视频编码编号。", "7"),
    FieldDefinition("DownloadSnapshot", "support_formats", "清晰度格式对象列表", "复合", "视频支持的格式与清晰度对象列表。", '[{"quality": 80, "format": "flv720", "codecs": ["avc1.640028"]}]'),
    FieldDefinition("DownloadSnapshot", "video_streams", "视频流对象列表", "复合", "下载接口返回的视频流对象列表。", '[{"id": 80, "bandwidth": 1250000, "codecs": "avc1.640028"}]'),
    FieldDefinition("DownloadSnapshot", "audio_streams", "音频流对象列表", "复合", "下载接口返回的音频流对象列表。", '[{"id": 30280, "bandwidth": 192000, "codecs": "mp4a.40.2"}]'),
    FieldDefinition("StreamFormatItem", "quality", "清晰度编号", "离散", "单个可选格式对应的清晰度编号。", "80"),
    FieldDefinition("StreamFormatItem", "format", "格式标记", "文本", "单个可选格式的格式名称。", "flv720"),
    FieldDefinition("StreamFormatItem", "codecs", "编码列表", "复合", "该格式支持的编码信息列表。", '["avc1.640028"]'),
    FieldDefinition("StreamFormatItem", "display_desc", "展示文案", "文本", "前端展示的清晰度说明。", "1080P 高清"),
    FieldDefinition("StreamFormatItem", "new_description", "新描述文案", "文本", "接口返回的新版清晰度描述。", "1080P"),
    FieldDefinition("StreamFormatItem", "superscript", "角标文案", "文本", "清晰度角标或会员限制文案。", "大会员"),
    FieldDefinition("StreamFormatItem", "can_watch_qn_reason", "可观看原因", "文本", "当前账号可观看该清晰度的原因说明。", ""),
    FieldDefinition("StreamFormatItem", "limit_watch_reason", "受限原因", "文本", "当前账号受限无法观看该清晰度的原因。", ""),
    FieldDefinition("StreamItem", "id", "流 ID", "离散", "单条音视频流对应的流编号。", "80"),
    FieldDefinition("StreamItem", "bandwidth", "码率", "连续", "单条音视频流的带宽或码率信息。", "1250000"),
    FieldDefinition("StreamItem", "codecid", "编码 ID", "离散", "单条音视频流的编码编号。", "7"),
    FieldDefinition("StreamItem", "codecs", "编码名称", "文本", "单条音视频流的编码名称。", "avc1.640028"),
    FieldDefinition("StreamItem", "width", "流宽度", "连续", "单条视频流对应的画面宽度。", "1920"),
    FieldDefinition("StreamItem", "height", "流高度", "连续", "单条视频流对应的画面高度。", "1080"),
    FieldDefinition("StreamItem", "mime_type", "流 MIME 类型", "离散", "单条音视频流的 MIME 类型。", "video/mp4"),
    FieldDefinition("StreamItem", "base_url", "主流地址", "文本", "单条音视频流的主下载地址。", "https://upos-sz-mirrorcoso1.bilivideo.com/upgcxcode/example.m4s"),
    FieldDefinition("StreamItem", "backup_url", "备用流地址列表", "复合", "单条音视频流的备用下载地址列表。", '["https://cn-gotcha.example.com/example.m4s"]'),
    FieldDefinition("VideoChargeSnapshot", "bvid", "视频唯一标识", "离散", "充电信息对应的视频 BVID。", "BV1xx411c7mD"),
    FieldDefinition("VideoChargeSnapshot", "snapshot_time", "充电抓取时间", "时间", "本次充电信息的抓取时间。", "2026-03-15T14:35:10"),
    FieldDefinition("VideoChargeSnapshot", "count", "当前充电人数", "连续", "接口当前返回的充电人数。", "36"),
    FieldDefinition("VideoChargeSnapshot", "total_count", "累计充电人数", "连续", "视频累计充电人数。", "128"),
    FieldDefinition("VideoChargeSnapshot", "display_num", "前端展示人数", "文本", "平台前端显示的充电人数文本。", "128人充电"),
    FieldDefinition("VideoChargeSnapshot", "show_info", "展示信息对象", "复合", "平台用于展示充电信息的结构化对象。", '{"show": true}'),
    FieldDefinition("VideoChargeSnapshot", "charger_list", "充电用户列表", "复合", "参与充电的用户对象列表。", '[{"mid": 55667788, "uname": "科研小助手"}]'),
    FieldDefinition("ChargeUserItem", "mid", "充电用户 ID", "离散", "单个充电用户的账号 ID。", "55667788"),
    FieldDefinition("ChargeUserItem", "uname", "充电用户名", "文本", "单个充电用户的昵称。", "科研小助手"),
    FieldDefinition("ChargeUserItem", "rank", "充电排名", "连续", "用户在当前视频充电列表中的排序。", "1"),
    FieldDefinition("ChargeUserItem", "avatar", "充电用户头像 URL", "文本", "单个充电用户头像图片的访问地址。", "https://i0.hdslb.com/bfs/face/example.jpg"),
    FieldDefinition("ChargeUserItem", "message", "充电留言", "文本", "用户充电时附带的留言文本。", "继续更新，支持你！"),
    FieldDefinition("ChargeUserItem", "trend_type", "充电趋势类型", "离散", "平台返回的充电趋势状态编号。", "0"),
    FieldDefinition("UploaderProfile", "mid", "作者 ID", "离散", "上传用户的 UID。", "12345678"),
    FieldDefinition("UploaderProfile", "name", "作者昵称", "文本", "上传用户的公开昵称。", "数据实验室"),
    FieldDefinition("UploaderProfile", "sex", "作者性别", "离散", "上传用户公开展示的性别。", "男"),
    FieldDefinition("UploaderProfile", "sign", "作者签名", "文本", "上传用户空间展示的个性签名。", "长期分享数据分析实验。"),
    FieldDefinition("UploaderProfile", "face", "作者头像 URL", "文本", "上传用户头像图片的访问地址。", "https://i0.hdslb.com/bfs/face/example.jpg"),
    FieldDefinition("UploaderProfile", "top_photo", "空间横幅 URL", "文本", "上传用户空间顶部横幅图片地址。", "https://i0.hdslb.com/bfs/space/example.png"),
    FieldDefinition("UploaderProfile", "level", "作者账号等级", "连续", "上传用户的账号等级。", "6"),
    FieldDefinition("UploaderProfile", "rank", "作者站内 rank", "连续", "上传用户在站内接口中的 rank 值。", "10000"),
    FieldDefinition("UploaderProfile", "official", "认证信息对象", "复合", "上传用户的官方认证信息对象。", '{"type": 0, "title": "知名 UP 主"}'),
    FieldDefinition("UploaderProfile", "vip", "会员信息对象", "复合", "上传用户的会员类型与有效状态对象。", '{"type": 2, "status": 1}'),
    FieldDefinition("UploaderProfile", "fans_badge", "是否展示粉丝徽章", "布尔", "上传用户是否展示粉丝徽章。", "True"),
    FieldDefinition("UploaderProfile", "fans_medal", "粉丝勋章对象", "复合", "上传用户当前佩戴或展示的粉丝勋章信息。", '{"show": true, "medal": {"level": 4}}'),
    FieldDefinition("UploaderProfile", "live_room", "直播间信息对象", "复合", "上传用户直播间的状态与房间信息对象。", '{"roomid": 9454253, "liveStatus": 0}'),
    FieldDefinition("UploaderProfile", "profession", "职业信息对象", "复合", "上传用户公开展示的职业或机构信息对象。", '{"name": "", "department": ""}'),
    FieldDefinition("UploaderProfile", "user_honour_info", "用户荣誉对象", "复合", "上传用户获得的站内荣誉信息对象。", '{"mid": 12345678, "tags": []}'),
    FieldDefinition("UploaderRelationSnapshot", "mid", "作者 ID", "离散", "关系统计对应的作者 UID。", "12345678"),
    FieldDefinition("UploaderRelationSnapshot", "snapshot_time", "关系抓取时间", "时间", "本次作者关系数据的采集时间。", "2026-03-15T14:32:11"),
    FieldDefinition("UploaderRelationSnapshot", "follower", "作者粉丝数", "连续", "作者当前粉丝数量。", "245000"),
    FieldDefinition("UploaderRelationSnapshot", "following", "作者关注数", "连续", "作者当前关注的用户数量。", "512"),
    FieldDefinition("UploaderRelationSnapshot", "whisper", "悄悄关注数", "连续", "作者账号的悄悄关注数量。", "0"),
    FieldDefinition("UploaderRelationSnapshot", "black", "黑名单数", "连续", "作者账号黑名单相关数量。", "0"),
    FieldDefinition("UploaderOverviewStat", "mid", "作者 ID", "离散", "概览统计对应的作者 UID。", "12345678"),
    FieldDefinition("UploaderOverviewStat", "snapshot_time", "概览抓取时间", "时间", "本次作者空间概览数据的采集时间。", "2026-03-15T14:32:11"),
    FieldDefinition("UploaderOverviewStat", "video", "公开视频数量", "连续", "作者空间公开视频数量。", "318"),
    FieldDefinition("UploaderOverviewStat", "article", "专栏数量", "连续", "作者空间专栏内容数量。", "12"),
    FieldDefinition("UploaderOverviewStat", "album", "相册数量", "连续", "作者空间相册内容数量。", "33"),
    FieldDefinition("UploaderOverviewStat", "audio", "音频数量", "连续", "作者空间音频投稿数量。", "8"),
    FieldDefinition("UploaderOverviewStat", "bangumi", "追番/影视数量", "连续", "作者空间番剧或影视相关数量。", "22"),
    FieldDefinition("UploaderOverviewStat", "favourite", "收藏夹数量", "复合", "作者空间收藏夹主客体数量对象。", '{"master": 3, "guest": 3}'),
    FieldDefinition("UploaderOverviewStat", "channel", "合集数量", "复合", "作者空间合集或系列数量对象。", '{"master": 11, "guest": 11}'),
    FieldDefinition("UploaderOverviewStat", "tag", "关注标签数", "连续", "作者关注的标签或话题数量。", "5"),
    FieldDefinition("UploaderOverviewStat", "opus", "图文动态数量", "连续", "作者空间图文动态相关数量。", "39"),
    FieldDefinition("UploaderOverviewStat", "season_num", "系列数量", "连续", "作者空间季或系列相关数量。", "8"),
    FieldDefinition("CommentSnapshot", "bvid", "视频唯一标识", "离散", "评论快照对应的视频 BVID。", "BV1xx411c7mD"),
    FieldDefinition("CommentSnapshot", "snapshot_time", "评论抓取时间", "时间", "最热评论快照的采集时间。", "2026-03-15T14:33:02"),
    FieldDefinition("CommentSnapshot", "limit", "评论抓取条数上限", "连续", "本次计划抓取的最热评论条数。", "10"),
    FieldDefinition("CommentSnapshot", "comment_top10", "最热评论列表", "复合", "按热度排序截取的评论对象列表。", '[{"rpid": 99887766, "message": "讲得很清楚"}]'),
    FieldDefinition("CommentItem", "rpid", "评论 ID", "离散", "单条评论的唯一回复 ID。", "99887766"),
    FieldDefinition("CommentItem", "message", "评论内容", "文本", "评论正文文本。", "讲得很清楚，期待下一期。"),
    FieldDefinition("CommentItem", "like", "评论点赞数", "连续", "该条评论当前获得的点赞量。", "37"),
    FieldDefinition("CommentItem", "ctime", "评论发布时间", "时间", "该条评论的发布时间。", "2026-03-15T14:28:45"),
    FieldDefinition("CommentItem", "mid", "评论用户 ID", "离散", "发表评论用户的账号 ID。", "55667788"),
    FieldDefinition("CommentItem", "uname", "评论用户名", "文本", "发表评论用户的昵称。", "科研小助手"),
    FieldDefinition("MediaResult", "bvid", "视频唯一标识", "离散", "媒体抓取结果对应的视频 BVID。", "BV1xx411c7mD"),
    FieldDefinition("MediaResult", "cid", "媒体对应 CID", "离散", "此次媒体抓取使用的播放段 CID。", "1486239001"),
    FieldDefinition("MediaResult", "video_object_key", "视频对象键", "文本", "视频流文件在 GCS 中的对象 key。", "bilibili-media/BV1xx411c7mD/1486239001/video.m4s"),
    FieldDefinition("MediaResult", "audio_object_key", "音频对象键", "文本", "音频流文件在 GCS 中的对象 key。", "bilibili-media/BV1xx411c7mD/1486239001/audio.m4s"),
    FieldDefinition("MediaResult", "video_format_selected", "视频格式选择结果", "文本", "最终选中的视频清晰度或编码格式描述。", "1920x1080_H264"),
    FieldDefinition("MediaResult", "audio_format_selected", "音频格式选择结果", "文本", "最终选中的音频码率或编码格式描述。", "AUDIO_192K"),
    FieldDefinition("MediaResult", "upload_session_id", "媒体抓取会话 ID", "离散", "同一轮媒体下载/上传流程的会话标识。", "media-20260315-143355-BV1xx411c7mD"),
    FieldDefinition("MediaResult", "video_asset", "视频资产对象", "复合", "视频流资产的结构化元数据对象。", '{"asset_type": "video", "file_size": 24876543}'),
    FieldDefinition("MediaResult", "audio_asset", "音频资产对象", "复合", "音频流资产的结构化元数据对象。", '{"asset_type": "audio", "file_size": 4321987}'),
    FieldDefinition("MediaAssetRef", "asset_type", "媒体类型", "离散", "当前资产是视频流还是音频流。", "video"),
    FieldDefinition("MediaAssetRef", "storage_backend", "存储后端", "离散", "媒体文件实际写入的存储位置类型。", "gcs"),
    FieldDefinition("MediaAssetRef", "object_key", "对象键", "文本", "媒体对象在存储中的完整 key。", "bilibili-media/BV1xx411c7mD/1486239001/video.m4s"),
    FieldDefinition("MediaAssetRef", "format_selected", "选中媒体格式", "文本", "抓取阶段最终选中的媒体格式。", "1920x1080_H264"),
    FieldDefinition("MediaAssetRef", "mime_type", "媒体 MIME 类型", "离散", "媒体文件对应的 MIME 类型。", "video/mp4"),
    FieldDefinition("MediaAssetRef", "file_size", "文件大小", "连续", "媒体文件字节大小。", "24876543"),
    FieldDefinition("MediaAssetRef", "sha256", "文件哈希值", "文本", "媒体文件内容的 SHA256 摘要。", "7bc3c1d8f1a0b21f..."),
    FieldDefinition("MediaAssetRef", "chunk_count", "分块数量", "连续", "该媒体文件被切分存储的块数。", "7"),
    FieldDefinition("MediaAssetRef", "bucket_name", "GCS Bucket 名称", "离散", "若使用 GCS，则记录目标 Bucket 名称。", "thesis-bilibili-media"),
    FieldDefinition("MediaAssetRef", "storage_endpoint", "存储端点", "文本", "当前媒体对象所在的 GCS API Endpoint。", "https://storage.googleapis.com"),
    FieldDefinition("MediaAssetRef", "object_url", "对象访问 URL", "文本", "媒体对象的 `gs://` URI 或公开访问 URL。", "gs://thesis-bilibili-media/bilibili-media/BV1xx411c7mD/1486239001/video.m4s"),
    FieldDefinition("MediaAssetRef", "etag", "对象 ETag", "文本", "对象上传后返回的 ETag。", "F0E1D2C3B4A59687"),
)


SECTION_TITLES = {
    "MetaResult": "MetaResult：视频基础元数据",
    "VideoTagItem": "VideoTagItem：单个视频标签对象",
    "PageInfo": "PageInfo：单个分 P 信息对象",
    "VideoRights": "VideoRights：视频权限对象",
    "StatSnapshot": "StatSnapshot：视频统计快照",
    "DownloadSnapshot": "DownloadSnapshot：下载与清晰度信息",
    "StreamFormatItem": "StreamFormatItem：单个清晰度格式对象",
    "StreamItem": "StreamItem：单个音视频流对象",
    "VideoChargeSnapshot": "VideoChargeSnapshot：视频充电信息快照",
    "ChargeUserItem": "ChargeUserItem：单个充电用户对象",
    "UploaderProfile": "UploaderProfile：上传用户资料对象",
    "UploaderRelationSnapshot": "UploaderRelationSnapshot：上传用户关系快照",
    "UploaderOverviewStat": "UploaderOverviewStat：上传用户空间概览统计",
    "CommentSnapshot": "CommentSnapshot：评论快照",
    "CommentItem": "CommentItem：单条评论对象",
    "MediaResult": "MediaResult：媒体抓取结果",
    "MediaAssetRef": "MediaAssetRef：单个媒体资产对象",
}


def build_field_reference_rows() -> list[dict[str, str]]:
    return [
        {
            "数据模块": item.section,
            "变量名称": item.variable_name,
            "特征名称": item.feature_name,
            "特征类型": item.feature_type,
            "描述": item.description,
            "具体数据或内容": item.example_value,
        }
        for item in FIELD_DEFINITIONS
    ]


def build_field_reference_markdown() -> str:
    lines = [
        "## Bilibili Video Data Crawler 字段定义",
        "",
        "本文档用于直观展示 `Bilibili Video Data Crawler` 当前对单条视频可抓取的主要字段定义。",
        "文档内容与前端“字段定义”页面共用同一份字段字典；后续若新增字段或数据类型，应优先更新 `src/bili_pipeline/field_reference.py`。",
        "",
        "### 字段说明列",
        "",
        "- `变量名称`：程序中的字段 key。",
        "- `特征名称`：便于阅读和分析的中文名称。",
        "- `特征类型`：当前使用 `离散 / 连续 / 文本 / 布尔 / 时间 / 复合` 六类标注。",
        "- `描述`：说明该字段表示什么。",
        "- `具体数据或内容`：列举单条视频可能出现的一种实际值。",
        "",
    ]

    for section_name, section_title in SECTION_TITLES.items():
        section_rows = [item for item in FIELD_DEFINITIONS if item.section == section_name]
        lines.extend(
            [
                f"### {section_title}",
                "",
                "| 变量名称 | 特征名称 | 特征类型 | 描述 | 具体数据或内容 |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for item in section_rows:
            lines.append(
                f"| `{item.variable_name}` | {item.feature_name} | {item.feature_type} | {item.description} | `{item.example_value}` |"
            )
        lines.append("")

    lines.extend(
        [
            "### 维护约定",
            "",
            "- 该文档由统一字段字典生成，默认与 `bvd-crawler` 前端展示保持一致。",
            "- 当新增新的抓取结果结构、补充已有字段、或调整字段含义时，应同步更新 `src/bili_pipeline/field_reference.py`。",
            "- 若后续扩展出新的数据模块，建议新增独立 section，并继续沿用同样的五列展示格式。",
            "",
        ]
    )
    return "\n".join(lines)


def sync_field_reference_markdown(target_path: str | Path) -> Path:
    doc_path = Path(target_path)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    content = build_field_reference_markdown()
    if not doc_path.exists() or doc_path.read_text(encoding="utf-8") != content:
        doc_path.write_text(content, encoding="utf-8")
    return doc_path
