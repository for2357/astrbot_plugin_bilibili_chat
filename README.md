# astrbot_plugin_bilibili_chat

在 AstrBot 中轮询 B站视频评论、@评论和私信，调用 AstrBot 已配置的
LLM 生成短回复，再通过 `bilibili-api-python` 异步发送。

> 当前版本为 v1.0.0 初始发布，后续将持续优化直至插件稳定正常工作。
> 欢迎提交 Issue 或 PR 参与改进。
> 本插件代码 100% 由 AI 生成。

## 功能

- Cookie 登录与 `/bili login` 扫码登录
- Credential、私信游标和回复去重状态持久化
- 监听自己最近投稿的新评论、@自己的评论、新私信
- 使用默认 LLM，或在 WebUI 中指定 LLM Provider
- 跨轮询去重、近期回复文本去重、UID 黑名单
- 滑动窗口限流，代码层面强制不超过 10 条/分钟
- 首次运行默认只建立基线，不回复历史消息

## 安装

将本目录放入：

```text
AstrBot/data/plugins/astrbot_plugin_bilibili_chat/
```

安装依赖并在 WebUI 中重载插件：

```bash
pip install -r requirements.txt
```

要求 Python 3.10+、AstrBot 4.16+。AstrBot 本身必须已经配置至少一个可用
的聊天 LLM Provider。

## 首次配置

1. 打开 AstrBot WebUI 的插件配置页。
2. 在「B站登录」中填写完整 Cookie。至少需要 `SESSDATA` 和
   `bili_jct`，建议同时提供 `DedeUserID`、`buvid3`、`buvid4`。
3. 配置人设、轮询间隔、黑名单与限流。
4. 保存并重载插件，确认 `/bili status` 显示已登录。
5. 开启「启用自动轮询与回复」，再次重载插件。

也可以不填写 Cookie，由 AstrBot 管理员在聊天中执行：

```text
/bili login
```

插件会发送二维码，扫码确认后将 Credential 保存到 AstrBot 插件数据目录。
二维码登录得到的 Credential 优先在没有 WebUI Cookie 时使用；WebUI Cookie
非空时始终优先。

管理命令：

```text
/bili help
/bili status
/bili login
/bili poll
```

`login` 和 `poll` 仅限 AstrBot 管理员。

## 数据与安全

运行状态位于 AstrBot 的插件数据目录中：

- `credential.json`：扫码或 Cookie 规范化后的登录凭据
- `bili_poll_state.json`：私信轮询游标
- `reply_state.json`：已处理事件和近期回复哈希

请限制 AstrBot 数据目录的文件权限，不要把 Cookie、二维码或
`credential.json` 提交到 Git。插件日志不会主动输出 Cookie。

自动回复属于账号写操作。建议先保持 `enabled=false`，用 `/bili poll`
观察日志；确认人设和黑名单正确后再开启。默认轮询间隔为 60 秒，避免设置得
过低。

## 实现说明

`main.py` 在 `initialize()` 中创建后台 `asyncio.Task`，并在
`terminate()` 中取消和回收任务。插件命令通过 AstrBot 当前的
`event.plain_result()` / `event.image_result()` 返回消息链；后台 B站回复
则直接调用异步 SDK。

LLM 默认调用：

```python
provider = context.get_using_provider()
response = await provider.text_chat(...)
```

指定 Provider 时改用 `context.llm_generate()`。所有 B站网络调用均通过
`bilibili-api-python` 的异步 API 并使用 `await`。

## 上游状态说明

截至 2026-07-06，`Nemo2011/bilibili-api` GitHub 仓库已停止维护并关停
源码。本插件固定依赖最后发布到 PyPI 的
`bilibili-api-python==17.4.2`，并把所有 SDK 适配集中在
`bili_client.py`。B站非公开接口未来变化时，评论或私信功能可能失效，届时
应优先替换该适配层，不要在轮询与 LLM 逻辑中散改接口。

使用前请确认符合 B站服务条款、当地法律和账号运营规范，避免骚扰、刷屏或
其他滥用。

## 文件结构

```text
astrbot_plugin_bilibili_chat/
├── metadata.yaml
├── main.py
├── _conf_schema.json
├── bili_client.py
├── reply_engine.py
├── requirements.txt
└── tests/
```
