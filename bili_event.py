"""将 B站消息构造为 AstrBot 消息事件，走完整管道处理。

本模块不修改 AstrBot 核心代码。它把一条 B站评论/@评论/私信包装成一条
"私聊消息"事件，投递到 AstrBot 的 EventBus 队列中，从而复用完整的消息
处理管道：人设、技能、知识库、插件事件钩子、回复前缀、安全审查、会话
历史等。

不同来源（视频评论/@评论/私信）使用不同的平台 id（bilibili_video 等），
配合 AstrBot 的配置路由（UmopConfigRouter），可以把各来源绑定到不同的
AstrBot 配置文件（conf），从而各自使用独立的人设、知识库、提供商等。
回复结果通过重写 send() 转投回 B站。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

from astrbot import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.core.message.components import Plain, Record
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.utils.metrics import Metric

from .bili_client import BiliClient, BiliIncoming
from .reply_engine import ReplyEngine

# 消息来源 → 平台 id。
# 平台 id 会作为 unified_msg_origin 的第一段（如 bilibili_video:FriendMessage:...），
# 插件据此在配置路由表中注册 bilibili_video:: 等路由，把各来源分发到不同的
# AstrBot 配置文件。
SOURCE_PLATFORM_IDS = {
    "video_comment": "bilibili_video",
    "mention": "bilibili_mention",
    "private_message": "bilibili_private",
}


def _platform_meta_for(kind: str) -> PlatformMetadata:
    pid = SOURCE_PLATFORM_IDS.get(kind, "bilibili")
    return PlatformMetadata(
        name=pid,
        description="Bilibili Chat",
        id=pid,
        support_streaming_message=False,
        support_proactive_message=False,
    )


def _chain_text(chain) -> str:
    """从消息链中提取可发送的纯文本。

    管道装饰阶段（TTS）可能把 Plain 替换为 Record（音频），Record 仍保留原始
    文本；t2i 会替换为 Image，B站评论不支持图片，忽略即可。
    """
    components = chain.chain if isinstance(chain, MessageChain) else chain
    parts: list[str] = []
    for comp in components:
        if isinstance(comp, Plain):
            parts.append(comp.text or "")
        elif isinstance(comp, Record):
            parts.append(getattr(comp, "text", "") or "")
    return "".join(parts)


class BilibiliMessageEvent(AstrMessageEvent):
    """B站消息事件。

    该事件被投递到 AstrBot 管道后，经过 9 个阶段处理，最终由 RespondStage
    调用 send() 把回复发送到 B站。
    """

    _bili: BiliClient
    _incoming: BiliIncoming
    _reply_engine: ReplyEngine

    async def send(self, message: MessageChain) -> None:
        """管道 RespondStage 调用此处，将回复发送到 B站。"""
        asyncio.create_task(
            Metric.upload(msg_event_tick=1, adapter_name=self.platform_meta.name)
        )
        self._has_send_oper = True

        text = _chain_text(message).strip()
        if not text:
            logger.info(
                "B站回复内容为空或仅包含图片等媒体，跳过发送。事件=%s。",
                self._incoming.event_key,
            )
            return

        # 与近期已发送的回复去重，避免复读
        if await self._reply_engine.should_skip_duplicate_reply(text):
            logger.warning(
                "LLM 生成了近期重复回复，跳过发送。事件=%s。",
                self._incoming.event_key,
            )
            return

        text = text[: self._reply_engine.max_reply_chars].strip()
        if not text:
            return

        try:
            await self._reply_engine.send_limited(
                lambda: self._bili.reply(self._incoming, text)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # 发送失败：事件已在入队时标记为已处理，避免无限重试；记录日志供排查。
            logger.exception(
                "B站回复发送失败，事件=%s。该消息将不会自动重试。",
                self._incoming.event_key,
            )
            return

        await self._reply_engine.mark_processed(self._incoming.event_key, text)
        logger.info(
            "已回复%s，用户 UID=%s，事件=%s。",
            self._incoming.source_label,
            self._incoming.sender_uid,
            self._incoming.event_key,
        )

    async def send_streaming(
        self,
        generator: AsyncGenerator[MessageChain, None],
        use_fallback: bool = False,
    ) -> None:
        """B站不支持流式输出；累积全部片段后一次性发送。"""
        self._has_send_oper = True
        parts: list[str] = []
        async for chain in generator:
            parts.append(_chain_text(chain))
        text = "".join(parts).strip()
        if text:
            await self.send(MessageChain([Plain(text)]))


def build_bili_event(
    incoming: BiliIncoming,
    bili: BiliClient,
    reply_engine: ReplyEngine,
) -> BilibiliMessageEvent:
    """把一条 B站消息包装成走完整管道的 AstrBot 消息事件。"""
    message_obj = AstrBotMessage()
    # 使用私聊类型：默认配置下私聊消息在 WakingCheckStage 中会被自动唤醒，
    # 无需消息以唤醒词开头即可触发 LLM。
    message_obj.type = MessageType.FRIEND_MESSAGE
    message_obj.self_id = str(bili.self_uid)
    # 每个 B站用户独立会话，保证对话历史与限流互不干扰。
    message_obj.session_id = f"uid{incoming.sender_uid}"
    message_obj.sender = MessageMember(
        user_id=str(incoming.sender_uid),
        nickname=incoming.sender_name,
    )
    message_obj.message = [Plain(incoming.text)]
    message_obj.message_str = incoming.text
    message_obj.message_id = f"bili-{incoming.event_key}"
    message_obj.raw_message = None

    event = BilibiliMessageEvent(
        message_str=message_obj.message_str,
        message_obj=message_obj,
        platform_meta=_platform_meta_for(incoming.kind),
        session_id=message_obj.session_id,
    )
    event._incoming = incoming
    event._bili = bili
    event._reply_engine = reply_engine
    # 强制关闭流式输出（管道 internal.py 会读取该 extra 决定是否流式）。
    event.set_extra("enable_streaming", False)
    return event
