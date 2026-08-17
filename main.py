"""AstrBot plugin entry point for Bilibili AI replies."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.star.filter.command import GreedyStr

from .bili_client import AuthenticationRequired, BiliClient, BiliIncoming
from .bili_event import SOURCE_PLATFORM_IDS, build_bili_event
from .reply_engine import ReplyEngine

# 平台 id（路由前缀） → 插件配置 conf_routing 中的字段名。
# 每个来源都注册为 AstrBot 的一条配置路由（如 bilibili_video:: -> conf_id），
# 使不同来源（视频评论/@评论/私信）走不同的 AstrBot 配置文件（人设、知识库、模型等）。
SOURCE_ROUTE_FIELDS = (
    ("bilibili_video", "video_comments_conf"),
    ("bilibili_mention", "mentions_conf"),
    ("bilibili_private", "private_messages_conf"),
)

# /bili route 命令中允许使用的来源别名 → 平台 id。
SOURCE_ALIASES = {
    "video": "bilibili_video",
    "mention": "bilibili_mention",
    "private": "bilibili_private",
}


@register(
    "astrbot_plugin_bilibili_chat",
    "astrbot-community",
    "B站评论、@评论和私信 AI 自动回复",
    "0.3.0",
)
class BilibiliChatPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.logger = logger
        self.config = config
        self.data_dir: Path = Path()
        self.bili: BiliClient | None = None
        self.reply_engine: ReplyEngine | None = None
        self.poll_task: asyncio.Task[None] | None = None
        self._poll_lock = asyncio.Lock()
        self._login_lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_bilibili_chat")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        polling = self._section("polling")
        risk = self._section("risk_control")
        reply_config = {**self._section("llm"), **risk}
        self.reply_engine = ReplyEngine(
            self.context,
            self.data_dir,
            reply_config,
            self.logger,
        )
        await self.reply_engine.initialize()

        await self._apply_conf_routing()

        self.bili = BiliClient(
            self.data_dir,
            self._section("auth"),
            polling,
            self.logger,
        )
        try:
            logged_in = await self.bili.initialize()
        except AuthenticationRequired as exc:
            logged_in = False
            self.logger.warning(
                "B站登录不可用：%s。请在 WebUI 更新 Cookie，"
                "或由 AstrBot 管理员执行 /bili login。",
                exc,
            )
        except Exception:
            logged_in = False
            self.logger.exception(
                "初始化 B站登录失败。请检查网络与 Cookie，"
                "或由 AstrBot 管理员执行 /bili login。"
            )

        if not logged_in:
            self.logger.warning(
                "Bilibili Chat 尚未登录。首次使用请在 WebUI 插件配置中"
                "填写 Cookie，或执行 /bili login 扫码。"
            )
        elif bool(self.config.get("enabled", False)):
            self._ensure_poll_task()
        else:
            self.logger.info("Bilibili Chat 已登录，但自动轮询尚未启用。")

    def _ensure_poll_task(self) -> None:
        if self.poll_task is None or self.poll_task.done():
            self.poll_task = asyncio.create_task(
                self._poll_loop(),
                name="astrbot-bilibili-chat-poller",
            )

    async def _poll_loop(self) -> None:
        interval = max(
            30,
            int(self._section("polling").get("interval_seconds", 60)),
        )
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except AuthenticationRequired:
                self.logger.exception(
                    "B站登录已失效，自动轮询暂停本轮；请重新配置 Cookie 或扫码登录。"
                )
            except Exception:
                self.logger.exception("B站轮询发生未处理异常。")
            await asyncio.sleep(interval)

    async def _poll_once(self) -> int:
        if self.bili is None or self.reply_engine is None:
            return 0
        if not self.bili.authenticated:
            raise AuthenticationRequired("尚未登录 B站")

        async with self._poll_lock:
            polling = self._section("polling")
            incoming: list[BiliIncoming] = []
            private_cursor = None

            if polling.get("listen_video_comments", True):
                incoming.extend(await self.bili.poll_video_comments())
            if polling.get("listen_mentions", True):
                incoming.extend(await self.bili.poll_mentions())
            if polling.get("listen_private_messages", True):
                private_messages, private_cursor = (
                    await self.bili.poll_private_messages()
                )
                incoming.extend(private_messages)

            incoming = self.bili._sorted_unique(incoming)
            if (
                polling.get("ignore_existing_on_first_run", True)
                and not self.reply_engine.baseline_ready
            ):
                await self.reply_engine.establish_baseline(
                    [item.event_key for item in incoming]
                )
                if private_cursor is not None:
                    await self.bili.commit_private_cursor(private_cursor)
                self.logger.info(
                    "首次轮询基线已建立，忽略 %d 条已有消息。",
                    len(incoming),
                )
                return 0

            handled = 0
            all_succeeded = True
            for item in incoming:
                succeeded = await self._handle_incoming(item)
                all_succeeded = all_succeeded and succeeded
                handled += int(succeeded)

            if private_cursor is not None and all_succeeded:
                await self.bili.commit_private_cursor(private_cursor)
            return handled

    async def _handle_incoming(self, incoming: BiliIncoming) -> bool:
        """将一条 B站消息包装为 AstrBot 消息事件并投递到管道。

        事件入队后由 AstrBot EventBus 异步处理（完整 9 阶段管道），本方法
        不等待回复生成；去重在入队时立即生效（一次性消费，失败只记日志）。
        """
        assert self.bili is not None
        assert self.reply_engine is not None

        if self.reply_engine.is_processed(incoming.event_key):
            return True
        if self.reply_engine.is_blacklisted(incoming.sender_uid):
            self.logger.info(
                "跳过黑名单用户 UID=%s 的%s。",
                incoming.sender_uid,
                incoming.source_label,
            )
            await self.reply_engine.mark_processed(incoming.event_key)
            return True

        try:
            event = build_bili_event(incoming, self.bili, self.reply_engine)
            self.context.get_event_queue().put_nowait(event)
            await self.reply_engine.mark_processed(incoming.event_key)
            self.logger.info(
                "已将%s（用户 UID=%s）送入 AstrBot 管道处理，事件=%s。",
                incoming.source_label,
                incoming.sender_uid,
                incoming.event_key,
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception(
                "入队处理%s失败，事件=%s。",
                incoming.source_label,
                incoming.event_key,
            )
            return False

    @filter.on_decorating_result()
    async def _on_decorating_result(self, event: AstrMessageEvent):
        """B站评论仅支持文本：阻止 t2i 把长回复替换为图片导致回复丢失。

        该钩子对其它平台无影响（按平台名过滤）。
        """
        if event.get_platform_name() not in SOURCE_PLATFORM_IDS.values():
            return
        result = event.get_result()
        if result is None:
            return
        result.use_t2i_ = False

    @filter.command_group("bili")
    def bili_group(self) -> None:
        """Bilibili Chat 管理命令。"""

    @bili_group.command("help")
    async def bili_help(self, event: AstrMessageEvent):
        yield event.plain_result(
            "Bilibili Chat 命令\n"
            "/bili status - 查看登录与轮询状态\n"
            "/bili login - 管理员扫码登录\n"
            "/bili poll - 管理员立即轮询一次\n"
            "/bili confs - 管理员查看可选的回复配置文件\n"
            "/bili route - 管理员查看/设置各来源使用的回复配置\n\n"
            "首次使用也可以在 WebUI → 插件配置中填写 B站 Cookie，"
            "保存后重载插件，再启用自动轮询。"
        )

    @bili_group.command("status")
    async def bili_status(self, event: AstrMessageEvent):
        logged_in = bool(self.bili and self.bili.authenticated)
        polling = bool(self.poll_task and not self.poll_task.done())
        account = (
            f"{self.bili.self_name} (UID {self.bili.self_uid})"
            if logged_in and self.bili
            else "未登录"
        )
        yield event.plain_result(
            "Bilibili Chat 状态\n"
            f"账号：{account}\n"
            f"自动回复配置：{'已启用' if self.config.get('enabled', False) else '未启用'}\n"
            f"轮询任务：{'运行中' if polling else '未运行'}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @bili_group.command("login")
    async def bili_login(self, event: AstrMessageEvent):
        if self.bili is None:
            yield event.plain_result("插件尚未初始化完成，请稍后再试。")
            return
        if self._login_lock.locked():
            yield event.plain_result("已有扫码登录正在进行，请先完成或等待其超时。")
            return
        async with self._login_lock:
            try:
                qr_path = await self.bili.begin_qr_login()
                yield event.plain_result(
                    "请在 3 分钟内使用哔哩哔哩 App 扫描二维码并确认登录。"
                )
                yield event.image_result(str(qr_path))
                state = await self.bili.wait_qr_login()
                if state != "done":
                    yield event.plain_result(
                        "二维码已过期，请重新执行 /bili login。"
                    )
                    return
                if bool(self.config.get("enabled", False)):
                    self._ensure_poll_task()
                yield event.plain_result(
                    f"登录成功：{self.bili.self_name} (UID {self.bili.self_uid})。"
                    "Credential 已持久化到插件数据目录。"
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("B站扫码登录失败。")
                yield event.plain_result("扫码登录失败，请查看 AstrBot 日志。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @bili_group.command("poll")
    async def bili_poll(self, event: AstrMessageEvent):
        try:
            count = await self._poll_once()
            yield event.plain_result(f"轮询完成，本轮处理 {count} 条消息。")
        except Exception as exc:
            self.logger.exception("手动轮询失败。")
            yield event.plain_result(f"轮询失败：{exc}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @bili_group.command("confs")
    async def bili_confs(self, event: AstrMessageEvent):
        """列出 AstrBot 所有可选的回复配置文件。"""
        mgr = self._conf_manager()
        if mgr is None:
            yield event.plain_result("无法访问 AstrBot 配置管理器。")
            return
        lines = ["AstrBot 可用的回复配置文件（名称 | 文件名 | ID）"]
        for conf in mgr.get_conf_list():
            if conf.get("id") == "default":
                lines.append("default | 默认配置 | default")
                continue
            lines.append(
                f"{conf.get('name') or '-'} | {conf.get('path')} | {conf.get('id')}"
            )
        lines.append(
            "提示：在插件配置 → conf_routing 中填写上述名称、文件名或 ID，"
            "即可按来源（视频评论/@/私信）绑定回复配置。"
        )
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @bili_group.command("route")
    async def bili_route(
        self,
        event: AstrMessageEvent,
        action: str = "show",
        source: str = "",
        conf: GreedyStr = GreedyStr(""),
    ):
        """查看/设置/清除各来源的回复配置路由。"""
        mgr = self._conf_manager()
        if mgr is None or getattr(mgr, "ucr", None) is None:
            yield event.plain_result("无法访问 AstrBot 配置管理器。")
            return
        action = (action or "show").strip().lower()
        if action == "show":
            yield event.plain_result(await self._route_status_text())
            return

        platform_id = SOURCE_ALIASES.get(
            (source or "").strip().lower(), (source or "").strip()
        )
        if platform_id not in SOURCE_PLATFORM_IDS.values():
            yield event.plain_result(
                "无效来源。可用：video（视频评论）/ mention（@评论）/ private（私信）。"
            )
            return
        umo = f"{platform_id}::"

        if action == "clear":
            try:
                await mgr.ucr.delete_route(umo)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                yield event.plain_result(f"清除路由失败：{exc}")
                return
            yield event.plain_result(f"已清除 {umo} 的路由，将使用默认配置。")
            return

        if action == "set":
            conf_id, err = self._resolve_conf(str(conf))
            if conf_id is None:
                yield event.plain_result(err or "未指定配置文件。")
                return
            try:
                await mgr.ucr.update_route(umo, conf_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                yield event.plain_result(f"设置路由失败：{exc}")
                return
            yield event.plain_result(f"已设置 {umo} -> {conf_id}。")
            return

        yield event.plain_result(
            "用法：\n"
            "/bili route - 查看当前路由\n"
            "/bili route set <video|mention|private> <配置名/文件名/ID>\n"
            "/bili route clear <video|mention|private>"
        )

    async def terminate(self) -> None:
        if self.poll_task is not None:
            self.poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.poll_task
            self.poll_task = None
        await self._clear_conf_routing()
        self.logger.info("Bilibili Chat 后台轮询任务已停止。")

    def _section(self, name: str) -> dict[str, Any]:
        section = self.config.get(name, {})
        return dict(section) if isinstance(section, dict) else {}

    def _conf_manager(self) -> Any:
        """获取 AstrBot 配置管理器（含配置路由能力），不可用时返回 None。"""
        return getattr(self.context, "astrbot_config_mgr", None)

    def _resolve_conf(self, value: str) -> tuple[str | None, str]:
        """把用户填写的配置（名称/文件名/ID）解析为 conf_id。

        Returns:
            (conf_id, 错误信息)：conf_id 为 None 时表示未解析成功；
            conf_id 为 None 且错误信息为空表示“default/未设置”。
        """
        value = (value or "").strip()
        if not value or value == "default":
            return None, ""
        mgr = self._conf_manager()
        if mgr is None:
            return None, "无法访问 AstrBot 配置管理器。"
        for conf in mgr.get_conf_list():
            if value in (conf.get("path"), conf.get("id"), conf.get("name")):
                return str(conf["id"]), ""
        return None, f"未找到配置「{value}」，可用 /bili confs 查看。"

    async def _apply_conf_routing(self) -> None:
        """根据插件配置把各来源绑定到对应的 AstrBot 配置文件。"""
        mgr = self._conf_manager()
        if mgr is None or getattr(mgr, "ucr", None) is None:
            self.logger.warning(
                "无法访问 AstrBot 配置管理器，跳过回复配置路由注册。"
            )
            return
        routing = self._section("conf_routing")
        for platform_id, field in SOURCE_ROUTE_FIELDS:
            umo = f"{platform_id}::"
            value = str((routing or {}).get(field, "")).strip()
            if not value or value == "default":
                try:
                    await mgr.ucr.delete_route(umo)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger.exception("清除配置路由 %s 失败。", umo)
                continue
            conf_id, err = self._resolve_conf(value)
            if conf_id is None:
                self.logger.warning("配置路由 %s 未生效：%s", umo, err)
                continue
            try:
                await mgr.ucr.update_route(umo, conf_id)
                self.logger.info("配置路由 %s -> %s", umo, conf_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("注册配置路由 %s 失败。", umo)

    async def _clear_conf_routing(self) -> None:
        """清除本插件注册的三条配置路由（插件卸载/重载时调用）。"""
        mgr = self._conf_manager()
        if mgr is None or getattr(mgr, "ucr", None) is None:
            return
        for platform_id, _field in SOURCE_ROUTE_FIELDS:
            try:
                await mgr.ucr.delete_route(f"{platform_id}::")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.debug(
                    "清理配置路由 %s 时发生异常（可忽略）。", platform_id
                )

    async def _route_status_text(self) -> str:
        mgr = self._conf_manager()
        if mgr is None:
            return "无法访问 AstrBot 配置管理器。"
        name_by_id: dict[str, str] = {}
        for conf in mgr.get_conf_list():
            name_by_id[str(conf.get("id"))] = (
                conf.get("name") or conf.get("path") or str(conf.get("id"))
            )
        routes = dict(
            getattr(getattr(mgr, "ucr", None), "umop_to_conf_id", {}) or {}
        )
        lines = ["Bilibili Chat 回复配置路由（来源 | 配置）"]
        for platform_id, _field in SOURCE_ROUTE_FIELDS:
            conf_id = routes.get(f"{platform_id}::")
            if not conf_id:
                lines.append(f"{platform_id} | （默认）")
            else:
                lines.append(f"{platform_id} | {name_by_id.get(conf_id, conf_id)} ({conf_id})")
        lines.append(
            "说明：可在插件配置 → conf_routing 中为每个来源填写配置名或"
            "abconf_*.json 文件名，也可用 /bili route set 临时调整。"
        )
        return "\n".join(lines)
