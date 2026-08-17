"""Async bilibili-api-python facade used by the AstrBot plugin."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bilibili_api import Credential, comment, login_v2, session, user


class AuthenticationRequired(RuntimeError):
    """Raised when no usable Bilibili credential is available."""


@dataclass(slots=True)
class BiliIncoming:
    kind: str
    event_key: str
    sender_uid: int
    sender_name: str
    text: str
    created_at: int
    oid: int | None = None
    root_rpid: int | None = None
    parent_rpid: int | None = None
    talker_id: int | None = None
    resource_type: Any = None

    @property
    def source_label(self) -> str:
        return {
            "video_comment": "视频评论",
            "mention": "@评论",
            "private_message": "B站私信",
        }.get(self.kind, self.kind)


@dataclass(slots=True)
class PrivateCursor:
    max_ts: int
    max_seqno: dict[str, int]


class BiliClient:
    """Keep SDK-specific response handling out of the plugin main loop."""

    def __init__(
        self,
        data_dir: Path,
        auth_config: dict[str, Any],
        polling_config: dict[str, Any],
        logger: Any,
    ) -> None:
        self.data_dir = data_dir
        self.logger = logger
        self.cookie = str(auth_config.get("cookie", "") or "").strip()
        self.max_videos = min(
            30, max(1, int(polling_config.get("max_videos", 5)))
        )
        self.video_refresh_seconds = max(
            60, int(polling_config.get("video_list_refresh_seconds", 300))
        )
        self.credential_path = data_dir / "credential.json"
        self.poll_state_path = data_dir / "bili_poll_state.json"
        self.credential: Credential | None = None
        self.self_uid = 0
        self.self_name = ""
        self._videos: list[tuple[int, str]] = []
        self._videos_loaded_at = 0.0
        self._dm_max_ts = int(time.time() * 1_000_000)
        self._dm_max_seqno: dict[str, int] = {}
        self._qr_login: login_v2.QrCodeLogin | None = None
        self._qr_lock = asyncio.Lock()

    @property
    def authenticated(self) -> bool:
        return self.credential is not None and self.self_uid > 0

    async def initialize(self) -> bool:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        credential = None
        if self.cookie:
            credential = self._credential_from_cookie(self.cookie)
        elif self.credential_path.exists():
            credential = await asyncio.to_thread(self._load_credential)

        if credential is None:
            return False
        await self._activate_credential(credential)
        await asyncio.to_thread(self._load_poll_state)
        if not self.poll_state_path.exists():
            await self._baseline_private_cursor()
        return True

    async def begin_qr_login(self) -> Path:
        async with self._qr_lock:
            qr = login_v2.QrCodeLogin(login_v2.QrCodeLoginChannel.WEB)
            await qr.generate_qrcode()
            picture = qr.get_qrcode_picture()
            qr_path = self.data_dir / "bilibili_login_qr.png"
            await asyncio.to_thread(qr_path.write_bytes, picture.content)
            self._qr_login = qr
            return qr_path

    async def wait_qr_login(
        self,
        timeout_seconds: int = 180,
        check_interval: float = 2.0,
    ) -> str:
        qr = self._qr_login
        if qr is None:
            raise RuntimeError("尚未生成登录二维码")
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            state = await qr.check_state()
            if state == login_v2.QrCodeLoginEvents.DONE:
                credential = qr.get_credential()
                await self._activate_credential(credential)
                await self._baseline_private_cursor()
                return "done"
            if state == login_v2.QrCodeLoginEvents.TIMEOUT:
                return "timeout"
            await asyncio.sleep(check_interval)
        return "timeout"

    async def validate(self) -> bool:
        if self.credential is None:
            return False
        return bool(await self.credential.check_valid())

    async def poll_video_comments(self) -> list[BiliIncoming]:
        credential = self._require_credential()
        await self._refresh_videos()
        incoming: list[BiliIncoming] = []
        for aid, _title in self._videos:
            data = await comment.get_comments(
                oid=aid,
                type_=comment.CommentResourceType.VIDEO,
                page_index=1,
                order=comment.OrderType.TIME,
                credential=credential,
            )
            replies = data.get("replies") or []
            for raw in replies:
                top = self._comment_from_raw(raw, aid, raw.get("rpid"))
                if top is not None:
                    incoming.append(top)
                for child in raw.get("replies") or []:
                    nested = self._comment_from_raw(
                        child,
                        aid,
                        raw.get("rpid"),
                    )
                    if nested is not None:
                        incoming.append(nested)
        return self._sorted_unique(incoming)

    async def poll_mentions(self) -> list[BiliIncoming]:
        credential = self._require_credential()
        data = await session.get_at(credential)
        incoming: list[BiliIncoming] = []
        for raw in data.get("items") or []:
            item = raw.get("item") or {}
            sender = raw.get("user") or raw.get("member") or {}
            sender_uid = self._first_int(
                sender.get("mid"),
                sender.get("uid"),
                raw.get("mid"),
            )
            if not sender_uid or sender_uid == self.self_uid:
                continue
            text = self._first_text(
                item.get("source_content"),
                item.get("message"),
                item.get("content"),
                raw.get("message"),
            )
            # 关键修复：B站 @ 通知流除了“@了我”的提醒，还会混入“@了我
            # 关注的人”的提醒（评论里同时 @ 了机器人关注/互动过的用户）。
            # 这类提醒的 at_details 不含机器人本体，若直接回复会造成误触发
            # （例如别人 @ 关注列表中的人时，机器人也跟着回复）。这里校验
            # 被 @ 的目标是否包含机器人，不包含则跳过。
            at_details = item.get("at_details") or []
            at_uids = {
                self._first_int(d.get("mid"), d.get("uid"))
                for d in at_details
                if isinstance(d, dict)
            }
            at_uids.discard(0)
            if self.self_uid not in at_uids:
                # 兜底：个别情况下 B站 未在 at_details 中返回被 @ 者，
                # 若文本中显式 @ 了本账号昵称，仍按“@了我”处理。
                mentioned_in_text = bool(self.self_name) and (
                    f"@{self.self_name}" in text
                )
                if not mentioned_in_text:
                    self.logger.debug(
                        "跳过@提醒（未@机器人本体，可能只是@了关注的人）："
                        "sender_uid=%s at_uids=%s text=%r",
                        sender_uid,
                        sorted(at_uids),
                        text[:60],
                    )
                    continue
            oid = self._first_int(
                item.get("subject_id"),
                item.get("business_id"),
                item.get("oid"),
                raw.get("subject_id"),
            )
            rpid = self._first_int(
                item.get("source_id"),
                item.get("target_id"),
                item.get("rpid"),
                raw.get("id"),
            )
            root = self._first_int(item.get("root_id"), rpid)
            if not text or not oid or not rpid:
                self.logger.debug("忽略无法定位评论资源的 @ 通知: %s", raw)
                continue
            resource_type = self._resource_type_from_notice(item)
            if resource_type is None:
                self.logger.debug(
                    "忽略不支持的 @ 通知业务类型 business_id=%s。",
                    item.get("business_id"),
                )
                continue
            incoming.append(
                BiliIncoming(
                    kind="mention",
                    event_key=f"comment:{resource_type.value}:{oid}:{rpid}",
                    sender_uid=sender_uid,
                    sender_name=self._first_text(
                        sender.get("nickname"),
                        sender.get("uname"),
                        sender.get("name"),
                    ),
                    text=text,
                    created_at=self._first_int(
                        raw.get("at_time"),
                        raw.get("reply_time"),
                        raw.get("ctime"),
                    ),
                    oid=oid,
                    root_rpid=root,
                    parent_rpid=rpid,
                    resource_type=resource_type,
                )
            )
        return self._sorted_unique(incoming)

    async def poll_private_messages(
        self,
    ) -> tuple[list[BiliIncoming], PrivateCursor]:
        credential = self._require_credential()
        data = await session.new_sessions(credential, begin_ts=self._dm_max_ts)
        max_ts = self._dm_max_ts
        max_seqno = dict(self._dm_max_seqno)
        incoming: list[BiliIncoming] = []

        for conversation in data.get("session_list") or []:
            session_type = self._first_int(conversation.get("session_type"), 1)
            if session_type != 1:
                continue
            talker_id = self._first_int(conversation.get("talker_id"))
            if not talker_id:
                continue
            messages = await session.fetch_session_msgs(
                talker_id=talker_id,
                credential=credential,
                session_type=session_type,
                begin_seqno=max_seqno.get(str(talker_id), 0),
            )
            for raw in messages.get("messages") or []:
                event = session.Event(raw, self.self_uid)
                if (
                    event.sender_uid == self.self_uid
                    or event.msg_type != session.EventType.TEXT.value
                ):
                    continue
                text = str(event.content or "").strip()
                if not text:
                    continue
                msg_id = str(
                    getattr(event, "msg_key", "")
                    or raw.get("msg_key")
                    or raw.get("seqno")
                )
                incoming.append(
                    BiliIncoming(
                        kind="private_message",
                        event_key=f"dm:{talker_id}:{msg_id}",
                        sender_uid=int(event.sender_uid),
                        sender_name=self._first_text(
                            raw.get("sender_name"),
                            raw.get("talker_name"),
                        ),
                        text=text,
                        created_at=self._first_int(
                            getattr(event, "timestamp", 0),
                            raw.get("timestamp"),
                        ),
                        talker_id=talker_id,
                    )
                )
            max_ts = max(
                max_ts,
                self._first_int(
                    conversation.get("session_ts"),
                    conversation.get("timestamp"),
                ),
            )
            max_seqno[str(talker_id)] = max(
                max_seqno.get(str(talker_id), 0),
                self._first_int(conversation.get("max_seqno")),
            )
        return self._sorted_unique(incoming), PrivateCursor(max_ts, max_seqno)

    async def commit_private_cursor(self, cursor: PrivateCursor) -> None:
        self._dm_max_ts = cursor.max_ts
        self._dm_max_seqno = cursor.max_seqno
        await asyncio.to_thread(self._save_poll_state)

    async def reply(self, incoming: BiliIncoming, text: str) -> dict[str, Any]:
        credential = self._require_credential()
        # 发送前再次清理，防止其他 B 站插件在此期间向共享 jar 写入旧鉴权 cookie。
        self._purge_session_auth_cookies(credential)
        if incoming.kind == "private_message":
            if incoming.talker_id is None:
                raise ValueError("私信事件缺少 talker_id")
            return await session.send_msg(
                credential=credential,
                receiver_id=incoming.talker_id,
                msg_type=session.EventType.TEXT,
                content=text,
            )
        if incoming.oid is None or incoming.parent_rpid is None:
            raise ValueError("评论事件缺少资源 ID 或评论 ID")
        root = incoming.root_rpid or incoming.parent_rpid
        parent = (
            incoming.parent_rpid
            if incoming.parent_rpid != root
            else None
        )
        return await comment.send_comment(
            text=text,
            oid=incoming.oid,
            type_=incoming.resource_type or comment.CommentResourceType.VIDEO,
            root=root,
            parent=parent,
            credential=credential,
        )

    async def _activate_credential(self, credential: Credential) -> None:
        if not credential.has_sessdata() or not credential.has_bili_jct():
            raise AuthenticationRequired("Cookie 缺少 SESSDATA 或 bili_jct")
        # 换 cookie 后，先清掉全局会话里残留的旧鉴权 cookie，避免 -111。
        self._purge_session_auth_cookies(credential, verbose=True)
        if not await credential.check_valid():
            raise AuthenticationRequired("B站 Cookie 已失效，请重新登录")
        info = await user.get_self_info(credential)
        self.credential = credential
        self.self_uid = int(info["mid"])
        self.self_name = str(info.get("uname") or info.get("name") or "")
        await asyncio.to_thread(self._save_credential)

    def _purge_session_auth_cookies(
        self,
        credential: Credential | None = None,
        *,
        verbose: bool = False,
    ) -> None:
        """清理进程内共享 curl_cffi 会话 jar 中与凭据冲突的旧 cookie。

        bilibili_api 17.4.2 的 curl_cffi 客户端按事件循环复用同一个
        AsyncSession，请求时把 cookies 参数合并进持久 cookie jar。换 cookie
        后，旧 SESSDATA/bili_jct 若以 .bilibili.com 等更宽域名残留在 jar 中，
        会与本次请求传入的新值一并发送，B 站解析到旧 bili_jct 即返回 -111。
        每次鉴权/写操作前把凭据涉及的 cookie 名从 jar 中删除，请求级新值会
        自动补齐，不影响其他插件对 bilibili_api 的 impersonate 等全局设置。
        """
        target = credential or self.credential
        if target is None:
            return
        try:
            from bilibili_api.utils.network import get_session

            wrapped = get_session()
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("获取全局会话失败，跳过 cookie 清理：%s", exc)
            return
        jar = getattr(wrapped, "cookies", None)
        if jar is None:
            return
        # 诊断：激活时打印 jar 中残留的鉴权 cookie（含域名，用于确认根因）。
        if verbose:
            found = []
            try:
                for cookie in jar.jar:
                    if cookie.name in (
                        "SESSDATA",
                        "bili_jct",
                        "DedeUserID",
                        "buvid3",
                        "buvid4",
                    ):
                        value = str(cookie.value or "")[:8]
                        found.append(f"{cookie.name}={value}…@{cookie.domain}")
            except Exception:  # noqa: BLE001
                pass
            if found:
                self.logger.info(
                    "清理前全局会话中的鉴权 cookie：%s", "; ".join(found)
                )
            else:
                self.logger.info("清理前全局会话中无残留鉴权 cookie。")
        names = set(target.get_cookies().keys())
        for name in names:
            try:
                jar.delete(name)
            except Exception:  # noqa: BLE001
                continue
        self.logger.debug("已清理全局会话中的鉴权 cookie 键：%s", ", ".join(sorted(names)))

    async def _refresh_videos(self) -> None:
        now = time.monotonic()
        if self._videos and now - self._videos_loaded_at < self.video_refresh_seconds:
            return
        credential = self._require_credential()
        data = await user.User(self.self_uid, credential=credential).get_videos(
            pn=1,
            ps=self.max_videos,
            order=user.VideoOrder.PUBDATE,
        )
        video_list = ((data.get("list") or {}).get("vlist") or [])
        self._videos = [
            (int(item["aid"]), str(item.get("title", "")))
            for item in video_list
            if item.get("aid")
        ]
        self._videos_loaded_at = now

    async def _baseline_private_cursor(self) -> None:
        credential = self._require_credential()
        data = await session.get_sessions(credential, session_type=4)
        self._dm_max_ts = int(time.time() * 1_000_000)
        self._dm_max_seqno = {
            str(item["talker_id"]): int(item.get("max_seqno", 0))
            for item in data.get("session_list") or []
            if item.get("talker_id")
        }
        await asyncio.to_thread(self._save_poll_state)

    def _comment_from_raw(
        self,
        raw: dict[str, Any],
        aid: int,
        root_rpid: Any,
    ) -> BiliIncoming | None:
        member = raw.get("member") or {}
        sender_uid = self._first_int(raw.get("mid"), member.get("mid"))
        rpid = self._first_int(raw.get("rpid"), raw.get("rpid_str"))
        text = self._first_text((raw.get("content") or {}).get("message"))
        if (
            not sender_uid
            or sender_uid == self.self_uid
            or not rpid
            or not text
        ):
            return None
        root = self._first_int(raw.get("root"), root_rpid, rpid)
        return BiliIncoming(
            kind="video_comment",
            event_key=f"comment:{comment.CommentResourceType.VIDEO.value}:{aid}:{rpid}",
            sender_uid=sender_uid,
            sender_name=self._first_text(
                member.get("uname"),
                member.get("name"),
            ),
            text=text,
            created_at=self._first_int(raw.get("ctime")),
            oid=aid,
            root_rpid=root,
            parent_rpid=rpid,
            resource_type=comment.CommentResourceType.VIDEO,
        )

    @staticmethod
    def _resource_type_from_notice(item: dict[str, Any]) -> Any | None:
        raw_type = BiliClient._first_int(item.get("business_id"))
        for resource_type in comment.CommentResourceType:
            if resource_type.value == raw_type:
                return resource_type
        return None

    def _require_credential(self) -> Credential:
        if self.credential is None:
            raise AuthenticationRequired("尚未登录 B站")
        return self.credential

    def _credential_from_cookie(self, cookie_text: str) -> Credential:
        cookies: dict[str, str] = {}
        for part in cookie_text.split(";"):
            key, separator, value = part.strip().partition("=")
            if separator and key:
                cookies[key] = value
        known = {
            "sessdata": cookies.pop("SESSDATA", cookies.pop("sessdata", "")),
            "bili_jct": cookies.pop("bili_jct", ""),
            "buvid3": cookies.pop("buvid3", cookies.pop("BUVID3", "")),
            "buvid4": cookies.pop("buvid4", cookies.pop("BUVID4", "")),
            "dedeuserid": cookies.pop(
                "DedeUserID", cookies.pop("DEDEUSERID", "")
            ),
            "ac_time_value": cookies.pop("ac_time_value", ""),
        }
        return Credential(**known, **cookies)

    def _load_credential(self) -> Credential | None:
        try:
            data = json.loads(self.credential_path.read_text(encoding="utf-8"))
            return Credential(**data)
        except (OSError, ValueError, TypeError):
            self.logger.exception("读取持久化 B站 Credential 失败。")
            return None

    def _save_credential(self) -> None:
        if self.credential is None:
            return
        self.credential_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "sessdata": self.credential.sessdata or "",
            "bili_jct": self.credential.bili_jct or "",
            "buvid3": self.credential.buvid3 or "",
            "buvid4": self.credential.buvid4 or "",
            "dedeuserid": self.credential.dedeuserid or "",
            "ac_time_value": self.credential.ac_time_value or "",
        }
        temp_path = self.credential_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self.credential_path)
        try:
            self.credential_path.chmod(0o600)
        except OSError:
            pass

    def _load_poll_state(self) -> None:
        if not self.poll_state_path.exists():
            return
        try:
            data = json.loads(self.poll_state_path.read_text(encoding="utf-8"))
            self._dm_max_ts = int(data.get("dm_max_ts", self._dm_max_ts))
            self._dm_max_seqno = {
                str(key): int(value)
                for key, value in data.get("dm_max_seqno", {}).items()
            }
        except (OSError, ValueError, TypeError):
            self.logger.exception("读取 B站轮询游标失败，将从当前状态开始。")

    def _save_poll_state(self) -> None:
        data = {
            "dm_max_ts": self._dm_max_ts,
            "dm_max_seqno": self._dm_max_seqno,
        }
        temp_path = self.poll_state_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self.poll_state_path)

    @staticmethod
    def _sorted_unique(items: list[BiliIncoming]) -> list[BiliIncoming]:
        unique = {item.event_key: item for item in items}
        return sorted(unique.values(), key=lambda item: item.created_at)

    @staticmethod
    def _first_int(*values: Any) -> int:
        for value in values:
            try:
                if value is not None and str(value) != "":
                    return int(value)
            except (TypeError, ValueError):
                continue
        return 0

    @staticmethod
    def _first_text(*values: Any) -> str:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""
