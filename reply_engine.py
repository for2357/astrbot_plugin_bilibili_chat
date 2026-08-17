"""B站回复的去重、风控限流与回复发送辅助。

自 v0.2 起，回复内容改由 AstrBot 完整管道生成（人设、技能、知识库、其他
插件、回复前缀等）。本模块不再直接调用 LLM，只负责：

- 事件去重：已处理过的消息不再回复
- 重复回复检测：避免对相似回复复读
- B站回复限流：滑动窗口限流，防止触发平台风控
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any


class SlidingWindowRateLimiter:
    """Async sliding-window limiter with a hard maximum of 10/minute."""

    def __init__(self, limit: int, window_seconds: float = 60.0) -> None:
        self.limit = min(10, max(1, int(limit)))
        self.window_seconds = float(window_seconds)
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while (
                    self._timestamps
                    and now - self._timestamps[0] >= self.window_seconds
                ):
                    self._timestamps.popleft()
                if len(self._timestamps) < self.limit:
                    self._timestamps.append(now)
                    return
                delay = self.window_seconds - (now - self._timestamps[0])
            await asyncio.sleep(max(0.05, delay))


class ReplyEngine:
    """回复去重、重复回复检测与 B站回复限流。"""

    def __init__(
        self,
        context: Any,
        data_dir: Path,
        config: dict[str, Any],
        logger: Any,
    ) -> None:
        self.context = context
        self.logger = logger
        self.max_reply_chars = min(
            1000, max(20, int(config.get("max_reply_chars", 300)))
        )
        self.retention_seconds = max(
            1, int(config.get("dedup_retention_days", 30))
        ) * 86400
        self.blacklist = {
            str(value).strip()
            for value in config.get("blacklist_uids", [])
            if str(value).strip()
        }
        self.rate_limiter = SlidingWindowRateLimiter(
            int(config.get("max_replies_per_minute", 10))
        )
        self.state_path = data_dir / "reply_state.json"
        self._processed: dict[str, float] = {}
        self._reply_hashes: dict[str, float] = {}
        self._baseline_ready = False
        self._state_lock = asyncio.Lock()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._load_state)
        await self._cleanup()

    @property
    def baseline_ready(self) -> bool:
        return self._baseline_ready

    def is_blacklisted(self, uid: int | str) -> bool:
        return str(uid) in self.blacklist

    def is_processed(self, event_key: str) -> bool:
        return event_key in self._processed

    async def establish_baseline(self, event_keys: list[str]) -> None:
        now = time.time()
        async with self._state_lock:
            for key in event_keys:
                self._processed[key] = now
            self._baseline_ready = True
            await asyncio.to_thread(self._save_state)

    async def mark_processed(self, event_key: str, reply: str = "") -> None:
        """标记事件为已处理。幂等，可重复调用。

        reply 非空时同时记录回复哈希，用于后续重复回复检测。
        """
        now = time.time()
        async with self._state_lock:
            self._processed[event_key] = now
            if reply:
                self._reply_hashes[self._hash(reply)] = now
            self._baseline_ready = True
            await asyncio.to_thread(self._save_state)

    async def should_skip_duplicate_reply(self, text: str) -> bool:
        """回复内容是否与近期已发送的回复重复。"""
        async with self._state_lock:
            return self._hash(text) in self._reply_hashes

    async def send_limited(
        self,
        send: Callable[[], Awaitable[Any]],
    ) -> Any:
        await self.rate_limiter.acquire()
        return await send()

    async def _cleanup(self) -> None:
        cutoff = time.time() - self.retention_seconds
        async with self._state_lock:
            self._processed = {
                key: ts for key, ts in self._processed.items() if ts >= cutoff
            }
            self._reply_hashes = {
                key: ts for key, ts in self._reply_hashes.items() if ts >= cutoff
            }
            await asyncio.to_thread(self._save_state)

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self._processed = {
                str(key): float(value)
                for key, value in data.get("processed", {}).items()
            }
            self._reply_hashes = {
                str(key): float(value)
                for key, value in data.get("reply_hashes", {}).items()
            }
            self._baseline_ready = bool(data.get("baseline_ready", False))
        except (OSError, ValueError, TypeError):
            self.logger.exception("读取回复去重状态失败，将使用空状态。")

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_path.with_suffix(".tmp")
        data = {
            "baseline_ready": self._baseline_ready,
            "processed": self._processed,
            "reply_hashes": self._reply_hashes,
        }
        temp_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self.state_path)

    @staticmethod
    def _hash(text: str) -> str:
        normalized = " ".join(text.split()).casefold()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
