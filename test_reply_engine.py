from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from reply_engine import ReplyEngine, SlidingWindowRateLimiter


class _Logger:
    def warning(self, *_args, **_kwargs):
        pass

    def exception(self, *_args, **_kwargs):
        pass


class _Context:
    pass


class ReplyEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_mark_reload_and_dedup_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "max_reply_chars": 8,
                "max_replies_per_minute": 10,
                "dedup_retention_days": 30,
            }
            engine = ReplyEngine(
                _Context(),
                Path(temp_dir),
                config,
                _Logger(),
            )
            await engine.initialize()
            reply = "这是一条用于测试的回复"
            await engine.mark_processed("comment:1:2:3", reply)
            # 相同文本应被识别为重复回复
            self.assertTrue(await engine.should_skip_duplicate_reply(reply))
            # 不同文本不重复
            self.assertFalse(
                await engine.should_skip_duplicate_reply("完全不同的回复")
            )

            reloaded = ReplyEngine(
                _Context(),
                Path(temp_dir),
                config,
                _Logger(),
            )
            await reloaded.initialize()
            self.assertTrue(reloaded.is_processed("comment:1:2:3"))
            self.assertTrue(await reloaded.should_skip_duplicate_reply(reply))

    async def test_rate_limiter_waits_for_sliding_window(self):
        limiter = SlidingWindowRateLimiter(limit=2, window_seconds=0.05)
        await limiter.acquire()
        await limiter.acquire()
        started = time.monotonic()
        await limiter.acquire()
        self.assertGreaterEqual(time.monotonic() - started, 0.04)

    async def test_rate_limit_is_hard_capped_at_ten(self):
        limiter = SlidingWindowRateLimiter(limit=999, window_seconds=0.01)
        self.assertEqual(limiter.limit, 10)


if __name__ == "__main__":
    unittest.main()
