# core/floating.py
import asyncio
import math
import random
from core.viewer import ViewerBot

class FloatingOnline:
    def __init__(self, task_runner, percent_min_viewers: int, percent_max_viewers: int, percent: int, delay: int):
        self.t = task_runner
        self.percent_min = int(percent_min_viewers)
        self.percent_max = int(percent_max_viewers)
        self.percent_step = int(percent)
        self.delay = max(1, int(delay))
        self._count_down = 0
        self._count_up = 0
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            if self.t.status in ("stopped", "shutdown", "raid", "waiting"):
                return
            if self.t.status in ("paused", "offline"):
                while self._running:
                    await asyncio.sleep(3)
                    if self.t.status == "running": break
                    if self.t.status in ("stopped", "shutdown", "waiting"):
                        return

            base = self.t.viewers_count
            maxv = base + int(math.floor(base * (self.percent_max / 100.0)))
            minv = max(0, base - int(math.floor(base * (self.percent_min / 100.0))))
            await self._change(minv, maxv)
            await asyncio.sleep(self.delay)

    async def _change(self, min_viewers: int, max_viewers: int):
        current = len(self.t.viewers)
        step = int(math.floor(current * (self.percent_step / 100.0)))
        if step <= 0:
            step = 1 if current > 0 else 0

        if self._count_down == 2:
            self._count_up += 1; self._count_down = 0
            await self._add(step); return

        if self._count_up == 2:
            self._count_down += 1; self._count_up = 0
            self._remove(step); return

        if (current + step) <= max_viewers and (current - step) >= min_viewers:
            if random.randint(0, 1) == 0:
                self._count_up += 1; self._count_down = 0
                await self._add(step)
            else:
                self._count_down += 1; self._count_up = 0
                self._remove(step)
            return

        if (current + step) > max_viewers:
            self._count_down += 1; self._count_up = 0
            self._remove(step); return

        if (current - step) < min_viewers:
            self._count_up += 1; self._count_down = 0
            await self._add(step); return

    async def _add(self, count: int):
        if count <= 0: return
        for _ in range(count):
            if self.t.status in ("stopped", "shutdown", "waiting"):
                return
            proxy = self.t._proxies_rr.next() if hasattr(self.t, "_proxies_rr") else None
            token = self.t._tokens_rr.next() if hasattr(self.t, "_tokens_rr") else None
            bot = ViewerBot(self.t.channel, proxy, token)
            self.t.viewers.append(bot)
            asyncio.create_task(bot.run(self.t._pause_event))

    def _remove(self, count: int):
        if count <= 0 or not self.t.viewers: return
        n = min(count, len(self.t.viewers))
        for _ in range(n):
            v = self.t.viewers.pop(); v.stop()

    def stop(self):
        self._running = False
