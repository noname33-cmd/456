# core/floating.py
import asyncio
import math
import random
from core.viewer import ViewerBot


class FloatingOnline:
    """
    Полный аналог Go floating_online.go:
    - max/min считаются от базового viewers_count
    - шаг изменения = floor(len(viewers) * percent/100)
    - два подряд повышения -> принудительно понижаем и наоборот
    - соблюдаем границы min/max
    """
    def __init__(
            self,
            task_runner,
            percent_min_viewers: int,
            percent_max_viewers: int,
            percent: int,
            delay: int
    ):
        self.t = task_runner
        self.percent_min = int(percent_min_viewers)
        self.percent_max = int(percent_max_viewers)
        self.percent_step = int(percent)
        self.delay = max(1, int(delay))

        # внутренние счётчики как в Go
        self._count_down = 0
        self._count_up = 0

        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            # условия выхода как в Go
            if self.t.status in ("stopped", "shutdown", "raid", "waiting"):
                return

            # если пауза/оффлайн — ждём пока выйдем в running, либо остановимся окончательно
            if self.t.status in ("paused", "offline"):
                while self._running:
                    await asyncio.sleep(3)
                    if self.t.status == "running":
                        break
                    if self.t.status in ("stopped", "shutdown", "waiting"):
                        return

            # вычисляем границы от базового числа зрителей
            base = self.t.viewers_count
            max_viewers = base + int(math.floor(base * (self.percent_max / 100.0)))
            min_viewers = max(0, base - int(math.floor(base * (self.percent_min / 100.0))))

            # меняем онлайн
            await self._change_floating_online(min_viewers, max_viewers)

            # ждём задержку
            await asyncio.sleep(self.delay)

    async def _change_floating_online(self, min_viewers: int, max_viewers: int):
        current = len(self.t.viewers)
        # шаг изменения: % от текущего онлайна (как в Go)
        step = int(math.floor(current * (self.percent_step / 100.0)))
        # хотя бы по одному двигаемся, если есть зрители
        if step <= 0:
            step = 1 if current > 0 else 0

        # Ветка логики как в Go
        if self._count_down == 2:
            # два раза снижали — теперь увеличиваем
            self._count_up += 1
            self._count_down = 0
            await self._add_viewers(step)
            # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (forced up)")
            return

        if self._count_up == 2:
            # два раза повышали — теперь уменьшаем
            self._count_down += 1
            self._count_up = 0
            self._remove_viewers(step)
            # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (forced down)")
            return

        # если укладываемся в границы — случайно вверх/вниз
        if (current + step) <= max_viewers and (current - step) >= min_viewers:
            flip = (random.randint(0, 1) == 0)
            if flip:
                self._count_up += 1
                self._count_down = 0
                await self._add_viewers(step)
                # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (up)")
            else:
                self._count_down += 1
                self._count_up = 0
                self._remove_viewers(step)
                # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (down)")
            return

        # если выше max — снижаем
        if (current + step) > max_viewers:
            self._count_down += 1
            self._count_up = 0
            self._remove_viewers(step)
            # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (>max, down)")
            return

        # если ниже min — повышаем
        if (current - step) < min_viewers:
            self._count_up += 1
            self._count_down = 0
            await self._add_viewers(step)
            # print(f"[FLOAT] {current} -> {len(self.t.viewers)} (<min, up)")
            return

    async def _add_viewers(self, count: int):
        if count <= 0:
            return
        for _ in range(count):
            if self.t.status in ("stopped", "shutdown", "waiting"):
                return
            # round-robin как в Go (Helper.Next())
            proxy = self.t._proxies_rr.next() if hasattr(self.t, "_proxies_rr") else None
            token = self.t._tokens_rr.next() if hasattr(self.t, "_tokens_rr") else None
            bot = ViewerBot(self.t.channel, proxy, token)
            self.t.viewers.append(bot)
            asyncio.create_task(bot.run(self.t._pause_event))

    def _remove_viewers(self, count: int):
        if count <= 0 or not self.t.viewers:
            return
        n = min(count, len(self.t.viewers))
        # как в Go — просто берём первых n (у нас безопасно с конца)
        for _ in range(n):
            v = self.t.viewers.pop()
            v.stop()

    def stop(self):
        self._running = False
