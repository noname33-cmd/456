# core/task.py
import asyncio
from typing import List, Optional
from models import Task
from core.viewer import ViewerBot
from core.floating import FloatingOnline
from core.raid import RaidManager
from core.live_status import LiveStatusChecker
from core.proxy import RoundRobin


class TaskRunner:
    def __init__(
            self,
            id: str,
            channel: str,
            viewers: int,
            proxies: List[str],
            tokens: List[str],
            floating_config: dict = None,
            live_check: bool = False,
            time_in_minutes: Optional[int] = None,   # если есть — как в Go, используем для задержки между стартами
            start_gap: Optional[float] = None,       # fallback (секунды) если time_in_minutes не задан
            **kwargs
    ):
        self.id = id
        self.channel = channel
        self.channel_id: str = ""
        self.viewers_count = max(0, int(viewers))
        self.viewers: List[ViewerBot] = []

        # round-robin по прокси/токенам (как Helper.Next() в Go)
        self._proxies_rr = RoundRobin(proxies or [])
        self._tokens_rr = RoundRobin(tokens or [])

        self.status = "running"
        self._pause_event = asyncio.Event()
        self._pause_event.set()

        self.floating_config = floating_config
        self._floating_task: Optional[asyncio.Task] = None

        self.live_checker = LiveStatusChecker(self) if live_check else None
        self._live_task: Optional[asyncio.Task] = None

        # задержка между стартами зрителей: как в Go delayBetweenStart = (timeInMinutes*60000)/viewers
        if time_in_minutes and self.viewers_count > 0:
            self._start_gap = max(0.01, (time_in_minutes * 60) / self.viewers_count)
        else:
            self._start_gap = float(start_gap) if start_gap is not None else 0.15  # дефолтная мелкая пауза

    async def start(self):
        # уже запускались — не дублируем
        if not self.viewers and self.viewers_count > 0:
            for _ in range(self.viewers_count):
                proxy = self._proxies_rr.next()
                token = self._tokens_rr.next()
                bot = ViewerBot(self.channel, proxy, token)
                self.viewers.append(bot)
                asyncio.create_task(bot.run(self._pause_event))
                # “staggered start”, чтобы не бомбить одним пачком
                await asyncio.sleep(self._start_gap)

        # флоатинг
        if self.floating_config and not self._floating_task:
            floating = FloatingOnline(self, **self.floating_config)
            self._floating_task = asyncio.create_task(floating.start())

        # live-checker
        if self.live_checker and not self._live_task:
            self._live_task = asyncio.create_task(self.live_checker.start())

        self.status = "running"
        print(f"[TASK {self.id}] Started {len(self.viewers)} viewers on #{self.channel}")

    def pause(self):
        if self.status == "running":
            self._pause_event.clear()
            self.status = "paused"
            print(f"[TASK {self.id}] Paused")

    def resume(self):
        if self.status in ("paused", "offline", "waiting"):
            self._pause_event.set()
            self.status = "running"
            print(f"[TASK {self.id}] Resumed")

    def stop(self):
        # останавливаем всех и чистим список
        for v in self.viewers:
            v.stop()
        self.viewers.clear()
        self.status = "stopped"
        print(f"[TASK {self.id}] Stopped")

        # выключаем флоатинг
        if self._floating_task:
            self._floating_task.cancel()
            self._floating_task = None

        # выключаем live-checker
        if self.live_checker:
            self.live_checker.stop()
        self._live_task = None

    async def raid(self, target_channel: str, delay: int = 5):
        raid_mgr = RaidManager(self, target_channel, delay)
        asyncio.create_task(raid_mgr.start())

    def to_task_model(self) -> Task:
        return Task(
            id=self.id,
            channel=self.channel,
            status=self.status,
            viewers=len(self.viewers),
        )
