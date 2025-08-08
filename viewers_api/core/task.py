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
    def __init__(self, id: str, channel: str, viewers: int, proxies: List[str], tokens: List[str],
                 floating_config: dict = None, live_check: bool = False,
                 time_in_minutes: Optional[int] = None, start_gap: Optional[float] = None, **kwargs):
        self.id = id
        self.channel = channel
        self.channel_id: str = ""
        self.viewers_count = max(0, int(viewers))
        self.viewers: List[ViewerBot] = []

        self._proxies_rr = RoundRobin(proxies or [])
        self._tokens_rr = RoundRobin(tokens or [])

        self.status = "running"
        self._pause_event = asyncio.Event()
        self._pause_event.set()

        self.floating_config = floating_config
        self._floating_task: Optional[asyncio.Task] = None

        self.live_checker = LiveStatusChecker(self) if live_check else None
        self._live_task: Optional[asyncio.Task] = None

        # NEW: держим ссылки на все созданные таски ботов/рейда
        self._bot_tasks: list[asyncio.Task] = []
        self._raid_task: Optional[asyncio.Task] = None
        self._shutdown = False  # жёсткая метка для любых фоновых компонентов

        if time_in_minutes and self.viewers_count > 0:
            self._start_gap = max(0.01, (time_in_minutes * 60) / self.viewers_count)
        else:
            self._start_gap = float(start_gap) if start_gap is not None else 0.15

    async def start(self):
        if self._shutdown:
            return  # не перезапускаем после стопа

        # стартуем ботов только если их ещё нет
        if not self.viewers and self.viewers_count > 0:
            for _ in range(self.viewers_count):
                # guard на пустые rr
                proxy = self._proxies_rr.next() if self._proxies_rr else None
                token = self._tokens_rr.next() if self._tokens_rr else None

                bot = ViewerBot(self.channel, proxy, token)
                self.viewers.append(bot)
                t = asyncio.create_task(bot.run(self._pause_event))
                self._bot_tasks.append(t)
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
        # Жёстко глушим всё
        self._shutdown = True
        self._pause_event.set()  # разбудить ботов, чтобы они вышли из wait()

        # остановить ботов и очистить список
        for v in self.viewers:
            v.stop()
        self.viewers.clear()

        # отменить таски ботов
        for t in self._bot_tasks:
            t.cancel()
        self._bot_tasks.clear()

        # выключаем флоатинг
        if self._floating_task:
            self._floating_task.cancel()
            self._floating_task = None

        # выключаем live-checker
        if self.live_checker:
            self.live_checker.stop()
        if self._live_task:
            self._live_task.cancel()
            self._live_task = None

        # отменяем рейд (если шёл)
        if self._raid_task:
            self._raid_task.cancel()
            self._raid_task = None

        self.status = "stopped"
        print(f"[TASK {self.id}] Stopped")

    async def raid(self, target_channel: str, delay: int = 5):
        # сохраняем ссылку, чтобы можно было отменить при stop()
        mgr = RaidManager(self, delay=delay)
        self._raid_task = asyncio.create_task(mgr.start())

    def to_task_model(self) -> Task:
        return Task(id=self.id, channel=self.channel, status=self.status, viewers=len(self.viewers))
