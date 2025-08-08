# core/raid.py
import asyncio
import json
import math
import time
from typing import Optional, Set

import websockets

from core.viewer import ViewerBot

HERMES_WSS = "wss://hermes.twitch.tv/v1?clientId=kimne78kx3ncx6brgo4mv6wki5h1ko"


class RaidManager:
    """
    Поведение повторяет Go internal/twitch/task/raid.go:
    - подписка на raid.<channel_id> через Hermes
    - обработка raid_update_v2 / raid_go_v2
    - перенос части зрителей join_raid()
    - понижение онлайна по минутам (PercentDroppingInMinute)
    - при DroppingRaid — возврат к Start() через 2 минуты
    """

    def __init__(self, task_runner, delay: int = 5):
        self.t = task_runner
        self.delay = max(0, int(delay))
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._seen_raid_ids: Set[str] = set()
        self._raid_target_login: str = ""

    async def start(self):
        # если рейды выключены конфигом — выходим
        raid_cfg = getattr(self.t, "raid", None)
        if not raid_cfg or not getattr(raid_cfg, "enable", False):
            return

        self._running = True

        while self._running:
            # состояния при которых мы не держим подписку
            if self.t.status in ("shutdown", "raid", "waiting"):
                await self._cancel_subscribe()
                return

            # если пауза/оффлайн — снимаем подписку и ждём возврата в running
            if self.t.status in ("paused", "offline"):
                await self._cancel_subscribe()
                while self._running:
                    await asyncio.sleep(3)
                    if self.t.status in ("shutdown",):
                        return
                    if self.t.status in ("running", "waiting"):
                        break

            err = await self._subscribe_and_listen()
            if err:
                # подождём и попробуем снова
                await asyncio.sleep(2)

    async def _subscribe_and_listen(self) -> Optional[Exception]:
        try:
            async with websockets.connect(HERMES_WSS, ping_interval=60) as ws:
                self._ws = ws
                # Формируем сообщение подписки (как в Go subscribeMessage)
                msg = {
                    "type": "subscribe",
                    "id": "raid-sub",
                    "subscribe": {
                        "id": "raid-sub-1",
                        "type": "pubsub",
                        "pubsub": {
                            "topic": f"raid.{self.t.channel_id}",
                        },
                    },
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                await ws.send(json.dumps(msg))

                while self._running and self.t.status == "running":
                    raw = await ws.recv()
                    data = json.loads(raw)

                    # Ищем уведомления типа "notification" -> pubsub -> raid_update_v2/raid_go_v2
                    if data.get("type") != "notification":
                        continue
                    notify = data.get("notification") or {}
                    if notify.get("type") != "pubsub":
                        continue

                    pubsub_raw = notify.get("pubsub")
                    if not pubsub_raw:
                        continue

                    try:
                        pub = json.loads(pubsub_raw)
                    except Exception:
                        continue

                    if pub.get("type") not in ("raid_update_v2", "raid_go_v2"):
                        continue

                    raid = (pub.get("raid") or {})
                    raid_id = raid.get("id")
                    target_login = raid.get("target_login")

                    # дедупликация
                    if not raid_id or raid_id in self._seen_raid_ids:
                        continue
                    self._seen_raid_ids.add(raid_id)

                    # Только на update — инициируем перенос
                    if pub.get("type") == "raid_update_v2":
                        self.t.status = "raid"
                        self._raid_target_login = target_login or ""
                        await self._start_raid(raid_id)
                        self._start_dropping_viewers()
                        self._maybe_schedule_return()
        except Exception as e:
            return e
        finally:
            await self._cancel_subscribe()
        return None

    async def _cancel_subscribe(self):
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            finally:
                self._ws = None

    async def _start_raid(self, raid_id: str):
        """
        Переносим часть зрителей в рейд:
        - берём Percent от текущих
        - тем, кого переносим — делаем join_raid(raid_id) и меняем канал на target
        - остальных — стопаем
        """
        raid_cfg = getattr(self.t, "raid", None)
        percent = int(getattr(raid_cfg, "percent", 0)) if raid_cfg else 0

        current = list(self.t.viewers)  # снимок
        move_limit = int(math.floor(len(current) * (percent / 100.0))) if percent > 0 else len(current)

        async def _migrate(viewer: ViewerBot):
            retries = 0
            while retries <= 10 and self._running:
                try:
                    # ожидаем, что в ViewerBot есть join_raid(raid_id)
                    if hasattr(viewer, "join_raid"):
                        await viewer.join_raid(raid_id)
                    # переключаем канал после удачного join
                    viewer.change_channel(self._raid_target_login or self.t.channel)
                    return
                except Exception:
                    retries += 1
                    await asyncio.sleep(0.5)

        tasks = []
        # переносим часть
        for i, v in enumerate(current):
            if i < move_limit:
                tasks.append(asyncio.create_task(_migrate(v)))
            else:
                v.stop()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _start_dropping_viewers(self):
        """
        Каждую минуту уменьшаем число зрителей на PercentDroppingInMinute процентов,
        только пока статус 'raid' или 'offline' (как в Go).
        """
        raid_cfg = getattr(self.t, "raid", None)
        drop_percent = int(getattr(raid_cfg, "percent_dropping_in_minute", 0)) if raid_cfg else 0
        if drop_percent <= 0:
            return

        async def _runner():
            while self._running:
                if self.t.status not in ("raid", "offline"):
                    return
                await asyncio.sleep(60)
                number = int(math.floor(len(self.t.viewers) * (drop_percent / 100.0)))
                if number <= 0:
                    continue
                # просто стопаем первых number ботов
                for _ in range(min(number, len(self.t.viewers))):
                    bot = self.t.viewers.pop()
                    bot.stop()

        asyncio.create_task(_runner())

    def _maybe_schedule_return(self):
        """
        Если в конфиге DroppingRaid==True, через 2 минуты вернуть задачу к Start().
        """
        raid_cfg = getattr(self.t, "raid", None)
        dropping_raid = bool(getattr(raid_cfg, "dropping_raid", False)) if raid_cfg else False
        if not dropping_raid or self.t.status == "shutdown":
            return

        async def _back():
            await asyncio.sleep(120)
            # вернём задачу в normal flow
            await self.t.start()

        asyncio.create_task(_back())

    def stop(self):
        self._running = False
        asyncio.create_task(self._cancel_subscribe())
