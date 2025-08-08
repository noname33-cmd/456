# core/live_status.py
import asyncio
import aiohttp
from typing import Tuple

PERSISTED_HASH = "639d5f11bfb8bf3053b424d9ef650d04c4ebb7d94711d644afb08fe9a0fad5d9"
CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"


class LiveStatusChecker:
    """
    Логика максимально близка к Go:
    - если оффлайн коротко — просто держим паузу (у нас статус 'offline')
    - если оффлайн долго (≈10 минут) — останавливаем всех и ставим 'waiting'
    - если вернулся онлайн — либо resume(), либо start() (если долго ждали/ждём)
    - если задача вручную 'paused' — мониторинг не снимает паузу
    """
    def __init__(self, task_runner, check_interval: int = 5):
        self.task_runner = task_runner
        self.check_interval = max(1, int(check_interval))
        self.running = False
        self._session: aiohttp.ClientSession | None = None

    async def _get_stream_status(self, channel: str) -> Tuple[str, bool]:
        payload = {
            "extensions": {"persistedQuery": {"sha256Hash": PERSISTED_HASH, "version": 1}},
            "operationName": "UseLive",
            "variables": {"channelLogin": channel},
        }
        headers = {"Client-ID": CLIENT_ID}
        async with self._session.post("https://gql.twitch.tv/gql", json=payload, headers=headers) as r:
            if r.status != 200:
                return "", False
            data = await r.json()
            user = data.get("data", {}).get("user", {})
            return user.get("id", ""), bool(user.get("stream"))

    async def start(self):
        self.running = True
        offline_ticks = 0  # до 120 (≈10 минут при 5s)
        try:
            async with aiohttp.ClientSession() as session:
                self._session = session
                while self.running:
                    try:
                        channel_id, is_live = await self._get_stream_status(self.task_runner.channel)
                        self.task_runner.channel_id = channel_id

                        # Прекращаем мониторинг если задача завершена
                        if self.task_runner.status in ("shutdown", "stopped"):
                            return

                        # Не вмешиваемся, если пользователь сам поставил паузу
                        if self.task_runner.status == "paused":
                            await asyncio.sleep(self.check_interval)
                            continue

                        if is_live:
                            # Если вернулся онлайн
                            if self.task_runner.status in ("offline", "waiting"):
                                if offline_ticks <= 119:
                                    # короткий оффлайн — просто резюмим
                                    offline_ticks = 0
                                    self.task_runner.resume()
                                else:
                                    # долгий оффлайн — перезапускаем (создаст зрителей заново)
                                    offline_ticks = 0
                                    await self.task_runner.start()
                            else:
                                # уже running — ничего не делаем
                                offline_ticks = 0
                        else:
                            # Канал оффлайн
                            if self.task_runner.status == "running":
                                # как в Go: ставим offline (без полного стопа)
                                self.task_runner.pause()
                                self.task_runner.status = "offline"
                                offline_ticks = 0
                            elif self.task_runner.status == "offline" and offline_ticks <= 119:
                                offline_ticks += 1
                            elif self.task_runner.status in ("offline", "waiting") and offline_ticks > 119:
                                # долгий оффлайн — полностью выключаем и ждём
                                self.task_runner.stop()
                                self.task_runner.status = "waiting"
                                offline_ticks = 0

                    except Exception:
                        # глушим единичные ошибки сети/парсинга
                        pass

                    await asyncio.sleep(self.check_interval)
        finally:
            self._session = None

    def stop(self):
        self.running = False
