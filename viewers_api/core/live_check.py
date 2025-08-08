# core/live_check.py
import asyncio
import aiohttp

class LiveStatusChecker:
    def __init__(self, task_runner, client_ids: list, oauth_tokens: list, interval: int = 60):
        """
        task_runner : TaskRunner
            Ссылка на TaskRunner.
        client_ids : list
            Список Twitch Client-ID для ротации.
        oauth_tokens : list
            Список OAuth токенов для API.
        interval : int
            Период проверки в секундах.
        """
        self.task_runner = task_runner
        self.client_ids = client_ids
        self.oauth_tokens = oauth_tokens
        self.interval = interval
        self.running = False
        self._client_idx = 0
        self._token_idx = 0
        self.last_status = None

    def _next_client_id(self):
        self._client_idx = (self._client_idx + 1) % len(self.client_ids)
        return self.client_ids[self._client_idx]

    def _next_oauth_token(self):
        self._token_idx = (self._token_idx + 1) % len(self.oauth_tokens)
        return self.oauth_tokens[self._token_idx]

    async def start(self):
        self.running = True
        while self.running:
            try:
                is_live = await self._check_live()
                if is_live != self.last_status:
                    self.last_status = is_live
                    if is_live:
                        print(f"[LIVE] {self.task_runner.channel} is live. Starting viewers...")
                        if self.task_runner.status != "running":
                            self.task_runner.resume()
                    else:
                        print(f"[LIVE] {self.task_runner.channel} went offline. Pausing viewers...")
                        if self.task_runner.status == "running":
                            self.task_runner.pause()
            except Exception as e:
                print(f"[LIVE] Error: {e}")

            await asyncio.sleep(self.interval)

    async def _check_live(self) -> bool:
        client_id = self._next_client_id()
        oauth_token = self._next_oauth_token()
        url = f"https://api.twitch.tv/helix/streams?user_login={self.task_runner.channel}"

        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {oauth_token}"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Twitch API returned {resp.status}")
                data = await resp.json()
                return len(data.get("data", [])) > 0

    def stop(self):
        self.running = False
