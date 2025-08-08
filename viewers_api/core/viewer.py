# core/viewer.py
import asyncio
import aiohttp
import random
import ssl
import contextlib
import websockets
import urllib.parse
from typing import Optional

from core.kasada import KasadaSolver

TWITCH_GQL_URL = "https://gql.twitch.tv/gql"
TWITCH_USHER_URL = "https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8"
TWITCH_CHAT_WSS = "wss://irc-ws.chat.twitch.tv:443"

TWITCH_CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "n8z5w9dwbaj38c6u1hk3w1ys3k4hv9",
    "gp762oj0d8ezg5drgsw9j7f0ga1pno",
    "a9nwnjtb9ce8b6gmz9fwae3gftusds",
]

# === как в Go: генерация случайной строки фиксированной длины ===
_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
def _rand_str(n: int) -> str:
    # простая и быстрая реализация как в Go (rand/v2)
    return "".join(random.choice(_ALPHABET) for _ in range(n))


class ViewerBot:
    def __init__(self, channel: str, proxy: Optional[str] = None, token: str = ""):
        self.channel = channel
        self.proxy = proxy
        self.token = token

        self._running = True
        self._status = "idle"
        self._session: Optional[aiohttp.ClientSession] = None
        self._backoff = (1, 2, 4, 8, 15)
        self._integrity_headers: dict = {}
        self._playlist_url: Optional[str] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._playback_signature = ""
        self._playback_token = ""

        # === НОВОЕ: X-Device-Id хранится на боте и живёт всю жизнь экземпляра ===
        self._x_device_id = _rand_str(32)

    async def run(self, pause_event: asyncio.Event):
        self._status = "running"
        backoff_idx = 0
        while self._running:
            await pause_event.wait()
            if not self._running:
                break
            try:
                await self._get_kasada_headers()
                await self._ensure_session()
                await self._get_playback_token()
                await self._form_playlist_url()
                await self._connect_ws_chat()
                await self._heartbeat_loop()
                backoff_idx = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[ViewerBot] Error for {self.channel}: {e}")
                delay = self._backoff[min(backoff_idx, len(self._backoff) - 1)]
                backoff_idx += 1
                await self._cleanup(light=True)
                await asyncio.sleep(delay)
        await self._cleanup(light=False)

    async def _ensure_session(self):
        if self._session and not self._session.closed:
            await self._session.close()

        ua = self._integrity_headers.get("User-Agent") or self._integrity_headers.get("user-agent") \
             or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"

        headers = {
            "User-Agent": ua,
            "Accept": "*/*",
            "Origin": "https://www.twitch.tv",
            "Referer": f"https://www.twitch.tv/{self.channel}",
            # === НОВОЕ: прокидываем X-Device-Id в базовые хедеры сессии ===
            "X-Device-Id": self._x_device_id,
        }
        if self.token:
            headers["Authorization"] = f"OAuth {self.token}"

        for k, v in self._integrity_headers.items():
            if k.lower().startswith("x-kpsdk"):
                headers[k] = v

        self._session = aiohttp.ClientSession(headers=headers)

    async def _get_kasada_headers(self):
        self._integrity_headers = await KasadaSolver.get_integrity(
            token=self.token, proxy=self.proxy
        )
        if "User-Agent" not in self._integrity_headers and "user-agent" in self._integrity_headers:
            self._integrity_headers["User-Agent"] = self._integrity_headers["user-agent"]
        print(f"[ViewerBot] Kasada headers set for {self.channel}")

    async def _get_playback_token(self):
        client_id = random.choice(TWITCH_CLIENT_IDS)
        headers = {
            "Client-ID": client_id,
            "Content-Type": "application/json",
            "Origin": "https://www.twitch.tv",
            "Referer": f"https://www.twitch.tv/{self.channel}",
            # === НОВОЕ: как и в Go — добавляем X-Device-Id на GQL ===
            "X-Device-Id": self._x_device_id,
        }
        if self.token:
            headers["Authorization"] = f"OAuth {self.token}"

        payload = [{
            "operationName": "PlaybackAccessToken_Template",
            "variables": {
                "isLive": True,
                "login": self.channel,
                "isVod": False,
                "vodID": "",
                "playerType": "site"
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6"
                }
            }
        }]

        async with self._session.post(TWITCH_GQL_URL, headers=headers, json=payload, proxy=self.proxy) as resp:
            if resp.status != 200:
                raise RuntimeError(f"GQL error: {resp.status}")
            data = await resp.json()
            sig = data[0]["data"]["streamPlaybackAccessToken"]["signature"]
            token = data[0]["data"]["streamPlaybackAccessToken"]["value"]
            self._playback_signature = sig
            self._playback_token = token

        print(f"[ViewerBot] Playback token received for {self.channel}")

    async def _form_playlist_url(self):
        token_qs = urllib.parse.quote(self._playback_token, safe="")
        self._playlist_url = (
                TWITCH_USHER_URL.format(channel=self.channel) +
                f"?sig={self._playback_signature}&token={token_qs}" +
                "&allow_source=true&fast_bread=true&p=123456"
        )
        print(f"[ViewerBot] Playlist URL ready for {self.channel}")

    async def _connect_ws_chat(self):
        try:
            ssl_context = ssl.create_default_context()
            ws_headers = {
                "User-Agent": self._integrity_headers.get("User-Agent", "Mozilla/5.0"),
                "Origin": "https://www.twitch.tv",
            }
            self._ws = await websockets.connect(
                TWITCH_CHAT_WSS,
                extra_headers=ws_headers,
                ssl=ssl_context
            )
            if self.token:
                await self._ws.send(f"PASS oauth:{self.token}")
            nick = f"justinfan{random.randint(10000, 99999)}"
            await self._ws.send(f"NICK {nick}")
            await self._ws.send(f"JOIN #{self.channel}")
            print(f"[ViewerBot] WS Chat connected to {self.channel} as {nick}")
        except Exception as e:
            print(f"[ViewerBot] WS chat connect skipped: {e}")
            self._ws = None

    async def _heartbeat_loop(self):
        assert self._playlist_url, "Playlist URL not set"

        while self._running:
            async with self._session.get(self._playlist_url, proxy=self.proxy) as r:
                if r.status in (401, 403, 410):
                    await self._refresh_tokens()
                    continue
                r.raise_for_status()
                master = await r.text()

            media_url = None
            for line in (l.strip() for l in master.splitlines() if l.strip()):
                if line.endswith(".m3u8") and not line.startswith("#"):
                    media_url = urllib.parse.urljoin(self._playlist_url, line)
            if not media_url:
                await asyncio.sleep(10)
                continue

            async with self._session.get(media_url, proxy=self.proxy) as r:
                if r.status in (401, 403, 410):
                    await self._refresh_tokens()
                    continue
                r.raise_for_status()
                media = await r.text()

            seg_url = None
            for line in (l.strip() for l in media.splitlines() if l and not line.startswith("#")):
                seg_url = urllib.parse.urljoin(media_url, line)
                break

            if seg_url:
                async with self._session.get(seg_url, proxy=self.proxy) as r:
                    if r.status in (401, 403, 410):
                        await self._refresh_tokens()
                        continue
                    r.raise_for_status()
                    await r.read()

            await asyncio.sleep(20)

    async def _refresh_tokens(self):
        print(f"[ViewerBot] Refreshing tokens for {self.channel}")
        await self._get_kasada_headers()
        await self._ensure_session()
        await self._get_playback_token()
        await self._form_playlist_url()

    def stop(self):
        self._running = False
        self._status = "stopped"

    async def _cleanup(self, light: bool):
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        if not light and self._session and not self._session.closed:
            await self._session.close()
            self._session = None
