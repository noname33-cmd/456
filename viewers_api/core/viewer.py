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

PLAYBACK_TOKEN_QUERY = """
query PlaybackAccessToken_Template(
  $login: String!,
  $isLive: Boolean!,
  $vodID: ID!,
  $isVod: Boolean!,
  $playerType: String!
) {
  streamPlaybackAccessToken(
    channelName: $login,
    params: { platform: "web", playerBackend: "mediaplayer", playerType: $playerType }
  ) @include(if: $isLive) {
    value
    signature
    __typename
  }
  videoPlaybackAccessToken(
    id: $vodID,
    params: { platform: "web", playerBackend: "mediaplayer", playerType: $playerType }
  ) @include(if: $isVod) {
    value
    signature
    __typename
  }
}
""".strip()

_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
def _rand_str(n: int) -> str:
    return "".join(random.choice(_ALPHABET) for _ in range(n))

def _rand_p() -> str:
    return str(random.randint(100000, 99999999))

def _jitter(base: float, spread: float = 0.35) -> float:
    k = 1.0 + random.uniform(-spread, spread)
    return max(0.01, base * k)

QUALITY_PREFERENCES = ["chunked","1080p60","1080p","900p60","720p60","720p","480p","360p","160p"]

class ViewerBot:
    def __init__(self, channel: str, proxy: Optional[str] = None, token: str = ""):
        self.channel = channel
        self.proxy = proxy
        self.token = token

        self._running = True
        self._status = "idle"
        self._session: Optional[aiohttp.ClientSession] = None
        self._backoff = (1, 2, 4, 8, 15, 25)
        self._integrity_headers: dict = {}
        self._playlist_url: Optional[str] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._playback_signature = ""
        self._playback_token = ""
        self._last_quality = None

        self._x_device_id = _rand_str(32)

    async def run(self, pause_event: asyncio.Event):
        self._status = "running"
        backoff_idx = 0
        while self._running:
            await pause_event.wait()
            if not self._running:
                break
            try:
                if not self._session or self._session.closed:
                    await self._bootstrap_session()

                await self._ensure_tokens_and_playlist()
                await self._connect_ws_chat()
                await self._heartbeat_loop()
                backoff_idx = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[ViewerBot] Error for {self.channel}: {e}")
                delay = _jitter(self._backoff[min(backoff_idx, len(self._backoff) - 1)], 0.25)
                backoff_idx += 1
                await self._cleanup(light=True)
                await asyncio.sleep(delay)
        await self._cleanup(light=False)

    async def _bootstrap_session(self):
        self._integrity_headers = await KasadaSolver.get_integrity(token=self.token, proxy=self.proxy)
        ua = self._integrity_headers.get("User-Agent") or self._integrity_headers.get("user-agent") \
             or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"

        headers = {
            "User-Agent": ua,
            "Accept": "*/*",
            "Origin": "https://www.twitch.tv",
            "Referer": f"https://www.twitch.tv/{self.channel}",
            "X-Device-Id": self._x_device_id,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        if self.token:
            headers["Authorization"] = f"OAuth {self.token}"

        # Включаем ВСЕ x-* заголовки (x-kpsdk-*, x-is-human, …)
        for k, v in self._integrity_headers.items():
            kl = k.lower()
            if kl.startswith("x-"):
                headers[k] = v
            elif k in ("User-Agent", "user-agent"):
                headers["User-Agent"] = v

        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        print(f"[ViewerBot] Session bootstrap for {self.channel}")

    async def _ensure_tokens_and_playlist(self):
        await self._get_playback_token()
        await self._form_playlist_url()

    async def _get_playback_token(self):
        client_id = random.choice(TWITCH_CLIENT_IDS)
        headers = {
            "Client-ID": client_id,
            "Content-Type": "application/json",
            "Origin": "https://www.twitch.tv",
            "Referer": f"https://www.twitch.tv/{self.channel}",
            "X-Device-Id": self._x_device_id,
        }
        if self.token:
            headers["Authorization"] = f"OAuth {self.token}"

        # Добавляем kasada/x-* в заголовки GQL-запроса (они НЕ берутся из default headers)
        for k, v in self._integrity_headers.items():
            kl = k.lower()
            if kl.startswith("x-"):
                headers[k] = v
            elif k in ("User-Agent", "user-agent"):
                headers["User-Agent"] = v

        async def _query(is_live: bool, *, use_persisted: bool):
            variables = {
                "isLive": is_live,
                "login": self.channel,
                "isVod": False,
                "vodID": "",
                "playerType": "site",
            }

            if use_persisted:
                payload = [{
                    "operationName": "PlaybackAccessToken_Template",
                    "variables": variables,
                    "extensions": {
                        "persistedQuery": {
                            "version": 1,
                            "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6"
                        }
                    }
                }]
            else:
                payload = [{
                    "operationName": "PlaybackAccessToken_Template",
                    "variables": variables,
                    "query": PLAYBACK_TOKEN_QUERY,
                }]

            async with self._session.post(TWITCH_GQL_URL, headers=headers, json=payload, proxy=self.proxy) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"GQL error {resp.status}: {text[:300]}")
                try:
                    data = await resp.json()
                except Exception:
                    raise RuntimeError(f"GQL non-JSON reply: {text[:300]}")

                items = data if isinstance(data, list) else [data]
                if not items:
                    raise RuntimeError(f"GQL empty reply: {text[:300]}")
                item0 = items[0]

                if item0.get("errors"):
                    raise RuntimeError(f"GQL errors: {item0['errors']}")

                token_node = (item0.get("data") or {}).get("streamPlaybackAccessToken")
                if not token_node or "signature" not in token_node or "value" not in token_node:
                    raise RuntimeError(
                        f"GQL missing token node (isLive={is_live}, persisted={use_persisted}): {str(item0)[:300]}"
                    )
                return token_node["signature"], token_node["value"]

        async def _query_with_fallback(is_live: bool):
            try:
                return await _query(is_live, use_persisted=True)
            except Exception as e1:
                if "PersistedQueryNotFound" in str(e1):
                    try:
                        return await _query(is_live, use_persisted=False)
                    except Exception as e2:
                        raise RuntimeError(
                            f"Persisted->Text fallback failed (isLive={is_live}). p_err={e1}; t_err={e2}"
                        )
                raise

        try:
            sig, val = await _query_with_fallback(True)
        except Exception as e1:
            try:
                sig, val = await _query_with_fallback(False)
            except Exception as e2:
                raise RuntimeError(f"Playback token failed. live_err={e1}; fallback_err={e2}")

        self._playback_signature = sig
        self._playback_token = val
        print(f"[ViewerBot] Playback token received for {self.channel}")

    async def _form_playlist_url(self):
        token_qs = urllib.parse.quote(self._playback_token, safe="")
        self._playlist_url = (
                TWITCH_USHER_URL.format(channel=self.channel)
                + f"?sig={self._playback_signature}&token={token_qs}"
                + f"&allow_source=true&fast_bread={'true' if random.random()<0.5 else 'false'}&p={_rand_p()}"
        )
        print(f"[ViewerBot] Playlist URL ready for {self.channel}")

    async def _connect_ws_chat(self):
        try:
            ssl_context = ssl.create_default_context()
            self._ws = await websockets.connect(TWITCH_CHAT_WSS, ssl=ssl_context)
            if self.token:
                await self._ws.send(f"PASS oauth:{self.token}")
            nick = f"justinfan{random.randint(10000, 99999)}"
            await self._ws.send(f"NICK {nick}")
            await self._ws.send(f"JOIN #{self.channel}")
            print(f"[ViewerBot] WS Chat connected to {self.channel} as {nick}")
        except Exception as e:
            print(f"[ViewerBot] WS chat connect skipped: {e}")
            self._ws = None

    async def _pick_variant(self, master_text: str) -> Optional[str]:
        variants, last = [], None
        for line in (l.strip() for l in master_text.splitlines() if l.strip()):
            if line.startswith("#EXT-X-STREAM-INF"):
                last = line
            elif line.endswith(".m3u8") and not line.startswith("#"):
                variants.append((last or "", line))
        if not variants:
            return None

        scored = []
        for meta, path in variants:
            q = "chunked"
            if 'VIDEO="' in meta:
                try:
                    q = meta.split('VIDEO="', 1)[1].split('"', 1)[0]
                except Exception:
                    pass
            score = QUALITY_PREFERENCES.index(q) if q in QUALITY_PREFERENCES else 999
            scored.append((score, q, path))
        scored.sort()
        idx = 0
        if random.random() < 0.25 and len(scored) > 1:
            idx = random.randint(1, min(3, len(scored) - 1))
        self._last_quality = scored[idx][1]
        return urllib.parse.urljoin(self._playlist_url, scored[idx][2])

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
            for raw in master.splitlines():
                ls = raw.strip()
                if not ls or ls.startswith("#"):
                    continue
                if ls.endswith(".m3u8"):
                    media_url = urllib.parse.urljoin(self._playlist_url, ls)
                    break

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
            for raw in media.splitlines():
                ls = raw.strip()
                if not ls or ls.startswith("#"):
                    continue
                seg_url = urllib.parse.urljoin(media_url, ls)
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
        self._integrity_headers = await KasadaSolver.get_integrity(token=self.token, proxy=self.proxy)

        h = self._session.headers.copy()
        # Обновляем UA и ВСЕ x-* (включая x-is-human)
        if "User-Agent" in self._integrity_headers:
            h["User-Agent"] = self._integrity_headers["User-Agent"]
        elif "user-agent" in self._integrity_headers:
            h["User-Agent"] = self._integrity_headers["user-agent"]

        for k, v in self._integrity_headers.items():
            kl = k.lower()
            if kl.startswith("x-"):
                h[k] = v

        h["X-Device-Id"] = self._x_device_id
        # обновляем default headers без закрытия коннектора
        self._session._default_headers = aiohttp.helpers.CIMultiDict(h)  # да, private, но работает

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
