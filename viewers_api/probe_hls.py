# probe_hls.py
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from typing import Optional, Tuple

import aiohttp

# наши хедеры от касады
from core.kasada import KasadaSolver

TWITCH_GQL_URL = "https://gql.twitch.tv/gql"
USHER_URL_TMPL = "https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8"

# те же client-id что и в проекте
TWITCH_CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "kd1unb4b3q4t58fwlpcbzcbnm76a8fp",
    "n8z5w9dwbaj38c6u1hk3w1ys3k4hv9",
]

# persisted query payload, как было
def build_gql_payload(channel: str) -> list[dict]:
    return [{
        "operationName": "PlaybackAccessToken_Template",
        "variables": {
            "isLive": True,
            "login": channel,
            "isVod": False,
            "vodID": "",
            "playerType": "site",
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6",
            }
        }
    }]

# полный текст запроса — fallback, если получаем PersistedQueryNotFound
FULL_GQL_QUERY = """
query PlaybackAccessToken_Template($login: String!, $isLive: Boolean!, $vodID: ID!, $isVod: Boolean!, $playerType: String!) {
  streamPlaybackAccessToken(channelName: $login, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) @include(if: $isLive) {
    signature
    value
  }
  videoPlaybackAccessToken(id: $vodID, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) @include(if: $isVod) {
    signature
    value
  }
}
"""

def build_full_gql_payload(channel: str) -> dict:
    return {
        "operationName": "PlaybackAccessToken_Template",
        "query": FULL_GQL_QUERY,
        "variables": {
            "isLive": True,
            "login": channel,
            "isVod": False,
            "vodID": "",
            "playerType": "site",
        }
    }

async def post_json(session: aiohttp.ClientSession, url: str, payload, *, headers: dict, proxy: Optional[str]) -> tuple[int, str]:
    async with session.post(url, json=payload, headers=headers, proxy=proxy) as r:
        txt = await r.text()
        return r.status, txt

def load_lines(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def first_or_none(items: list[str]) -> Optional[str]:
    return items[0] if items else None

def make_proxy_url(raw: str) -> Optional[str]:
    raw = raw.strip()
    if not raw:
        return None
    if "://" not in raw:
        # поддержка user:pass@host:port и host:port:user:pass
        if "@" in raw:
            return "http://" + raw
        parts = raw.split(":")
        if len(parts) == 2:
            host, port = parts
            return f"http://{host}:{port}"
        if len(parts) == 4:
            host, port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        return None
    return raw

async def get_playback_token(
        session: aiohttp.ClientSession,
        channel: str,
        x_device_id: str,
        *,
        oauth: Optional[str],
        kasada_headers: dict,
        proxy: Optional[str],
) -> Tuple[str, str]:
    """
    Возвращает (signature, token). Сначала пробуем persistedQuery.
    Если ответ содержит PersistedQueryNotFound — повторяем с полным текстом запроса.
    """
    headers = {
        "Client-ID": random.choice(TWITCH_CLIENT_IDS),
        "Content-Type": "application/json",
        "Origin": "https://www.twitch.tv",
        "Referer": f"https://www.twitch.tv/{channel}",
        "X-Device-Id": x_device_id,
        "User-Agent": kasada_headers.get("User-Agent") or kasada_headers.get("user-agent") or "",
    }
    # пробрасываем x-kpsdk-* если есть
    for k in ("x-kpsdk-ct", "x-kpsdk-cd", "x-kpsdk-v"):
        v = kasada_headers.get(k)
        if v:
            headers[k] = v
    if oauth:
        headers["Authorization"] = f"OAuth {oauth}"

    # 1) persistedQuery
    payload = build_gql_payload(channel)
    status, txt = await post_json(session, TWITCH_GQL_URL, payload, headers=headers, proxy=proxy)
    if status == 200:
        try:
            data = json.loads(txt)
            node = data[0]["data"]["streamPlaybackAccessToken"]
            return node["signature"], node["value"]
        except Exception:
            if '"PersistedQueryNotFound"' not in txt:
                raise RuntimeError(f"GQL missing data -> {txt[:500]}")
            # иначе упадем в фулл-запрос
    else:
        if '"PersistedQueryNotFound"' not in txt:
            raise RuntimeError(f"GQL {status}: {txt[:500]}")

    # 2) полный текст запроса
    full_payload = build_full_gql_payload(channel)
    status2, txt2 = await post_json(session, TWITCH_GQL_URL, full_payload, headers=headers, proxy=proxy)
    if status2 != 200:
        raise RuntimeError(f"GQL(full) {status2}: {txt2[:500]}")
    try:
        data2 = json.loads(txt2)
        node2 = data2["data"]["streamPlaybackAccessToken"]
        return node2["signature"], node2["value"]
    except Exception:
        raise RuntimeError(f"GQL(full) missing data -> {txt2[:500]}")

async def fetch_text(session: aiohttp.ClientSession, url: str, *, headers: dict, proxy: Optional[str]) -> str:
    async with session.get(url, headers=headers, proxy=proxy) as r:
        if r.status != 200:
            txt = await r.text()
            raise RuntimeError(f"HTTP {r.status}: {txt[:200]}")
        return await r.text()

def build_usher_url(channel: str, sig: str, token: str) -> str:
    from urllib.parse import quote
    token_qs = quote(token, safe="")
    return (
            USHER_URL_TMPL.format(channel=channel)
            + f"?sig={sig}&token={token_qs}"
            + "&allow_source=true&fast_bread=true&p=" + str(random.randint(10_000, 99_999_999))
    )

def parse_media_url(master_m3u8: str, base_url: str) -> Optional[str]:
    from urllib.parse import urljoin
    media = None
    for line in (ln.strip() for ln in master_m3u8.splitlines() if ln.strip()):
        if line.endswith(".m3u8") and not line.startswith("#"):
            media = urljoin(base_url, line)
    return media

def first_segment_url(media_m3u8: str, base_url: str) -> Optional[str]:
    from urllib.parse import urljoin
    for line in (ln.strip() for ln in media_m3u8.splitlines() if ln and not ln.startswith("#")):
        return urljoin(base_url, line)
    return None

async def probe_once(
        channel: str,
        *,
        use_proxy: bool,
        use_oauth: bool,
        proxy_raws: list[str],
        oauth_tokens: list[str],
) -> bool:
    kasada = await KasadaSolver.get_integrity()
    print("[Kasada] headers ok (provider may be: notion/salamoonder )")

    x_device_id = "".join(random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(32))
    proxy_url = make_proxy_url(random.choice(proxy_raws)) if (use_proxy and proxy_raws) else None
    oauth = random.choice(oauth_tokens) if (use_oauth and oauth_tokens) else None

    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(f"[i] Getting token for #{channel} (proxy={'True' if proxy_url else 'False'}) …")
        sig, token = await get_playback_token(session, channel, x_device_id, oauth=oauth, kasada_headers=kasada, proxy=proxy_url)

        playlist_url = build_usher_url(channel, sig, token)
        headers = {
            "User-Agent": kasada.get("User-Agent") or kasada.get("user-agent") or "",
            "Origin": "https://www.twitch.tv",
            "Referer": f"https://www.twitch.tv/{channel}",
            "X-Device-Id": x_device_id,
        }
        for k in ("x-kpsdk-ct", "x-kpsdk-cd", "x-kpsdk-v"):
            v = kasada.get(k)
            if v:
                headers[k] = v

        master = await fetch_text(session, playlist_url, headers=headers, proxy=proxy_url)
        media_url = parse_media_url(master, playlist_url)
        if not media_url:
            print("[!] No media playlist in master")
            return False

        media = await fetch_text(session, media_url, headers=headers, proxy=proxy_url)
        seg_url = first_segment_url(media, media_url)
        if not seg_url:
            print("[!] No segment in media playlist")
            return False

        # пробуем скачать первый сегмент (head/или часть)
        async with session.get(seg_url, headers=headers, proxy=proxy_url) as r:
            if r.status != 200:
                txt = await r.text()
                print(f"[!] Segment HTTP {r.status}: {txt[:120]}")
                return False
            # читнем кусочек
            await r.content.readany()
        print("[OK] segment fetched")
        return True

async def main():
    ap = argparse.ArgumentParser(description="Probe Twitch HLS with persistedQuery fallback")
    ap.add_argument("--channel", required=True, help="twitch channel login")
    ap.add_argument("--retries", type=int, default=6)
    ap.add_argument("--use-proxy", action="store_true")
    ap.add_argument("--use-oauth", action="store_true")
    ap.add_argument("--proxies-file", default="proxies.txt")
    ap.add_argument("--tokens-file", default="tokens.txt")
    args = ap.parse_args()

    proxies = load_lines(args.proxies_file)
    tokens = load_lines(args.tokens_file)

    for attempt in range(1, args.retries + 1):
        print(f"=== Attempt {attempt}/{args.retries} | proxy={'yes' if args.use_proxy else 'no'} | oauth={'yes' if args.use_oauth else 'no'} ===")
        try:
            ok = await probe_once(
                args.channel,
                use_proxy=args.use_proxy,
                use_oauth=args.use_oauth,
                proxy_raws=proxies,
                oauth_tokens=tokens,
            )
            if ok:
                return
        except Exception as e:
            print(f"[ERR] {e}")
        await asyncio.sleep(1.2)

    sys.exit(2)

if __name__ == "__main__":
    asyncio.run(main())
