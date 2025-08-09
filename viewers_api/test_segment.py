# test_segment.py
import argparse
import asyncio
import time
from urllib.parse import urljoin, urlparse

import aiohttp


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

def normalize_proxy(raw: str | None) -> str | None:
    """
    Принимает:
      - http://user:pass@host:port
      - user:pass@host:port
      - host:port
    Возвращает http://.. URL или None.
    """
    if not raw:
        return None
    raw = raw.strip()
    if "://" in raw:
        return raw
    if "@" in raw:
        return f"http://{raw}"
    if ":" in raw:
        # просто host:port
        return f"http://{raw}"
    return None


async def fetch_text(session: aiohttp.ClientSession, url: str, proxy: str | None, timeout: int):
    async with session.get(url, proxy=proxy, timeout=timeout) as r:
        r.raise_for_status()
        return await r.text()


async def fetch_bytes(session: aiohttp.ClientSession, url: str, proxy: str | None, timeout: int):
    async with session.get(url, proxy=proxy, timeout=timeout) as r:
        r.raise_for_status()
        return await r.read()


def is_master_playlist(text: str) -> bool:
    """
    Очень грубая эвристика: есть EXT-X-STREAM-INF → мастер.
    """
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-STREAM-INF"):
            return True
    return False


def first_url_from_playlist(base_url: str, text: str, suffix: str = ".m3u8") -> str | None:
    """
    Берём первую строку, которая заканчивается на suffix (по умолчанию .m3u8 или .ts/.mp4 из медиаплейлиста),
    и склеиваем с base_url.
    """
    for line in (l.strip() for l in text.splitlines()):
        if not line or line.startswith("#"):
            continue
        if line.endswith(suffix) or ("?" in line and line.split("?")[0].endswith(suffix)):
            return urljoin(base_url, line)
    return None


def first_segment_from_media_playlist(base_url: str, text: str) -> str | None:
    """
    Ищем первый сегмент (первая не-комментарий строка).
    """
    for line in (l.strip() for l in text.splitlines()):
        if not line or line.startswith("#"):
            continue
        return urljoin(base_url, line)
    return None


async def probe_segment(session: aiohttp.ClientSession, segment_url: str, proxy: str | None, timeout: int):
    t0 = time.perf_counter()
    try:
        data = await fetch_bytes(session, segment_url, proxy, timeout)
        dt = (time.perf_counter() - t0) * 1000
        print(f"[OK] Segment {segment_url[:100]}... size={len(data)} bytes in {dt:.1f} ms")
        return True
    except asyncio.TimeoutError:
        print(f"[ERR] Timeout fetching segment: {segment_url}")
    except aiohttp.ClientResponseError as e:
        print(f"[ERR] HTTP {e.status} on segment: {segment_url} ({e.message})")
    except Exception as e:
        print(f"[ERR] Segment fetch error: {e}")
    return False


async def resolve_segment_from_playlist(session: aiohttp.ClientSession, playlist_url: str, proxy: str | None, timeout: int):
    """
    Если дали master.m3u8 — идём в первый вариант (media.m3u8), затем берём первый сегмент.
    Если сразу media.m3u8 — берём первый сегмент.
    Возвращает segment_url или None.
    """
    print(f"[INFO] Fetch playlist: {playlist_url}")
    master_text = await fetch_text(session, playlist_url, proxy, timeout)

    if is_master_playlist(master_text):
        media_url = first_url_from_playlist(playlist_url, master_text, ".m3u8")
        if not media_url:
            print("[ERR] Master playlist parsed, but no media .m3u8 found")
            return None
        print(f"[INFO] Master → media: {media_url}")
        media_text = await fetch_text(session, media_url, proxy, timeout)
        segment_url = first_segment_from_media_playlist(media_url, media_text)
        if not segment_url:
            print("[ERR] Media playlist parsed, but no segments found")
            return None
        return segment_url
    else:
        # это уже media.m3u8
        segment_url = first_segment_from_media_playlist(playlist_url, master_text)
        if not segment_url:
            print("[ERR] Media playlist parsed, but no segments found")
            return None
        return segment_url


async def main():
    parser = argparse.ArgumentParser(
        description="HLS segment checker (supports proxy, retries, backoff)."
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--segment", help="Полный URL сегмента (.ts/.mp4) для проверки")
    g.add_argument("--playlist", help="URL на master/media .m3u8 — скрипт сам найдёт первый сегмент")

    parser.add_argument("--proxy", help="Прокси (http://user:pass@host:port | user:pass@host:port | host:port)")
    parser.add_argument("--retries", type=int, default=5, help="Количество попыток (default: 5)")
    parser.add_argument("--timeout", type=int, default=15, help="Таймаут запроса в секундах (default: 15)")
    parser.add_argument("--backoff", type=float, default=1.5, help="Множитель бэкофа (default: 1.5)")
    parser.add_argument("--initial-wait", type=float, default=1.0, help="Начальная пауза между ретраями (sec) (default: 1.0)")

    args = parser.parse_args()

    proxy_url = normalize_proxy(args.proxy)

    # Базовые заголовки
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "*/*",
        "Origin": "https://www.twitch.tv",
        "Referer": "https://www.twitch.tv/",
    }

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Если передан плейлист — сначала получим segment_url
        if args.playlist:
            try:
                segment_url = await resolve_segment_from_playlist(session, args.playlist, proxy_url, args.timeout)
            except aiohttp.ClientResponseError as e:
                print(f"[ERR] Playlist HTTP {e.status}: {e.message}")
                return
            except asyncio.TimeoutError:
                print("[ERR] Playlist timeout")
                return
            except Exception as e:
                print(f"[ERR] Playlist error: {e}")
                return

            if not segment_url:
                return
        else:
            segment_url = args.segment

        # Ретраи с бэкофом
        wait = args.initial_wait
        for attempt in range(1, args.retries + 1):
            print(f"[TRY {attempt}/{args.retries}] GET segment")
            ok = await probe_segment(session, segment_url, proxy_url, args.timeout)
            if ok:
                return
            if attempt < args.retries:
                print(f"[WAIT] {wait:.1f}s before retry…")
                await asyncio.sleep(wait)
                wait *= args.backoff

        print("[FAIL] Segment is not reachable after all retries.")


if __name__ == "__main__":
    asyncio.run(main())
