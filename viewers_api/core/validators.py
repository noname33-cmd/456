# core/validators.py
import asyncio
import aiohttp
from typing import List, Tuple
from core.proxy import to_proxy_url

DEFAULT_TIMEOUT = 8


async def _check_proxy_one(session: aiohttp.ClientSession, proxy_url: str) -> bool:
    """
    Проверяем факт сетевого коннекта через прокси (быстрый 204 endpoint).
    На вход уже подаётся нормализованный proxy_url.
    """
    try:
        async with session.get(
                "https://www.google.com/generate_204",
                proxy=proxy_url,
        ) as resp:
            return resp.status in (200, 204)
    except Exception:
        return False


async def _check_token_one(session: aiohttp.ClientSession, token: str) -> bool:
    """
    Валидируем токен через GQL (достаточно 200/400/401/403, факт ответа важнее).
    """
    try:
        headers = {
            "Authorization": f"OAuth {token}",
            "Client-ID": "kimne78kx3ncx6brgo4mv6wki5h1ko",
        }
        async with session.post(
                "https://gql.twitch.tv/gql",
                json=[{
                    "operationName": "PlaybackAccessToken_Template",
                    "variables": {"isLive": False, "login": "twitch", "isVod": False, "vodID": "", "playerType": "site"},
                    "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6"}}
                }],
                headers=headers,
        ) as resp:
            return resp.status in (200, 400, 401, 403)
    except Exception:
        return False


async def validate_proxies(
        proxies: List[str],
        concurrency: int = 200,
        timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    """
    Принимает сырые строки прокси (ip:port, user:pass@ip:port, ...),
    возвращает ТОЛЬКО нормализованные URL'ы, прошедшие проверку.
    """
    if not proxies:
        return []

    # 1) нормализуем всё входящее (отсекаем мусор ещё до проверки)
    normalized: List[Tuple[str, str]] = []  # (raw, url)
    for p in proxies:
        url = to_proxy_url(p)
        if url:
            normalized.append((p, url))

    if not normalized:
        return []

    sem = asyncio.Semaphore(concurrency)

    async def _wrap(proxy_url: str):
        async with sem:
            return await _check_proxy_one(sess, proxy_url)

    timeout_cfg = aiohttp.ClientTimeout(total=timeout + 2)
    async with aiohttp.ClientSession(timeout=timeout_cfg) as sess:
        tasks = [asyncio.create_task(_wrap(url)) for _, url in normalized]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        # ВОЗВРАЩАЕМ ИМЕННО НОРМАЛИЗОВАННЫЕ URL
        return [url for (_, url), ok in zip(normalized, results) if ok]


async def validate_tokens(
        tokens: List[str],
        concurrency: int = 200,
        timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    """
    Принимает сырые токены и возвращает только валидные токены.
    """
    if not tokens:
        return []

    sem = asyncio.Semaphore(concurrency)

    async def _wrap(tok: str):
        async with sem:
            return await _check_token_one(sess, tok)

    timeout_cfg = aiohttp.ClientTimeout(total=timeout + 2)
    async with aiohttp.ClientSession(timeout=timeout_cfg) as sess:
        tasks = [asyncio.create_task(_wrap(t)) for t in tokens if t.strip()]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [t for t, ok in zip([t for t in tokens if t.strip()], results) if ok]


# ==== Синхронные обёртки (удобно для GUI/скриптов) ====

def validate_proxies_sync(
        proxies: List[str],
        concurrency: int = 200,
        timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    return asyncio.run(validate_proxies(proxies, concurrency=concurrency, timeout=timeout))


def validate_tokens_sync(
        tokens: List[str],
        concurrency: int = 200,
        timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    return asyncio.run(validate_tokens(tokens, concurrency=concurrency, timeout=timeout))
