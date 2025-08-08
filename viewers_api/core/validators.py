# core/validators.py
import asyncio
import aiohttp
from typing import List, Tuple, Optional
from core.proxy import to_proxy_url

DEFAULT_TIMEOUT = 8


async def _check_proxy_one(session: aiohttp.ClientSession, proxy: str) -> bool:
    try:
        proxy_url = to_proxy_url(proxy)
        async with session.get(
                "https://gql.twitch.tv/gql",
                proxy=proxy_url,
                timeout=DEFAULT_TIMEOUT,
        ) as resp:
            return resp.status in (200, 400, 401, 403)  # факт коннекта важнее контента
    except Exception:
        return False


async def _check_token_one(session: aiohttp.ClientSession, token: str) -> bool:
    try:
        headers = {"Authorization": f"OAuth {token}", "Client-ID": "kimne78kx3ncx6brgo4mv6wki5h1ko"}
        async with session.post(
                "https://gql.twitch.tv/gql",
                json=[{
                    "operationName": "PlaybackAccessToken_Template",
                    "variables": {"isLive": False, "login": "twitch", "isVod": False, "vodID": "", "playerType": "site"},
                    "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6"}}
                }],
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
        ) as resp:
            return resp.status in (200, 400, 401, 403)
    except Exception:
        return False


async def _gather_bounded(coros, max_concurrency: int) -> List[bool]:
    sem = asyncio.Semaphore(max_concurrency)

    async def _wrap(coro):
        async with sem:
            return await coro

    return await asyncio.gather(*[_wrap(c) for c in coros])


def validate_proxies(proxies: List[str], max_concurrency: int = 100) -> List[str]:
    """
    Возвращает список ВАЛИДНЫХ прокси.
    """
    async def _run():
        timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT + 2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            coros = [_check_proxy_one(session, p) for p in proxies]
            results = await _gather_bounded(coros, max_concurrency)
            return [p for p, ok in zip(proxies, results) if ok]

    return asyncio.run(_run())


def validate_tokens(tokens: List[str], max_concurrency: int = 100) -> List[str]:
    """
    Возвращает список ВАЛИДНЫХ токенов.
    """
    async def _run():
        timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT + 2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            coros = [_check_token_one(session, t) for t in tokens]
            results = await _gather_bounded(coros, max_concurrency)
            return [t for t, ok in zip(tokens, results) if ok]

    return asyncio.run(_run())
