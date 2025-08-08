# core/kasada.py
from __future__ import annotations

import contextlib
import json
import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Literal

import aiohttp

# читаем конфиг на каждый вызов — можно менять без рестарта
from config import load_settings

# ====== поведение/кэш ======
_LAST_OK_PROVIDER: Optional[Literal["notion", "salamoonder", "fallback"]] = None
_LAST_OK_TS: float = 0.0
_OK_TTL_SEC = 600  # 10 минут держим «любимого» провайдера

_COOLDOWN_UNTIL = {"notion": 0.0, "salamoonder": 0.0}
_COOLDOWN_SEC = 300  # 5 минут — не дёргаем упавшего провайдера

# ====== константы ======
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

TWITCH_CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "kd1unb4b3q4t58fwlpcbzcbnm76a8fp",
    "n8z5w9dwbaj38c6u1hk3w1ys3k4hv9",
]

@dataclass
class Integrity:
    user_agent: str
    x_kpsdk_ct: str
    x_kpsdk_cd: str
    x_kpsdk_v: str = ""
    x_is_human: str = ""   # Новое поле из Kasada+BotID
    task_id: str = ""
    provider: str = "fallback"  # 'notion' | 'salamoonder' | 'fallback'


# ====== helpers ======
def _now() -> float:
    return time.time()

def _provider_available(name: str) -> bool:
    return _now() >= _COOLDOWN_UNTIL.get(name, 0.0)

def _cooldown(name: str):
    _COOLDOWN_UNTIL[name] = _now() + _COOLDOWN_SEC

def _remember_ok(name: str):
    global _LAST_OK_PROVIDER, _LAST_OK_TS
    _LAST_OK_PROVIDER = name
    _LAST_OK_TS = _now()

def _preferred_cached() -> Optional[str]:
    if _LAST_OK_PROVIDER and (_now() - _LAST_OK_TS) <= _OK_TTL_SEC:
        return _LAST_OK_PROVIDER
    return None

def _headers_json():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": DEFAULT_UA,
    }

def _solution_url(host: str) -> str:
    return (host or "").rstrip("/") + "/api/v1/solution"


# ====== Notion ======
def _pick(data: dict, *keys: str, default: str = "") -> str:
    """Берём первое непустое значение из списка ключей (поддержка x_kpsdk_cd / x-kpsdk-cd и т.п.)."""
    for k in keys:
        v = data.get(k, "")
        if isinstance(v, str) and v:
            return v
    return default

async def _notion_get_or_post(host: str, apikey: str) -> Integrity:
    """
    1) Пытаемся GET /api/v1/solution?apikey=...&site=twitch
    2) Если 400 — пробуем POST на тот же путь с JSON телом {apikey, site}
    """
    url = _solution_url(host)
    params = {"apikey": apikey, "site": "twitch"}

    async with aiohttp.ClientSession() as s:
        # --- GET ---
        try:
            async with s.get(url, params=params, headers=_headers_json(), raise_for_status=False) as r:
                txt = await r.text()
                if r.status == 200:
                    data = json.loads(txt)
                    if not data.get("status"):
                        raise RuntimeError("notion: status=false")
                    print(f"[Kasada] provider=notion status=200 (GET)")
                    return Integrity(
                        user_agent=_pick(data, "user-agent", default=DEFAULT_UA),
                        x_kpsdk_cd=_pick(data, "x_kpsdk_cd", "x-kpsdk-cd"),
                        x_kpsdk_ct=_pick(data, "x_kpsdk_ct", "x-kpsdk-ct"),
                        x_kpsdk_v=_pick(data, "x_kpsdk_v", "x-kpsdk-v"),
                        x_is_human=_pick(data, "x_is_human", "x-is-human"),  # ← новое поле если вернут
                        task_id=data.get("id", ""),
                        provider="notion",
                    )
                if 400 <= r.status < 500:
                    print(f"[Kasada] notion GET {r.status} body={txt[:200]}")
                else:
                    print(f"[Kasada] notion GET {r.status}")
        except Exception as e:
            print(f"[Kasada] notion GET error: {e}")

        # --- POST (fallback) ---
        payload = {"apikey": apikey, "site": "twitch"}
        try:
            async with s.post(url, json=payload, headers=_headers_json(), raise_for_status=False) as r:
                txt = await r.text()
                if r.status == 200:
                    data = json.loads(txt)
                    if not data.get("status"):
                        raise RuntimeError("notion: status=false (POST)")
                    print(f"[Kasada] provider=notion status=200 (POST)")
                    return Integrity(
                        user_agent=_pick(data, "user-agent", default=DEFAULT_UA),
                        x_kpsdk_cd=_pick(data, "x_kpsdk_cd", "x-kpsdk-cd"),
                        x_kpsdk_ct=_pick(data, "x_kpsdk_ct", "x-kpsdk-ct"),
                        x_kpsdk_v=_pick(data, "x_kpsdk_v", "x-kpsdk-v"),
                        x_is_human=_pick(data, "x_is_human", "x-is-human"),
                        task_id=data.get("id", ""),
                        provider="notion",
                    )
                print(f"[Kasada] notion POST {r.status} body={txt[:200]}")
        except Exception as e:
            print(f"[Kasada] notion POST error: {e}")

    raise RuntimeError("error getting solution (notion)")

async def _notion_delete(host: str, apikey: str, task_id: str):
    if not (host and apikey and task_id):
        return
    url = _solution_url(host)
    params = {"apikey": apikey, "id": task_id}
    async with aiohttp.ClientSession() as s:
        with contextlib.suppress(Exception):
            await s.delete(url, params=params, headers=_headers_json())


# ====== Salamoonder ======
async def _salamoonder_get(apikey: str, timeout_sec: int = 15) -> Integrity:
    """
    1) POST /api/createTask -> taskId
    2) polling POST /api/getTaskResult
    Возвращает (помимо стандартных полей) x-is-human, если сервис его отдаёт.
    """
    create_url = "https://salamoonder.com/api/createTask"
    get_url = "https://salamoonder.com/api/getTaskResult"

    async with aiohttp.ClientSession() as s:
        # create
        create_payload = {
            "api_key": apikey,
            "task": {
                "type": "KasadaCaptchaSolver",
                "pjs": "https://k.twitchcdn.net/149e9513-01fa-4fb0-aad4-566afd725d1b/2d206a39-8ed7-437e-a3be-862e0f06eea3/p.js",
                "cdOnly": "false",
            },
        }
        async with s.post(create_url, json=create_payload, headers=_headers_json(), raise_for_status=False) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"salamoonder create: {r.status} {txt[:200]}")
            data = json.loads(txt)
            task_id = data.get("taskId", "")
            if not task_id:
                raise RuntimeError("salamoonder create: no taskId")

        # poll
        started = _now()
        while _now() - started <= timeout_sec:
            async with s.post(
                    get_url,
                    json={"api_key": apikey, "taskId": task_id},
                    headers=_headers_json(),
                    raise_for_status=False
            ) as r:
                txt = await r.text()
                if r.status != 200:
                    print(f"[Kasada] salamoonder get: {r.status} {txt[:200]}")
                else:
                    data = json.loads(txt)
                    if data.get("status") == "ready":
                        sol = data.get("solution", {}) or {}
                        x_is_human = _pick(sol, "x_is_human", "x-is-human")
                        if x_is_human:
                            print("[Kasada] provider=salamoonder status=200 (x-is-human present)")
                        else:
                            print("[Kasada] provider=salamoonder status=200")

                        return Integrity(
                            user_agent=_pick(sol, "user-agent", default=DEFAULT_UA),
                            x_kpsdk_cd=_pick(sol, "x-kpsdk-cd"),
                            x_kpsdk_ct=_pick(sol, "x-kpsdk-ct"),
                            x_kpsdk_v=_pick(sol, "x-kpsdk-v"),
                            x_is_human=x_is_human,  # ← пробрасываем, если отдали
                            provider="salamoonder",
                        )
            await asyncio.sleep(1)

    raise RuntimeError("timeout getting solution (salamoonder)")


# ====== Twitch fallback ======
async def _integrity_fallback() -> Integrity:
    async with aiohttp.ClientSession() as s:
        # пробуем с 1–2 разными Client-ID
        for cid in TWITCH_CLIENT_IDS[:2]:
            headers = _headers_json() | {
                "Origin": "https://www.twitch.tv",
                "Referer": "https://www.twitch.tv/",
                "Client-ID": cid,
            }
            try:
                async with s.post("https://gql.twitch.tv/integrity", headers=headers, json={}, raise_for_status=False) as r:
                    txt = await r.text()
                    if r.status == 200:
                        data = json.loads(txt)
                        print(f"[Kasada] provider=fallback status=200 cid={cid[:6]}…")
                        return Integrity(
                            user_agent=data.get("user_agent", DEFAULT_UA),
                            x_kpsdk_cd=data.get("x-kpsdk-cd", ""),
                            x_kpsdk_ct=data.get("x-kpsdk-ct", ""),
                            x_kpsdk_v=data.get("x-kpsdk-v", ""),
                            x_is_human="",  # Twitch fallback это не отдаёт
                            provider="fallback",
                        )
                    print(f"[Kasada] fallback cid={cid[:6]}… status={r.status} body={txt[:200]}")
            except Exception as e:
                print(f"[Kasada] fallback error cid={cid[:6]}…: {e}")

        # без Client-ID
        headers = _headers_json() | {
            "Origin": "https://www.twitch.tv",
            "Referer": "https://www.twitch.tv/",
        }
        try:
            async with s.post("https://gql.twitch.tv/integrity", headers=headers, json={}, raise_for_status=False) as r:
                txt = await r.text()
                if r.status == 200:
                    data = json.loads(txt)
                    print(f"[Kasada] provider=fallback status=200 no-cid")
                    return Integrity(
                        user_agent=data.get("user_agent", DEFAULT_UA),
                        x_kpsdk_cd=data.get("x-kpsdk-cd", ""),
                        x_kpsdk_ct=data.get("x-kpsdk-ct", ""),
                        x_kpsdk_v=data.get("x-kpsdk-v", ""),
                        x_is_human="",
                        provider="fallback",
                    )
                print(f"[Kasada] fallback no-cid status={r.status} body={txt[:200]}")
        except Exception as e:
            print(f"[Kasada] fallback error no-cid: {e}")

    # минимальный безопасный набор
    return Integrity(DEFAULT_UA, "", "", "", x_is_human="", provider="fallback")


# ====== Публичный API, совместимый с viewer.py ======
class KasadaSolver:
    @staticmethod
    async def get_integrity(*, token: str | None = None, proxy: str | None = None) -> dict:
        """
        Возвращает словарь заголовков/полей:
        - x-kpsdk-ct, x-kpsdk-cd, x-kpsdk-v
        - x-is-human (если вернул провайдер)
        - user-agent / User-Agent
        - task_id
        """
        del token, proxy  # совместимость со старым вызовом

        cfg = load_settings()
        sal = getattr(cfg.kasada, "salamoonder", None)
        notn = getattr(cfg.kasada, "notion", None)

        order = ("notion", "salamoonder")  # сначала Notion, потом Salamoonder

        async def _try(name: str) -> Optional[Integrity]:
            nonlocal sal, notn
            if name == "notion":
                if not (notn and getattr(notn, "enable", False)) or not _provider_available("notion"):
                    return None
                try:
                    integ = await _notion_get_or_post(
                        host=getattr(notn, "host", ""),
                        apikey=getattr(notn, "apikey", ""),
                    )
                    _remember_ok("notion")
                    return integ
                except Exception as e:
                    print(f"[Kasada] notion_failed='{e}' → next")
                    _cooldown("notion")
                    return None

            if name == "salamoonder":
                if not (sal and getattr(sal, "enable", False)) or not _provider_available("salamoonder"):
                    return None
                try:
                    integ = await _salamoonder_get(getattr(sal, "apikey", ""))
                    _remember_ok("salamoonder")
                    return integ
                except Exception as e:
                    print(f"[Kasada] salamoonder_failed='{e}' → next")
                    _cooldown("salamoonder")
                    return None
            return None

        # 0) кешированный провайдер
        pref = _preferred_cached()
        if pref:
            res = await _try(pref)
            if res:
                return _to_headers(res)

        # 1) основной порядок
        for name in order:
            res = await _try(name)
            if res:
                return _to_headers(res)

        # 2) fallback
        integ = await _integrity_fallback()
        _remember_ok("fallback")
        return _to_headers(integ)

    @staticmethod
    async def delete(task_id: str):
        # удаление доступно только для Notion
        cfg = load_settings()
        notn = getattr(cfg.kasada, "notion", None)
        if not (notn and getattr(notn, "enable", False) and task_id):
            return
        with contextlib.suppress(Exception):
            await _notion_delete(getattr(notn, "host", ""), getattr(notn, "apikey", ""), task_id)


def _to_headers(integ: Integrity) -> dict:
    # viewer.py ожидает именно эти ключи
    out = {
        "x-kpsdk-ct": integ.x_kpsdk_ct,
        "x-kpsdk-cd": integ.x_kpsdk_cd,
        "x-kpsdk-v": integ.x_kpsdk_v,
        "user-agent": integ.user_agent,
        "User-Agent": integ.user_agent,
        "task_id": integ.task_id,
    }
    # новое поле пробрасываем только если оно есть
    if integ.x_is_human:
        out["x-is-human"] = integ.x_is_human
    return out
