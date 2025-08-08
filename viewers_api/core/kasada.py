# core/kasada.py
import aiohttp
from dataclasses import dataclass
from config import settings

@dataclass
class Integrity:
    user_agent: str
    x_kpsdk_ct: str
    x_kpsdk_cd: str
    x_kpsdk_v: str = ""
    task_id: str = ""

async def _get_from_notion() -> Integrity:
    url = f"{settings.kasada.notion.host}/api/v1/solution"
    params = {"apikey": settings.kasada.notion.apikey, "site": "twitch"}
    async with aiohttp.ClientSession() as s:
        async with s.get(url, params=params) as r:
            r.raise_for_status()
            data = await r.json()
            if not data.get("status"):
                raise RuntimeError("Notion: status=false")
            return Integrity(
                user_agent=data.get("user-agent",""),
                x_kpsdk_cd=data.get("x_kpsdk_cd",""),
                x_kpsdk_ct=data.get("x_kpsdk_ct",""),
                x_kpsdk_v=data.get("x_kpsdk_v",""),
                task_id=data.get("id",""),
            )

async def _get_from_integrity() -> Integrity:
    # Fallback (упрощённый)
    async with aiohttp.ClientSession() as s:
        async with s.post("https://gql.twitch.tv/integrity") as r:
            r.raise_for_status()
            data = await r.json()
            return Integrity(
                user_agent=data.get("user_agent",""),
                x_kpsdk_cd=data.get("x-kpsdk-cd",""),
                x_kpsdk_ct=data.get("x-kpsdk-ct",""),
                x_kpsdk_v=data.get("x-kpsdk-v",""),
            )

async def get_integrity() -> Integrity:
    if settings.kasada.notion.enable and not settings.kasada.salamoonder.enable:
        return await _get_from_notion()
    if settings.kasada.salamoonder.enable and not settings.kasada.notion.enable:
        raise NotImplementedError("Salamoonder provider not implemented in Python yet")
    return await _get_from_integrity()

async def delete_integrity_task(task_id: str):
    if not (settings.kasada.notion.enable and task_id):
        return
    url = f"{settings.kasada.notion.host}/api/v1/solution"
    params = {"apikey": settings.kasada.notion.apikey, "id": task_id}
    async with aiohttp.ClientSession() as s:
        await s.delete(url, params=params)

# Совместимость с viewer.py
class KasadaSolver:
    @staticmethod
    async def get_integrity(token: str = None, proxy: str = None):
        integ = await get_integrity()
        return {
            "x-kpsdk-ct": integ.x_kpsdk_ct,
            "x-kpsdk-cd": integ.x_kpsdk_cd,
            "x-kpsdk-v": getattr(integ, "x_kpsdk_v", ""),
            "user-agent": getattr(integ, "user_agent", ""),
            "User-Agent": getattr(integ, "user_agent", ""),  # чтобы точно попал в headers.update(...)
            "task_id": getattr(integ, "task_id", ""),
        }

    @staticmethod
    async def delete(task_id: str):
        await delete_integrity_task(task_id)
