# manager/task_manager.py
import asyncio
import uuid
from typing import Dict, Optional, List

from core.task import TaskRunner
from models import Task, CreateTaskRequest

from core.proxy import load_proxies_raw, load_tokens_raw
from core.validators import validate_proxies, validate_tokens


class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, TaskRunner] = {}

    async def create(
            self,
            req: CreateTaskRequest,
            *,
            proxies: Optional[List[str]] = None,
            tokens: Optional[List[str]] = None,
    ) -> dict:
        try:
            task_id = str(uuid.uuid4())
            viewers = req.number_of_viewers

            # источники списков
            raw_proxies = (
                proxies
                if proxies is not None
                else (getattr(req, "proxies", None) or load_proxies_raw("proxies.txt"))
            )
            raw_tokens = (
                tokens
                if tokens is not None
                else (getattr(req, "tokens", None) or load_tokens_raw("tokens.txt"))
            )

            # валидация (validate_* уже возвращают готовые значения)
            valid_proxies: list[str] = []
            valid_tokens: list[str] = []

            if raw_proxies:
                valid_proxies = await validate_proxies(raw_proxies, concurrency=200, timeout=8)
            if raw_tokens:
                valid_tokens = await validate_tokens(raw_tokens, concurrency=200, timeout=8)

            # floating config
            flo = None
            if getattr(req, "floating_online", None) is not None:
                flo = req.floating_online.dict() if hasattr(req.floating_online, "dict") else req.floating_online

            runner = TaskRunner(
                id=task_id,
                channel=req.channel,
                viewers=viewers,
                proxies=valid_proxies,   # уже нормализованные URL
                tokens=valid_tokens,
                floating_config=flo if (flo and flo.get("enable")) else None,
                live_check=True,
                time_in_minutes=req.time_in_minutes,
                raid=getattr(req, "raid", None),
            )

            self.tasks[task_id] = runner
            asyncio.create_task(runner.start())

            return {"task": runner.to_task_model().dict(), "error": ""}

        except Exception as e:
            return {"task": None, "error": f"create failed: {e}"}

    async def delete(self, task_id: str):
        if task_id not in self.tasks:
            return False, f"task {task_id} not found"
        self.tasks[task_id].stop()
        del self.tasks[task_id]
        return True, ""

    def get_all(self) -> List[Task]:
        return [runner.to_task_model() for runner in self.tasks.values()]

    def get_by_id(self, task_id: str) -> Optional[Task]:
        r = self.tasks.get(task_id)
        if not r:
            raise ValueError(f"task {task_id} not found")
        return r.to_task_model()

    def pause(self, task_id: str):
        r = self.tasks.get(task_id)
        if not r:
            return False, f"task {task_id} not found"
        r.pause()
        return True, ""

    def resume(self, task_id: str):
        r = self.tasks.get(task_id)
        if not r:
            return False, f"task {task_id} not found"
        r.resume()
        return True, ""

    async def raid(self, task_id: str, target_channel: str, delay: int = 5):
        r = self.tasks.get(task_id)
        if not r:
            return False, f"task {task_id} not found"
        await r.raid(target_channel, delay)
        return True, ""

    async def stop_all(self):
        for r in list(self.tasks.values()):
            r.stop()
        self.tasks.clear()


# Глобальный менеджер
task_manager = TaskManager()
