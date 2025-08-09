# services/viewer_service.py
from manager.task_manager import task_manager
from models import CreateTaskRequest

class ViewerService:
    async def create(self, req: CreateTaskRequest):
        return await task_manager.create(req)

    def get_all(self):
        tasks = task_manager.get_all()
        return {"tasks": [t.dict() for t in tasks], "error": ""}

    def get_by_id(self, task_id: str):
        try:
            t = task_manager.get_by_id(task_id)
            return {"task": t.dict(), "error": ""}
        except Exception as e:
            return {"task": None, "error": str(e)}

    def pause(self, task_id: str):
        ok, err = task_manager.pause(task_id)
        return {"message": "paused" if ok else "", "error": err}

    def resume(self, task_id: str):
        ok, err = task_manager.resume(task_id)
        return {"message": "resumed" if ok else "", "error": err}

    async def delete(self, task_id: str):
        ok, err = await task_manager.delete(task_id)
        return {"message": "deleted" if ok else "", "error": err}

viewer_service = ViewerService()
