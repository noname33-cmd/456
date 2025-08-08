# api/handlers.py
from fastapi import APIRouter, HTTPException
from services.viewer_service import viewer_service
from models import CreateTaskRequest

router = APIRouter(prefix="/api", tags=["Tasks"])

@router.get("")
async def get_all():
    try:
        res = viewer_service.get_all()
        return {"tasks": res.get("tasks", [])}
    except Exception as e:
        return {"tasks": [], "error": str(e)}

@router.get("/{task_id}")
async def get_by_id(task_id: str):
    res = viewer_service.get_by_id(task_id)
    if res.get("error"):
        raise HTTPException(status_code=404, detail=res["error"])
    return res

@router.post("")  # POST /api
async def create_task(req: CreateTaskRequest):
    res = await viewer_service.create(req)
    if res.get("error"):
        raise HTTPException(status_code=400, detail=res["error"])
    return res

@router.post("/{task_id}/pause")
async def pause_task(task_id: str):
    res = viewer_service.pause(task_id)
    if res.get("error"):
        raise HTTPException(status_code=400, detail=res["error"])
    return res

@router.post("/{task_id}/resume")
async def resume_task(task_id: str):
    res = viewer_service.resume(task_id)
    if res.get("error"):
        raise HTTPException(status_code=400, detail=res["error"])
    return res

@router.delete("/{task_id}")
async def delete_task(task_id: str):
    res = await viewer_service.delete(task_id)
    if res.get("error"):
        raise HTTPException(status_code=400, detail=res["error"])
    return res

@router.get("/kasada/check")
async def kasada_check():
    from core.kasada import diagnose
    try:
        diag = await diagnose()
        return diag
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))