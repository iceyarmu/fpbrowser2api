"""Public API routes (task submit + task status)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..core.auth import verify_api_key_header
from ..core.database import Database
from ..core.models import TaskStatusResponse
from ..services.task_service import TaskService
from ..services.task_handler_registry import CreateTaskContext, get_create_task_handler


router = APIRouter()

db: Database | None = None
task_service: TaskService | None = None


def set_dependencies(database: Database) -> None:
    global db, task_service
    db = database
    task_service = TaskService(database)


class CreateTaskRequest(BaseModel):
    task_type_code: str = Field(min_length=2, max_length=64)
    json: Dict[str, Any] = Field(default_factory=dict)
    # 可选：指定执行窗口（仅用于调试/测试；不指定则走默认调度）
    mapping_id: Optional[int] = Field(default=None, ge=1)
    window_pk: Optional[int] = Field(default=None, ge=1)


@router.get("/v1/task-types")
async def list_task_types(api_key: str = Depends(verify_api_key_header)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    items = await db.list_task_types()
    return {"success": True, "task_types": [t.model_dump() for t in items]}

@router.get("/v1/task-types-public")
async def list_task_types(api_key: str = Depends(verify_api_key_header)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    items = await db.list_task_types_public()
    return {"success": True, "task_types": [t.model_dump() for t in items]}


@router.post("/v1/tasks")
async def create_task(
    api_key: str = Depends(verify_api_key_header),
    body: CreateTaskRequest = Body(...),
):
    if not db or not task_service:
        raise HTTPException(status_code=500, detail="service not initialized")

    try:
        # 系统维护：停止接收新任务
        try:
            syscfg = await db.get_system_config()
            if bool(getattr(syscfg, "stop_accepting_tasks", False)):
                raise HTTPException(status_code=503, detail="服务器稳定性&每日容量升级中，请稍后再试...")
        except HTTPException:
            raise
        except Exception:
            # 获取配置失败时不阻断（兜底）
            pass

        tcode = (body.task_type_code or "").strip()
        payload = body.json or {}
        task_type = await db.get_task_type_by_code(tcode)
        if not task_type or task_type.deleted or not task_type.enabled:
            raise ValueError("task_type_code 不存在或未启用")

        try:
            handler = get_create_task_handler(task_type.create_task_handler)
        except KeyError as e:
            raise ValueError(str(e))
        tid = await handler(
            CreateTaskContext(
                task_type=task_type,
                payload=payload,
                mapping_id=body.mapping_id,
                window_pk=body.window_pk,
                db=db,
                task_service=task_service,
            )
        )
        return {"success": True, "task_id": tid}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/v1/tasks/{task_id}")
async def get_task_status(task_id: str, api_key: str = Depends(verify_api_key_header)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    payload = TaskStatusResponse(
        task_id=task.task_id,
        status=task.status,
        progress=int(task.progress or 0),
        result=task.result,
        error_message=task.error_message,
    )
    return JSONResponse(content=payload.model_dump())

