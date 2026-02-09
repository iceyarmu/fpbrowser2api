"""Public API routes (task submit + task status)."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Body
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


class CreateTaskJsonRequest(BaseModel):
    task_type_code: str = Field(min_length=2, max_length=64)
    prompt: str = Field(min_length=1, max_length=20000)
    image_base64: Optional[str] = None  # 允许 data:image/...;base64,xxx 或纯 base64


def _ensure_upload_dir() -> Path:
    p = Path(__file__).parent.parent.parent / "data" / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_bytes(filename_hint: str, data: bytes) -> str:
    upload_dir = _ensure_upload_dir()
    safe = "".join(c for c in (filename_hint or "upload.bin") if c.isalnum() or c in ("-", "_", ".", " "))
    safe = safe.strip().replace(" ", "_") or "upload.bin"
    path = upload_dir / f"{os.urandom(8).hex()}_{safe}"
    path.write_bytes(data)
    return str(path)


@router.get("/v1/task-types")
async def list_task_types(api_key: str = Depends(verify_api_key_header)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    items = await db.list_task_types()
    return {"success": True, "task_types": [t.model_dump() for t in items]}


@router.post("/v1/tasks")
async def create_task(
    api_key: str = Depends(verify_api_key_header),
    # multipart/form-data
    task_type_code: Optional[str] = Form(default=None),
    prompt: Optional[str] = Form(default=None),
    image: Optional[UploadFile] = File(default=None),
    # json fallback
    json_body: Optional[CreateTaskJsonRequest] = Body(default=None),
):
    if not db or not task_service:
        raise HTTPException(status_code=500, detail="service not initialized")

    image_path: Optional[str] = None

    if json_body is not None and (task_type_code is None and prompt is None):
        task_type_code = json_body.task_type_code
        prompt = json_body.prompt
        if json_body.image_base64:
            b64 = json_body.image_base64
            if "base64," in b64:
                b64 = b64.split("base64,", 1)[1]
            try:
                data = base64.b64decode(b64)
                image_path = _save_bytes("image.png", data)
            except Exception:
                raise HTTPException(status_code=400, detail="image_base64 解析失败")

    if image is not None:
        data = await image.read()
        if data:
            image_path = _save_bytes(image.filename or "image.bin", data)

    try:
        tcode = (task_type_code or "").strip()
        p = (prompt or "").strip()
        task_type = await db.get_task_type_by_code(tcode)
        if not task_type or task_type.deleted or not task_type.enabled:
            raise ValueError("task_type_code 不存在或未启用")

        try:
            handler = get_create_task_handler(task_type.create_task_handler)
        except KeyError as e:
            raise ValueError(str(e))
        tid = await handler(CreateTaskContext(task_type=task_type, prompt=p, image_path=image_path, db=db, task_service=task_service))
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

