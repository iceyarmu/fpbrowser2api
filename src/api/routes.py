"""Public API routes (task submit + task status)."""

from __future__ import annotations

import asyncio
import os
import time
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

# ---- High-concurrency controls (public endpoints) ----
# 创建任务接口并发闸门，避免峰值时打爆 DB/线程资源。
_CREATE_TASK_MAX_INFLIGHT = max(1, int(os.getenv("PUBLIC_CREATE_TASK_MAX_INFLIGHT", "180")))
_CREATE_TASK_ACQUIRE_TIMEOUT_SEC = max(0.1, float(os.getenv("PUBLIC_CREATE_TASK_ACQUIRE_TIMEOUT_SEC", "1.5")))
_create_task_semaphore: asyncio.Semaphore | None = None

# 高频查询缓存：轮询场景下减少重复读库。
_STATUS_CACHE_TTL_PENDING_SEC = max(0.05, float(os.getenv("TASK_STATUS_CACHE_TTL_PENDING_SEC", "2.0")))
_STATUS_CACHE_TTL_FINAL_SEC = max(1.0, float(os.getenv("TASK_STATUS_CACHE_TTL_FINAL_SEC", "20")))
_status_cache: dict[str, tuple[float, Dict[str, Any]]] = {}
_status_inflight: dict[str, asyncio.Future[Optional[Dict[str, Any]]]] = {}
_status_lock = asyncio.Lock()

# create_task 读前置配置的短缓存（降低热点读库）。
_SYSTEM_CONFIG_TTL_SEC = max(0.1, float(os.getenv("SYSTEM_CONFIG_CACHE_TTL_SEC", "5.0")))
_TASK_TYPE_TTL_SEC = max(0.1, float(os.getenv("TASK_TYPE_CACHE_TTL_SEC", "60.0")))
_system_config_cache: tuple[float, Optional[bool]] = (0.0, None)
_task_type_cache: dict[str, tuple[float, Any]] = {}


def set_dependencies(database: Database) -> None:
    global db, task_service, _create_task_semaphore
    db = database
    task_service = TaskService(database)
    _create_task_semaphore = asyncio.Semaphore(_CREATE_TASK_MAX_INFLIGHT)


class CreateTaskRequest(BaseModel):
    task_type_code: str = Field(min_length=2, max_length=64)
    json: Dict[str, Any] = Field(default_factory=dict)
    # 可选：指定执行窗口（仅用于调试/测试；不指定则走默认调度）
    mapping_id: Optional[int] = Field(default=None, ge=1)
    window_pk: Optional[int] = Field(default=None, ge=1)


async def _is_stop_accepting_tasks_cached() -> bool:
    if not db:
        return False
    now = time.monotonic()
    global _system_config_cache
    expire_at, cached_flag = _system_config_cache
    if now < expire_at and cached_flag is not None:
        return bool(cached_flag)
    syscfg = await db.get_system_config()
    flag = bool(getattr(syscfg, "stop_accepting_tasks", False))
    _system_config_cache = (now + _SYSTEM_CONFIG_TTL_SEC, flag)
    return flag


async def _get_task_type_by_code_cached(task_type_code: str):
    if not db:
        return None
    tcode = (task_type_code or "").strip()
    if not tcode:
        return None
    now = time.monotonic()
    row = _task_type_cache.get(tcode)
    if row and now < row[0]:
        return row[1]
    task_type = await db.get_task_type_by_code(tcode)
    _task_type_cache[tcode] = (now + _TASK_TYPE_TTL_SEC, task_type)
    return task_type


async def _get_cached_task_status_payload(task_id: str) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    async with _status_lock:
        row = _status_cache.get(task_id)
        if not row:
            return None
        expire_at, payload = row
        if now >= expire_at:
            _status_cache.pop(task_id, None)
            return None
        return payload


async def _set_task_status_cache(task_id: str, payload: Dict[str, Any]) -> None:
    status = str(payload.get("status") or "").lower()
    ttl = _STATUS_CACHE_TTL_FINAL_SEC if status in {"completed", "failed"} else _STATUS_CACHE_TTL_PENDING_SEC
    async with _status_lock:
        _status_cache[task_id] = (time.monotonic() + ttl, payload)


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
    if not db or not task_service or not _create_task_semaphore:
        raise HTTPException(status_code=500, detail="service not initialized")

    acquired = False
    try:
        try:
            await asyncio.wait_for(_create_task_semaphore.acquire(), timeout=_CREATE_TASK_ACQUIRE_TIMEOUT_SEC)
            acquired = True
        except asyncio.TimeoutError:
            raise HTTPException(status_code=429, detail="请求过于繁忙，请稍后重试")

        # 系统维护：停止接收新任务
        try:
            if await _is_stop_accepting_tasks_cached():
                raise HTTPException(status_code=503, detail="服务器稳定性&每日容量升级中，请稍后再试...")
        except HTTPException:
            raise
        except Exception:
            # 获取配置失败时不阻断（兜底）
            pass

        tcode = (body.task_type_code or "").strip()
        payload = body.json or {}
        task_type = await _get_task_type_by_code_cached(tcode)
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
        await _set_task_status_cache(
            tid,
            TaskStatusResponse(
                task_id=tid,
                status="queued",
                progress=0,
                result=None,
                error_message=None,
            ).model_dump(),
        )
        return {"success": True, "task_id": tid}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if acquired:
            _create_task_semaphore.release()


@router.get("/v1/tasks/{task_id}")
async def get_task_status(task_id: str, api_key: str = Depends(verify_api_key_header)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    tid = (task_id or "").strip()
    if not tid:
        raise HTTPException(status_code=400, detail="task_id 不能为空")

    cached = await _get_cached_task_status_payload(tid)
    if cached is not None:
        return JSONResponse(content=cached)

    # 单飞：同一 task_id 的并发查询仅触发一次 DB 读取。
    leader = False
    async with _status_lock:
        fut = _status_inflight.get(tid)
        if fut is None:
            fut = asyncio.get_running_loop().create_future()
            _status_inflight[tid] = fut
            leader = True

    if not leader:
        payload = await fut
        if payload is None:
            raise HTTPException(status_code=404, detail="task not found")
        return JSONResponse(content=payload)

    payload: Optional[Dict[str, Any]] = None
    try:
        task = await db.get_task(tid)
        if task:
            payload = TaskStatusResponse(
                task_id=task.task_id,
                status=task.status,
                progress=int(task.progress or 0),
                result=task.result,
                error_message=task.error_message,
            ).model_dump()
            await _set_task_status_cache(tid, payload)
        fut.set_result(payload)
    except Exception as e:
        fut.set_exception(e)
        raise
    finally:
        async with _status_lock:
            if _status_inflight.get(tid) is fut:
                _status_inflight.pop(tid, None)

    if payload is None:
        raise HTTPException(status_code=404, detail="task not found")
    return JSONResponse(content=payload)

