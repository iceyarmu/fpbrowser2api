"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.database import Database
from ..core.logger import logger
from ..core.models import Task
from .task_executor import simulate_image_task, simulate_video_task


@dataclass
class PickedWindow:
    mapping_id: int
    window_pk: int
    task_code: str
    task_concurrency: int
    threshold: int
    timeout_seconds: int

    browser_vendor: str
    browser_base_url: str
    browser_access_key: Optional[str]
    space_id: str


class TaskService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._type_semaphores: dict[str, asyncio.Semaphore] = {}
        self._mapping_semaphores: dict[int, asyncio.Semaphore] = {}

    async def submit_task(self, task_type_code: str, prompt: str, image_path: Optional[str]) -> str:
        task_type_code = (task_type_code or "").strip()
        if not task_type_code:
            raise ValueError("task_type_code 不能为空")
        prompt = (prompt or "").strip()
        if not prompt:
            raise ValueError("prompt 不能为空")

        picked_raw = await self.db.pick_available_window(task_type_code)
        if not picked_raw:
            raise RuntimeError("没有可用窗口：请确认该任务类型已绑定窗口且额度>0、未冷却、已启用")

        picked = PickedWindow(
            mapping_id=int(picked_raw["id"]),
            window_pk=int(picked_raw["window_pk"]),
            task_code=str(picked_raw["task_code"]),
            task_concurrency=int(picked_raw.get("task_concurrency") or 1),
            threshold=int(picked_raw.get("continuous_error_threshold") or 3),
            timeout_seconds=int(picked_raw.get("timeout_seconds") or 1800),
            browser_vendor=str(picked_raw.get("vendor") or "generic"),
            browser_base_url=str(picked_raw.get("lan_addr") or ""),
            browser_access_key=picked_raw.get("access_key"),
            space_id=str(picked_raw.get("space_id") or ""),
        )

        task_id = uuid.uuid4().hex
        await self.db.create_task(
            Task(
                task_id=task_id,
                task_type_code=task_type_code,
                status="queued",
                progress=0,
                prompt=prompt,
                image_path=image_path,
                window_pk=picked.window_pk,
            )
        )

        asyncio.create_task(self._run_task(task_id, picked, prompt, image_path))
        return task_id

    def _get_type_sem(self, task_type_code: str, max_concurrency: int) -> asyncio.Semaphore:
        sem = self._type_semaphores.get(task_type_code)
        if sem is None:
            sem = asyncio.Semaphore(max(1, int(max_concurrency)))
            self._type_semaphores[task_type_code] = sem
        return sem

    def _get_mapping_sem(self, mapping_id: int) -> asyncio.Semaphore:
        sem = self._mapping_semaphores.get(mapping_id)
        if sem is None:
            # 窗口层不再配置并发：仍然固定为 1，避免同一绑定窗口被并行占用导致异常
            sem = asyncio.Semaphore(1)
            self._mapping_semaphores[mapping_id] = sem
        return sem

    async def _run_task(self, task_id: str, picked: PickedWindow, prompt: str, image_path: Optional[str]) -> None:
        type_sem = self._get_type_sem(picked.task_code, picked.task_concurrency)
        mapping_sem = self._get_mapping_sem(picked.mapping_id)

        async with type_sem:
            async with mapping_sem:
                await self.db.update_task(task_id, status="running", progress=1, set_started=True)
                logger.info("task started: %s type=%s window=%s mapping=%s", task_id, picked.task_code, picked.window_pk, picked.mapping_id)

                async def progress_cb(p: int, _payload: Optional[Dict[str, Any]]):
                    try:
                        await self.db.update_task(task_id, progress=int(p))
                    except Exception:
                        pass

                try:
                    if picked.task_code == "gen_image":
                        result = await asyncio.wait_for(simulate_image_task(prompt, image_path, progress_cb), timeout=float(picked.timeout_seconds))
                    elif picked.task_code == "gen_video":
                        result = await asyncio.wait_for(simulate_video_task(prompt, image_path, progress_cb), timeout=float(picked.timeout_seconds))
                    else:
                        # 其他类型先按通用模拟
                        result = await asyncio.wait_for(simulate_image_task(prompt, image_path, progress_cb), timeout=float(picked.timeout_seconds))

                    await self.db.update_task(task_id, status="completed", progress=100, result=result, set_completed=True)
                    await self.db.consume_mapping_quota(picked.mapping_id, amount=1)
                    await self.db.mark_mapping_success(picked.mapping_id)
                    logger.info("task completed: %s", task_id)
                except asyncio.TimeoutError:
                    await self.db.update_task(task_id, status="failed", error_message="任务超时", set_completed=True)
                    await self.db.mark_mapping_error(picked.mapping_id, threshold=picked.threshold, cooldown_seconds=1800)
                    logger.warning("task timeout: %s", task_id)
                except Exception as e:
                    await self.db.update_task(task_id, status="failed", error_message=str(e), set_completed=True)
                    await self.db.mark_mapping_error(picked.mapping_id, threshold=picked.threshold, cooldown_seconds=1800)
                    logger.exception("task failed: %s err=%s", task_id, e)

