"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.database import Database
from ..core.logger import logger
from ..core.models import Task
from .task_executor import simulate_image_task, simulate_video_task, sora_gen_video


@dataclass
class PickedWindow:
    mapping_id: int
    window_pk: int
    window_key: str
    task_code: str
    task_concurrency: int
    # 当前“可用窗口”总数（用于任务类型级别并发控制）
    available_window_total: int
    threshold: int
    timeout_seconds: int
    create_task_handler: Optional[str]

    browser_vendor: str
    browser_base_url: str
    browser_access_key: Optional[str]
    space_id: str


class TaskService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._type_semaphores: dict[str, asyncio.Semaphore] = {}
        self._mapping_semaphores: dict[int, asyncio.Semaphore] = {}
        # 仅内存保存 payload（不落库，节省 DB）
        self._task_payloads: dict[str, Dict[str, Any]] = {}

    async def submit_task(self, task_type_code: str, payload: Dict[str, Any]) -> str:
        task_type_code = (task_type_code or "").strip()
        if not task_type_code:
            raise ValueError("task_type_code 不能为空")
        payload = payload or {}

        print(f"count_available_windows: {task_type_code}")
        available_window_total = await self.db.count_available_windows(task_type_code)
        if available_window_total <= 0:
            raise RuntimeError("没有可用窗口：请确认该任务类型已绑定窗口且额度>0、未冷却、已启用1")

        picked_raw = await self.db.pick_available_window(task_type_code)
        if not picked_raw:
            raise RuntimeError("没有可用窗口：请确认该任务类型已绑定窗口且额度>0、未冷却、已启用2")

        picked = PickedWindow(
            mapping_id=int(picked_raw["id"]),
            window_pk=int(picked_raw["window_pk"]),
            window_key=str(picked_raw.get("window_key") or "").strip(),
            task_code=str(picked_raw["task_code"]),
            task_concurrency=int(picked_raw.get("task_concurrency") or 1),
            available_window_total=max(1, int(available_window_total or 0)),
            threshold=int(picked_raw.get("continuous_error_threshold") or 3),
            timeout_seconds=int(picked_raw.get("timeout_seconds") or 1800),
            create_task_handler=(str(picked_raw.get("create_task_handler") or "").strip() or None),
            browser_vendor=str(picked_raw.get("vendor") or "generic"),
            browser_base_url=str(picked_raw.get("lan_addr") or ""),
            browser_access_key=picked_raw.get("access_key"),
            space_id=str(picked_raw.get("space_id") or ""),
        )
        if not picked.window_key:
            raise RuntimeError("窗口缺少 window_key（指纹浏览器窗口标识），请先同步窗口或检查数据")

        task_id = uuid.uuid4().hex
        # 不把 payload 写入 DB；仅保存最小字段（prompt 存空字符串满足 NOT NULL）
        await self.db.create_task(
            Task(
                task_id=task_id,
                task_type_code=task_type_code,
                status="queued",
                progress=0,
                prompt="",
                image_path=None,
                window_pk=picked.window_pk,
            )
        )

        self._task_payloads[task_id] = payload
        asyncio.create_task(self._run_task(task_id, picked))
        return task_id

    def _get_type_sem(self, task_type_code: str, available_window_total: int) -> asyncio.Semaphore:
        sem = self._type_semaphores.get(task_type_code)
        if sem is None:
            # task_type 级别并发：按“可用窗口总数”控制，避免超过实际可用窗口数量
            sem = asyncio.Semaphore(max(1, int(available_window_total)))
            self._type_semaphores[task_type_code] = sem
        return sem

    def _get_mapping_sem(self, mapping_id: int, task_concurrency: int) -> asyncio.Semaphore:
        sem = self._mapping_semaphores.get(mapping_id)
        if sem is None:
            # mapping 级别并发：按 task_concurrency 控制
            sem = asyncio.Semaphore(max(1, int(task_concurrency)))
            self._mapping_semaphores[mapping_id] = sem
        return sem

    async def _run_task(self, task_id: str, picked: PickedWindow) -> None:
        type_sem = self._get_type_sem(picked.task_code, picked.available_window_total)
        mapping_sem = self._get_mapping_sem(picked.mapping_id, picked.task_concurrency)

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
                    payload = self._task_payloads.get(task_id) or {}
                    prompt = str(payload.get("prompt") or "").strip()

                    # 执行分发：优先按 task_type 配置的 create_task_handler 决定执行器
                    if picked.create_task_handler == "sora_gen_video":
                        result = await asyncio.wait_for(
                            sora_gen_video(
                                payload,
                                progress_cb,
                                browser_vendor=picked.browser_vendor,
                                browser_base_url=picked.browser_base_url,
                                browser_access_key=picked.browser_access_key,
                                space_id=picked.space_id,
                                window_key=picked.window_key,
                                timeout_seconds=float(picked.timeout_seconds),
                            ),
                            timeout=float(picked.timeout_seconds),
                        )
                    elif picked.task_code == "gen_video":
                        result = await asyncio.wait_for(simulate_video_task(prompt, None, progress_cb), timeout=float(picked.timeout_seconds))
                    else:
                        # 默认按图片模拟（包括 gen_image 以及其它未实现类型）
                        result = await asyncio.wait_for(simulate_image_task(prompt, None, progress_cb), timeout=float(picked.timeout_seconds))

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
                finally:
                    # 清理内存 payload（避免堆积）
                    self._task_payloads.pop(task_id, None)

