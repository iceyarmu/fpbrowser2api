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
        self._mapping_semaphores: dict[int, asyncio.Semaphore] = {}
        # mapping_id -> max_concurrency（来自 task_types.concurrency；同一 mapping 可能随配置变更而变化）
        self._mapping_max_concurrency: dict[int, int] = {}
        # mapping_id -> 当前运行中的任务数（用于负载均衡挑选；与 semaphore 配合使用）
        self._mapping_inflight: dict[int, int] = {}
        self._mapping_inflight_lock = asyncio.Lock()
        # 仅内存保存 payload（不落库，节省 DB）
        self._task_payloads: dict[str, Dict[str, Any]] = {}
        # 任务预占的 semaphore（避免 _run_task 等待）
        self._task_reserved_sems: dict[str, asyncio.Semaphore] = {}

    async def submit_task(self, task_type_code: str, payload: Dict[str, Any]) -> str:
        task_type_code = (task_type_code or "").strip()
        if not task_type_code:
            raise ValueError("task_type_code 不能为空")
        payload = payload or {}

        picked_pack = await self._pick_window_balanced(task_type_code)
        if not picked_pack:
            raise RuntimeError("没有可用窗口：请确认该任务类型已绑定窗口且额度>0、未冷却、已启用2")
        picked, reserved_sem = picked_pack

        task_id = uuid.uuid4().hex
        try:
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
            self._task_reserved_sems[task_id] = reserved_sem
            asyncio.create_task(self._run_task(task_id, picked))
            return task_id
        except Exception:
            # 兜底：若创建任务失败，释放预占槽位避免泄漏
            await self._release_mapping_slot(picked.mapping_id, reserved_sem)
            raise

    def _get_mapping_sem(self, mapping_id: int, task_concurrency: int) -> asyncio.Semaphore:
        sem = self._mapping_semaphores.get(mapping_id)
        if sem is None:
            # mapping 级别并发：按 task_concurrency 控制
            max_c = max(1, int(task_concurrency))
            sem = asyncio.Semaphore(max_c)
            self._mapping_semaphores[mapping_id] = sem
            self._mapping_max_concurrency[mapping_id] = max_c
        else:
            # 若配置发生变更，避免运行中替换 semaphore 导致计数错乱；仅记录并打日志
            new_max_c = max(1, int(task_concurrency))
            old = int(self._mapping_max_concurrency.get(mapping_id) or 0)
            if old and old != new_max_c:
                logger.warning(
                    "mapping concurrency changed but semaphore kept (runtime): mapping=%s old=%s new=%s",
                    mapping_id,
                    old,
                    new_max_c,
                )
        return sem

    async def _reserve_mapping_slot(self, mapping_id: int, task_concurrency: int, timeout_seconds: float = 0.02) -> Optional[asyncio.Semaphore]:
        """尝试为某个 mapping 预占一个并发槽位。

        返回：
        - 成功：返回 semaphore（已 acquire）
        - 失败：返回 None（不会阻塞太久）
        """
        sem = self._get_mapping_sem(mapping_id, task_concurrency)
        if sem.locked():
            return None
        try:
            # 说明：这里用极短 wait_for 防止极端竞争导致 submit_task 卡住
            await asyncio.wait_for(sem.acquire(), timeout=float(timeout_seconds))
        except Exception:
            return None

        async with self._mapping_inflight_lock:
            self._mapping_inflight[mapping_id] = int(self._mapping_inflight.get(mapping_id) or 0) + 1
        return sem

    async def _release_mapping_slot(self, mapping_id: int, sem: asyncio.Semaphore) -> None:
        try:
            sem.release()
        except Exception:
            pass
        async with self._mapping_inflight_lock:
            cur = int(self._mapping_inflight.get(mapping_id) or 0)
            if cur <= 1:
                self._mapping_inflight.pop(mapping_id, None)
            else:
                self._mapping_inflight[mapping_id] = cur - 1

    async def _pick_window_balanced(self, task_type_code: str) -> Optional[tuple[PickedWindow, asyncio.Semaphore]]:
        """综合 DB 候选 + 当前并发占用，挑选未满载窗口并预占槽位（避免 _run_task 排队）。"""
        candidates = await self.db.list_available_windows_for_pick(task_type_code=task_type_code, limit=80)
        if not candidates:
            return None

        async with self._mapping_inflight_lock:
            inflight_snapshot = dict(self._mapping_inflight)

        scored: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []
        for r in candidates:
            mid = int(r["id"])
            max_c = max(1, int(r.get("task_concurrency") or 1))
            inflight = int(inflight_snapshot.get(mid) or 0)
            # 利用率：inflight/max_c，转成整数避免 float
            util = int(inflight * 1000 // max_c)
            consec = int(r.get("consecutive_errors") or 0)
            remain = int(r.get("remaining_quota") or 0)
            # util 越低越优；其次 inflight 越少越优；再按健康度与额度
            score = (util, inflight, consec, -remain, -mid)
            scored.append((score, r))

        scored.sort(key=lambda x: x[0])

        for _, r in scored:
            mid = int(r["id"])
            sem = await self._reserve_mapping_slot(mid, int(r.get("task_concurrency") or 1))
            if not sem:
                continue

            picked = PickedWindow(
                mapping_id=mid,
                window_pk=int(r["window_pk"]),
                window_key=str(r.get("window_key") or "").strip(),
                task_code=str(r["task_code"]),
                task_concurrency=int(r.get("task_concurrency") or 1),
                threshold=int(r.get("continuous_error_threshold") or 3),
                timeout_seconds=int(r.get("timeout_seconds") or 1800),
                create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
                browser_vendor=str(r.get("vendor") or "generic"),
                browser_base_url=str(r.get("lan_addr") or ""),
                browser_access_key=r.get("access_key"),
                space_id=str(r.get("space_id") or ""),
            )
            if not picked.window_key:
                await self._release_mapping_slot(mid, sem)
                continue
            return picked, sem

        return None

    async def _run_task(self, task_id: str, picked: PickedWindow) -> None:
        sem = self._task_reserved_sems.pop(task_id, None)
        if isinstance(sem, asyncio.Semaphore):
            mapping_sem = sem
            reserved = True
        else:
            mapping_sem = self._get_mapping_sem(picked.mapping_id, picked.task_concurrency)
            reserved = False

        if not reserved:
            await mapping_sem.acquire()
            async with self._mapping_inflight_lock:
                self._mapping_inflight[picked.mapping_id] = int(self._mapping_inflight.get(picked.mapping_id) or 0) + 1

        try:
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
                # Sora：若执行器返回了 nf_check，则用其回写余额/限流信息（覆盖本地扣减，更贴近真实剩余）
                try:
                    nf = (result or {}).get("nf_check") if isinstance(result, dict) else None
                    rate = (nf or {}) if isinstance(nf, dict) else None
                    if rate and rate.get("remaining_count") is not None:
                        await self.db.update_task_type_window(
                            mapping_id=picked.mapping_id,
                            remaining_quota=int(rate.get("remaining_count") or 0),
                            sora_remaining_count=int(rate.get("remaining_count") or 0),
                            sora_rate_limit_reached=bool(rate.get("rate_limit_reached", False)),
                            sora_access_resets_in_seconds=int(rate.get("access_resets_in_seconds") or 0),
                            cooldown_until=(str(rate.get("cooldown_until")) if rate.get("cooldown_until") else None),
                        )
                except Exception:
                    pass
                logger.info("task completed: %s", task_id)
            except asyncio.TimeoutError as t:
                await self.db.update_task(task_id, status="failed", error_message="任务超时"+str(t), set_completed=True)
                await self.db.mark_mapping_error(picked.mapping_id, threshold=picked.threshold, cooldown_seconds=1800)
                logger.warning("task timeout: %s", str(t))
            except Exception as e:
                await self.db.update_task(task_id, status="failed", error_message=str(e), set_completed=True)
                # 某些错误不应计入“窗口连续错误”（例如：Sora create 400 invalid_request、未抓到 POST 等环境/请求错误）
                # 执行器侧会抛出带 no_penalty=true 的异常（或同名属性），这里做兼容判断。
                if not bool(getattr(e, "no_penalty", False)):
                    await self.db.mark_mapping_error(picked.mapping_id, threshold=picked.threshold, cooldown_seconds=1800)
                logger.exception("task failed: %s err=%s", task_id, e)
            finally:
                # 清理内存 payload（避免堆积）
                self._task_payloads.pop(task_id, None)
                self._task_reserved_sems.pop(task_id, None)
        finally:
            await self._release_mapping_slot(picked.mapping_id, mapping_sem)

