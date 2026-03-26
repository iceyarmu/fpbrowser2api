"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.database import Database
from ..core.logger import logger
from ..core.models import Task
from ..core.public_api_limits import DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT, calc_public_browser_pool_limit
from .image_task_executor import simulate_image_task
from .video_task_executor import simulate_video_task
from .sora_task_executor import get_or_create_sora_session, sora_fetch_access_token_in_window, sora_gen_video
from .sora_wm_remove_executor import sora_wm_remove
from .sora_plus_register_executor import sora_plus_register
from .veo_workflow_executor import (
    _veo_resolve_n_frames,
    get_or_create_veo_session,
    veo_fetch_credits_in_window,
    veo_workflow,
)


@dataclass
class PickedWindow:
    mapping_id: int
    window_pk: int
    window_key: str
    task_code: str
    task_concurrency: int
    threshold: int
    close_window_threshold: int
    timeout_seconds: int
    create_task_handler: Optional[str]
    browser_vendor: str
    browser_base_url: str
    browser_access_key: Optional[str]
    space_id: str
    sora_access_token: Optional[str] = None
    sora_access_expires: Optional[str] = None
    default_target_url: Optional[str] = None
    window_ip: Optional[str] = None
    headless: bool = False
    error_retry_count: int = 0


@dataclass
class QueuedTask:
    task_id: str
    task_type_code: str
    payload: Dict[str, Any]
    enqueued_at: float
    retry_attempt: int = 0
    required_window_pk: Optional[int] = None
    is_dedicated_window: bool = False


def _remaining_quota_exclusive_floor_for_pick(
    task_type_code: str, payload: Optional[Dict[str, Any]]
) -> int:
    """与 pick 时 remaining_quota > floor 及预扣额度对齐（见 _consume_quota_after_window_pick）。"""
    code = (task_type_code or "").strip()
    if code == "sora_gen_video":
        return 2
    if code == "veo_workflow":
        return 19 if _veo_resolve_n_frames(payload or {}) > 1 else 0
    return 2


class TaskService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._browser_pool_limit: int = calc_public_browser_pool_limit(DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT)
        # 任务 payload 仍保留一份内存副本供执行器使用；DB 侧仅保存一个“可查看/可检索”的 prompt 字符串
        self._task_payloads: dict[str, Dict[str, Any]] = {}
        # 1) payload["prompt"] 本身的长度上限（便于查看，也避免超长文本撑爆 DB）
        self._payload_prompt_max_chars: int = 1000
        # 2) 最终落库到 tasks.prompt 的总长度上限（兼容某些历史/自定义 schema 的较短字段）
        self._prompt_max_chars: int = 2000

        # ---- 专用窗口并发控制（generation_id + head_url 类任务） ----
        self._dedicated_window_inflight: int = 0
        self._dedicated_window_lock = asyncio.Lock()
        self._browser_open_concurrency: int = 3

        # ---- 排队机制：窗口满载时入队等待，窗口释放时自动派发 ----
        self._pending_queue: deque[QueuedTask] = deque()
        self._queue_lock = asyncio.Lock()
        self._dispatch_event = asyncio.Event()
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._queue_max_size: int = 1000
        self._queue_timeout_seconds: float = 300.0
        self._dispatch_poll_interval: float = 5.0
        # 从 DB 缓存读取排队配置（避免频繁读库）
        self._queue_config_cache: tuple[float, int, float] = (0.0, 1000, 300.0)
        self._queue_config_ttl: float = 30.0

    def set_browser_pool_limit(self, limit: int) -> None:
        """Hot-update scheduling candidate pool size."""
        try:
            self._browser_pool_limit = max(1, int(limit))
        except Exception:
            pass

    def _truncate_text(self, s: str, max_chars: int, *, label: str) -> str:
        s = str(s or "")
        max_chars = int(max_chars or 0)
        if max_chars <= 0:
            return ""
        if len(s) <= max_chars:
            return s
        suffix = f"…({label} truncated, orig_chars={len(s)}, max_chars={max_chars})"
        keep = max(0, max_chars - len(suffix))
        if keep <= 0:
            return suffix[:max_chars]
        return s[:keep] + suffix

    @staticmethod
    def _task_created_at_for_sql(v: Any) -> Optional[str]:
        """将任务行的 created_at 转为 SQLite 可接受的本地时间字符串（用于 INSERT 覆盖）。"""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        s = str(v).strip()
        return s or None

    def _payload_to_prompt_text(self, payload: Dict[str, Any]) -> str:
        """把 payload 序列化成可落库的 prompt 文本（尽量是 JSON，且控制长度）。"""

        def _dumps(obj: Any) -> str:
            return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

        total_max = max(64, int(self._prompt_max_chars or 0))
        prompt_max = max(0, int(self._payload_prompt_max_chars or 0))

        base_payload: Dict[str, Any]
        if isinstance(payload, dict):
            base_payload = dict(payload or {})
        else:
            base_payload = {"payload": payload}

        # 先对 payload["prompt"] 做“字段级”限长（<=1000）
        orig_prompt = str(base_payload.get("prompt") or "")
        if "prompt" in base_payload or orig_prompt:
            base_payload["prompt"] = self._truncate_text(orig_prompt, prompt_max, label="prompt")

        try:
            s = _dumps(base_payload)
        except Exception:
            # 极端兜底：保证永远能落库
            s = self._truncate_text(str(payload or {}), total_max, label="payload")

        if len(s) <= total_max:
            return s

        # 若整段 JSON 仍超长：降级为最小可查看 JSON（保证总长度 <= 2000 且尽量保持可解析）
        minimal_flag_key = "_payload_trimmed"
        prompt_text = str(base_payload.get("prompt") or "")

        def _minimal_json(prompt_val: str) -> str:
            return _dumps({"prompt": prompt_val, minimal_flag_key: True})

        # 二分裁剪 prompt（在不超过字段级上限的前提下），直到 minimal JSON 满足 total_max
        hi = len(prompt_text)
        lo = 0
        best = ""
        while lo <= hi:
            mid = (lo + hi) // 2
            cand_prompt = self._truncate_text(prompt_text, mid, label="prompt_db")
            cand = _minimal_json(cand_prompt)
            if len(cand) <= total_max:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1

        if best:
            return best

        # 最后兜底：即使 prompt 为空也要可落库
        empty = _minimal_json("")
        if len(empty) <= total_max:
            return empty
        return empty[:total_max]

    async def submit_task(
        self,
        task_type_code: str,
        payload: Dict[str, Any],
        *,
        mapping_id: Optional[int] = None,
        window_pk: Optional[int] = None,
    ) -> str:
        task_type_code = (task_type_code or "").strip()
        if not task_type_code:
            raise ValueError("task_type_code 不能为空")
        payload = payload or {}

        # Sora 角色创建分支：payload.generation_id + payload.head_url
        # 需求：若能走该分支，则优先复用 generation_id 对应历史任务的窗口
        payload_generation_id = str(payload.get("generation_id") or "").strip() or None
        payload_head_url = str(payload.get("head_url") or "").strip() or None

        picked: Optional[PickedWindow] = None
        _is_dedicated_window = False
        # 指定窗口优先级：mapping_id > window_pk > 默认自动挑选
        if mapping_id is not None:
            picked = await self._pick_window_by_mapping(
                task_type_code, mapping_id=int(mapping_id), payload=payload
            )
        elif window_pk is not None:
            picked = await self._pick_window_by_window_pk(
                task_type_code, window_pk=int(window_pk), payload=payload
            )
        else:
            # 若 payload 满足“基于 generation_id 创建角色”分支，则尝试按 generation_id 绑定窗口
            if payload_generation_id and payload_head_url:
                try:
                    win_pk = await self.db.get_task_window_pk_by_generation_id(payload_generation_id)
                except Exception:
                    win_pk = None

                if win_pk is None:
                    raise RuntimeError("该视频不属于我们的账号，请先生成视频再使用返回的generation_id创建角色")

                # 并发控制：专用窗口任务受 browser_open_concurrency 限制
                await self._refresh_queue_config()
                _over_limit = False
                async with self._dedicated_window_lock:
                    if self._dedicated_window_inflight >= self._browser_open_concurrency:
                        _over_limit = True
                    else:
                        self._dedicated_window_inflight += 1
                if _over_limit:
                    return await self._enqueue_task(
                        task_type_code, payload,
                        required_window_pk=win_pk,
                        is_dedicated_window=True,
                    )
                _is_dedicated_window = True

                picked = await self._pick_window_by_window_pk(task_type_code, win_pk, payload=payload)
                if not picked:
                    async with self._dedicated_window_lock:
                        self._dedicated_window_inflight = max(0, self._dedicated_window_inflight - 1)
                    raise RuntimeError("该视频不属于我们的账号，请先生成视频再使用返回的generation_id创建角色")
            if not picked:
                picked = await self._pick_window(task_type_code, payload=payload)
        if not picked:
            if mapping_id is not None or window_pk is not None:
                raise RuntimeError("指定窗口不可用：请确认该窗口已绑定该任务类型、未删除、已启用")
            return await self._enqueue_task(task_type_code, payload)

        task_id = uuid.uuid4().hex
        try:
            # 把 payload 序列化落库到 prompt 里，便于管理台查看/检索（控制长度，避免字段溢出）
            prompt_text = self._payload_to_prompt_text(payload)
            await self.db.create_task(
                Task(
                    task_id=task_id,
                    task_type_code=task_type_code,
                    generation_id=None,
                    status="queued",
                    progress=0,
                    prompt=prompt_text,
                    image_path=None,
                    window_pk=picked.window_pk,
                    window_ip=picked.window_ip,
                )
            )
            self._task_payloads[task_id] = payload
            asyncio.create_task(self._run_task(task_id, picked, _is_dedicated_window=_is_dedicated_window))
            return task_id
        except Exception:
            if _is_dedicated_window:
                async with self._dedicated_window_lock:
                    self._dedicated_window_inflight = max(0, self._dedicated_window_inflight - 1)
            # 兜底：若创建任务失败，释放预占槽位避免泄漏
            try:
                await self.db.release_mapping_slot(picked.mapping_id)
            except Exception:
                pass
            raise

    async def _consume_quota_after_window_pick(
        self, picked: PickedWindow, payload: Optional[Dict[str, Any]] = None
    ) -> None:
        """挑选窗口成功后按 handler 预扣 mapping 额度（与真实消耗对齐）。"""
        handler = (picked.create_task_handler or "").strip()
        if handler == "sora_gen_video":
            try:
                await self.db.consume_mapping_quota(picked.mapping_id, amount=2)
            except Exception:
                pass
        elif handler == "veo_workflow":
            try:
                if _veo_resolve_n_frames(payload or {}) > 1:
                    await self.db.consume_mapping_quota(picked.mapping_id, amount=20)
            except Exception:
                pass

    async def _pick_window(self, task_type_code: str, payload: Optional[Dict[str, Any]] = None) -> Optional[PickedWindow]:
        """从 DB 候选中挑选窗口，并在 DB 中原子预占并发槽位。

        说明：
        - 预占由 DB 字段 inflight_slots 完成（支持多进程/多实例，避免超卖）
        - 挑选排序由 DB 决定（consecutive_errors 最低优先，其次 remaining_quota 最少优先）
        """
        r = await self.db.pick_and_reserve_window_for_task(
            task_type_code=task_type_code,
            browser_pool_limit=self._browser_pool_limit,
            remaining_quota_exclusive_floor=_remaining_quota_exclusive_floor_for_pick(
                task_type_code, payload
            ),
        )
        if not r:
            return None

        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
            default_target_url=(str(r.get("default_target_url") or "").strip() or None),
            headless=bool(r.get("headless")),
            error_retry_count=int(r.get("error_retry_count") or 0),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        return picked

    async def _pick_window_by_mapping(
        self, task_type_code: str, mapping_id: int, payload: Optional[Dict[str, Any]] = None
    ) -> Optional[PickedWindow]:
        """指定 mapping_id（task_type_windows.id）预占并发槽位并返回窗口上下文。"""
        # 显式指定窗口：不按“额度/冷却/熔断/并发上限”等资源约束拒绝，直接选中该窗口
        r = await self.db.force_reserve_mapping_for_task(task_type_code=task_type_code, mapping_id=int(mapping_id))
        if not r:
            return None
        # 复用字段解析逻辑：与 _pick_window 保持一致
        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
            default_target_url=(str(r.get("default_target_url") or "").strip() or None),
            headless=bool(r.get("headless")),
            error_retry_count=int(r.get("error_retry_count") or 0),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        return picked

    async def _pick_window_by_window_pk(
        self, task_type_code: str, window_pk: int, payload: Optional[Dict[str, Any]] = None
    ) -> Optional[PickedWindow]:
        """指定 window_pk 预占并发槽位并返回窗口上下文。"""
        # 显式指定窗口：不按“额度/冷却/熔断/并发上限”等资源约束拒绝，直接选中该窗口
        r = await self.db.force_reserve_window_for_task(task_type_code=task_type_code, window_pk=int(window_pk))
        if not r:
            return None
        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
            default_target_url=(str(r.get("default_target_url") or "").strip() or None),
            headless=bool(r.get("headless")),
            error_retry_count=int(r.get("error_retry_count") or 0),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        return picked

    # ---- 排队与调度 ----

    def _ensure_dispatcher(self) -> None:
        if self._dispatcher_task is None or self._dispatcher_task.done():
            self._dispatcher_task = asyncio.create_task(self._dispatcher_loop())

    async def _refresh_queue_config(self) -> None:
        now = time.monotonic()
        expire_at, _, _ = self._queue_config_cache
        if now < expire_at:
            return
        try:
            syscfg = await self.db.get_system_config()
            max_size = max(1, int(getattr(syscfg, "task_queue_max_size", 0) or 1000))
            timeout = max(10.0, float(getattr(syscfg, "task_queue_timeout_seconds", 0) or 300.0))
            browser_open_concurrency = max(1, int(getattr(syscfg, "browser_open_concurrency", 0) or 3))
        except Exception:
            max_size, timeout = self._queue_max_size, self._queue_timeout_seconds
            browser_open_concurrency = self._browser_open_concurrency
        self._queue_config_cache = (now + self._queue_config_ttl, max_size, timeout)
        self._queue_max_size = max_size
        self._queue_timeout_seconds = timeout
        self._browser_open_concurrency = browser_open_concurrency

    async def _enqueue_task(
        self,
        task_type_code: str,
        payload: Dict[str, Any],
        *,
        required_window_pk: Optional[int] = None,
        is_dedicated_window: bool = False,
    ) -> str:
        self._ensure_dispatcher()
        await self._refresh_queue_config()

        if len(self._pending_queue) >= self._queue_max_size:
            raise RuntimeError("任务队列已满，请稍后重试")

        task_id = uuid.uuid4().hex
        prompt_text = self._payload_to_prompt_text(payload)
        await self.db.create_task(
            Task(
                task_id=task_id,
                task_type_code=task_type_code,
                generation_id=None,
                status="queued",
                progress=0,
                prompt=prompt_text,
                image_path=None,
                window_pk=None,
                window_ip=None,
            )
        )
        self._task_payloads[task_id] = payload

        async with self._queue_lock:
            self._pending_queue.append(
                QueuedTask(
                    task_id=task_id,
                    task_type_code=task_type_code,
                    payload=payload,
                    enqueued_at=time.monotonic(),
                    required_window_pk=required_window_pk,
                    is_dedicated_window=is_dedicated_window,
                )
            )
        self._dispatch_event.set()
        logger.info(
            "task queued: %s type=%s queue_size=%d",
            task_id,
            task_type_code,
            len(self._pending_queue),
        )
        return task_id

    async def _dispatcher_loop(self) -> None:
        while True:
            try:
                try:
                    await asyncio.wait_for(
                        self._dispatch_event.wait(),
                        timeout=self._dispatch_poll_interval,
                    )
                except asyncio.TimeoutError:
                    pass
                self._dispatch_event.clear()

                if not self._pending_queue:
                    continue

                await self._refresh_queue_config()
                await self._try_dispatch_all()
            except Exception as e:
                logger.exception("dispatcher_loop error: %s", e)
                await asyncio.sleep(1.0)

    async def _try_dispatch_all(self) -> None:
        async with self._queue_lock:
            still_pending: deque[QueuedTask] = deque()
            exhausted_types: set[str] = set()
            now = time.monotonic()

            while self._pending_queue:
                item = self._pending_queue.popleft()

                if now - item.enqueued_at > self._queue_timeout_seconds:
                    try:
                        await self.db.update_task(
                            item.task_id,
                            status="failed",
                            error_message="排队超时，请稍后重试",
                            set_completed=True,
                        )
                    except Exception:
                        pass
                    self._task_payloads.pop(item.task_id, None)
                    logger.warning("task queue timeout: %s", item.task_id)
                    continue

                if item.task_type_code in exhausted_types and item.required_window_pk is None:
                    still_pending.append(item)
                    continue

                # 专用窗口任务：先检查并发限制
                _dedicated_acquired = False
                if item.is_dedicated_window:
                    async with self._dedicated_window_lock:
                        if self._dedicated_window_inflight >= self._browser_open_concurrency:
                            still_pending.append(item)
                            continue
                        self._dedicated_window_inflight += 1
                        _dedicated_acquired = True

                if item.required_window_pk is not None:
                    picked = await self._pick_window_by_window_pk(
                        item.task_type_code, item.required_window_pk, payload=item.payload
                    )
                else:
                    picked = await self._pick_window(item.task_type_code, payload=item.payload)
                if picked:
                    try:
                        await self.db.update_task(
                            item.task_id,
                            window_pk=picked.window_pk,
                            window_ip=picked.window_ip,
                        )
                    except Exception:
                        pass
                    asyncio.create_task(self._run_task(
                        item.task_id, picked,
                        _retry_attempt=item.retry_attempt,
                        _is_dedicated_window=item.is_dedicated_window,
                    ))
                    logger.info(
                        "task dispatched from queue: %s type=%s window=%s retry=%d (waited %.1fs)",
                        item.task_id,
                        item.task_type_code,
                        picked.window_pk,
                        item.retry_attempt,
                        now - item.enqueued_at,
                    )
                else:
                    if _dedicated_acquired:
                        async with self._dedicated_window_lock:
                            self._dedicated_window_inflight = max(0, self._dedicated_window_inflight - 1)
                    exhausted_types.add(item.task_type_code)
                    still_pending.append(item)

            self._pending_queue = still_pending

    async def get_queue_info(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "queue_size": len(self._pending_queue),
            "queue_max_size": self._queue_max_size,
            "queue_timeout_seconds": self._queue_timeout_seconds,
            "dispatcher_running": self._dispatcher_task is not None and not self._dispatcher_task.done(),
        }
        try:
            info["task_stats"] = await self.db.task_status_summary()
        except Exception:
            info["task_stats"] = {}
        return info

    async def _run_task(self, task_id: str, picked: PickedWindow, *, _retry_attempt: int = 0, _is_dedicated_window: bool = False) -> None:
        _need_retry = False
        _retry_error_msg = ""
        try:
            await self.db.update_task(task_id, status="running", progress=0, set_started=True)
            logger.info("task started: %s type=%s window=%s mapping=%s attempt=%d", task_id, picked.task_code, picked.window_pk, picked.mapping_id, _retry_attempt)

            _last_saved_progress = -1

            async def progress_cb(p: int, _payload: Optional[Dict[str, Any]]):
                nonlocal _last_saved_progress
                pi = int(p)
                if pi == _last_saved_progress:
                    return
                # 只在关键节点或变化 >=5 时写库，大幅减少写频率
                if pi not in (0, 100) and abs(pi - _last_saved_progress) < 5:
                    return
                try:
                    await self.db.update_task(task_id, progress=pi)
                    _last_saved_progress = pi
                except Exception:
                    pass

            payload = self._task_payloads.get(task_id) or {}
            prompt = str(payload.get("prompt") or "").strip()
            target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
            try:
                refresh_timeout_seconds = max(1.0, float(payload.get("sora_balance_refresh_timeout_seconds") or 60.0))
            except Exception:
                refresh_timeout_seconds = 60.0

            async def _refresh_sora_balance() -> Optional[Dict[str, Any]]:
                handler = str(picked.create_task_handler or "").strip().lower()
                if not handler.startswith("sora_gen_video"):
                    return None

                try:
                    sess = get_or_create_sora_session(
                        vendor=picked.browser_vendor,
                        base_url=picked.browser_base_url,
                        access_key=picked.browser_access_key,
                        space_id=picked.space_id,
                        window_key=picked.window_key,
                    )
                    sess.browser_headless = picked.headless
                except Exception:
                    return None

                nf_check: Optional[Dict[str, Any]] = None
                nf_check_err: Optional[Exception] = None
                try:
                    checked = await sess.api_nf_check(target_url=target_url)
                    nf_check = checked if isinstance(checked, dict) else None
                except Exception as e:
                    nf_check_err = e
                    nf_check = None

                try:
                    if nf_check and nf_check.get("remaining_count") is not None:
                        await self.db.update_task_type_window(
                            mapping_id=picked.mapping_id,
                            remaining_quota=int(nf_check.get("remaining_count") or 0),
                            sora_remaining_count=int(nf_check.get("remaining_count") or 0),
                            sora_purchased_remaining_count=int(nf_check.get("purchased_remaining_count") or 0),
                            sora_rate_limit_reached=bool(nf_check.get("rate_limit_reached", False)),
                            sora_access_resets_in_seconds=int(nf_check.get("access_resets_in_seconds") or 0),
                            cooldown_until=(str(nf_check.get("cooldown_until")) if nf_check.get("cooldown_until") else None),
                        )
                except Exception:
                    pass

                # 当 remaining_count=0 时，参考 admin 接口逻辑尝试在窗口内刷新一次 access_token
                try:
                    if nf_check is None or (nf_check and int(nf_check.get("remaining_count") or 0) == 0):
                        info = await sora_fetch_access_token_in_window(sess=sess, target_url=target_url)
                        access_token = str((info or {}).get("access_token") or "").strip() or None
                        expires = str((info or {}).get("expires") or "").strip() or None
                        if access_token:
                            await self.db.update_task_type_window(
                                mapping_id=picked.mapping_id,
                                sora_access_token=access_token,
                                sora_access_expires=expires,
                            )
                            picked.sora_access_token = access_token
                            picked.sora_access_expires = expires
                            try:
                                sess.set_access_token(access_token, expires)
                            except Exception:
                                pass

                            try:
                                checked = await sess.api_nf_check(target_url=target_url)
                                nf_check = checked if isinstance(checked, dict) else None
                            except Exception as e:
                                nf_check_err = e
                                nf_check = None

                            try:
                                if nf_check and nf_check.get("remaining_count") is not None:
                                    await self.db.update_task_type_window(
                                        mapping_id=picked.mapping_id,
                                        remaining_quota=int(nf_check.get("remaining_count") or 0),
                                        sora_remaining_count=int(nf_check.get("remaining_count") or 0),
                                        sora_purchased_remaining_count=int(nf_check.get("purchased_remaining_count") or 0),
                                        sora_rate_limit_reached=bool(nf_check.get("rate_limit_reached", False)),
                                        sora_access_resets_in_seconds=int(nf_check.get("access_resets_in_seconds") or 0),
                                        cooldown_until=(str(nf_check.get("cooldown_until")) if nf_check.get("cooldown_until") else None),
                                    )
                            except Exception:
                                pass

                except Exception as e:
                    print("refresh access token error", e)
                    pass

                # 余额低/查询异常时倾向于回收会话，余额充足时保持会话热态
                try:
                    if nf_check_err is not None:
                        print("余额更新失败:", nf_check_err)
                        sess._schedule_idle_close()
                    else:
                        remaining = int((nf_check or {}).get("remaining_count") or 0)
                        if remaining <= 2:
                            sess._schedule_idle_close()
                        else:
                            sess._cancel_idle_close()
                except Exception:
                    pass

                return nf_check

            async def _refresh_sora_balance_best_effort() -> None:
                """余额刷新只做尽力而为，不能影响任务终态写回。"""
                try:
                    await asyncio.wait_for(_refresh_sora_balance(), timeout=refresh_timeout_seconds)
                except Exception as e:
                    logger.warning(
                        "refresh_sora_balance skipped: task=%s mapping=%s err=%s",
                        task_id,
                        picked.mapping_id,
                        e,
                    )

            async def _refresh_veo_balance() -> Optional[Dict[str, Any]]:
                """VEO：在指纹窗口内 fetch aisandbox credits，与 admin 刷新额度一致。"""
                if str(picked.create_task_handler or "").strip().lower() != "veo_workflow":
                    return None

                at = str(picked.sora_access_token or "").strip()
                if not at:
                    return None

                try:
                    veo_sess = get_or_create_veo_session(
                        vendor=picked.browser_vendor,
                        base_url=picked.browser_base_url,
                        access_key=picked.browser_access_key,
                        space_id=picked.space_id,
                        window_key=picked.window_key,
                    )
                    veo_sess.browser_headless = picked.headless
                except Exception:
                    return None

                veo_target = str(picked.default_target_url or "").strip() or "https://labs.google/fx"
                veo_info: Optional[Dict[str, Any]] = None
                try:
                    veo_info = await veo_fetch_credits_in_window(
                        sess=veo_sess,
                        target_url=veo_target,
                        access_token=at,
                    )
                except Exception as e:
                    logger.warning(
                        "refresh_veo_balance failed: task=%s mapping=%s err=%s",
                        task_id,
                        picked.mapping_id,
                        e,
                    )
                    return None

                try:
                    if veo_info is not None and veo_info.get("credits") is not None:
                        _kw: Dict[str, Any] = {
                            "mapping_id": picked.mapping_id,
                            "remaining_quota": int(veo_info.get("credits") or 0),
                            "sora_remaining_count": int(veo_info.get("credits") or 0),
                        }
                        _cu = veo_info.get("cooldown_until")
                        if _cu:
                            _kw["cooldown_until"] = str(_cu)
                        await self.db.update_task_type_window(**_kw)
                except Exception:
                    pass
                return veo_info

            async def _refresh_veo_balance_best_effort() -> None:
                try:
                    await asyncio.wait_for(_refresh_veo_balance(), timeout=refresh_timeout_seconds)
                except Exception as e:
                    logger.warning(
                        "refresh_veo_balance skipped: task=%s mapping=%s err=%s",
                        task_id,
                        picked.mapping_id,
                        e,
                    )

            try:
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
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                            headless=picked.headless,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.create_task_handler == "veo_workflow":
                    veo_payload = dict(payload or {})
                    if picked.default_target_url and not str(
                        veo_payload.get("veo_url") or veo_payload.get("target_url") or ""
                    ).strip():
                        veo_payload["veo_url"] = picked.default_target_url
                    result = await asyncio.wait_for(
                        veo_workflow(
                            veo_payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                            headless=picked.headless,
                            db=self.db,
                            task_type_window_id=picked.mapping_id,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.create_task_handler == "sora_wm_remove":
                    result = await asyncio.wait_for(
                        sora_wm_remove(
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
                elif picked.create_task_handler == "sora_plus_register":
                    result = await asyncio.wait_for(
                        sora_plus_register(
                            payload,
                            progress_cb,
                            db=self.db,
                            window_pk=picked.window_pk,
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

                # Sora：单独把 generation_id 落库（用于后续按 generation_id 绑定窗口）
                try:
                    if isinstance(result, dict):
                        gid = str(result.get("generation_id") or "").strip() or None
                        if gid:
                            await self.db.update_task(task_id, generation_id=gid)
                except Exception:
                    pass

                if picked.create_task_handler == "veo_workflow":
                    await _refresh_veo_balance_best_effort()
                elif picked.create_task_handler == "sora_gen_video":
                    await _refresh_sora_balance_best_effort()
                try:
                    if isinstance(result, dict) and result.get("drafts_count") is not None:
                        await self.db.update_task_type_window(
                            mapping_id=picked.mapping_id,
                            sora_drafts_count=int(result.get("drafts_count") or 0),
                        )
                except Exception:
                    pass
                # 清空一下result中的nf_check，避免敏感信息泄露
                if isinstance(result, dict):
                    result["nf_check"] = None
                await self.db.update_task(task_id, status="completed", progress=100, result=result, set_completed=True)
                #await self.db.consume_mapping_quota(picked.mapping_id, amount=1)
                await self.db.mark_mapping_success(picked.mapping_id)
                logger.info("task completed: %s", task_id)
            except Exception as e:
                if picked.create_task_handler == "veo_workflow":
                    await _refresh_veo_balance_best_effort()
                elif picked.create_task_handler == "sora_gen_video":
                    await _refresh_sora_balance_best_effort()
                # 失败：尽量把“是否不扣罚(no_penalty)”等信息写入 result_json，便于上游做退款/分类。
                no_penalty = bool(getattr(e, "no_penalty", False))
                status_code = getattr(e, "status_code", None)
                err_result: Dict[str, Any] = {
                    "error_type": e.__class__.__name__,
                    "no_penalty": no_penalty,
                }
                if status_code is not None:
                    try:
                        err_result["status_code"] = int(status_code)
                    except Exception:
                        err_result["status_code"] = str(status_code)
                _err_lower = str(e).lower()
                _is_violation = int(
                    "sora_content_violation" in _err_lower
                    or "cameo_not_found" in _err_lower
                    or "cameo_permission_denied" in _err_lower
                    or "包含违禁画面" in str(e)
                    or "包含违规内容" in str(e)
                )
                # ---- 错误重试逻辑 ----
                max_retries = picked.error_retry_count
                can_retry = (
                    max_retries > 0
                    and _retry_attempt < max_retries
                    and not _is_violation
                )
                if can_retry:
                    archive_id = uuid.uuid4().hex
                    try:
                        _orig_row = await self.db.get_task(task_id)
                        _archive_created_at = self._task_created_at_for_sql(
                            getattr(_orig_row, "created_at", None) if _orig_row else None
                        )
                        prompt_text = self._payload_to_prompt_text(payload)
                        await self.db.create_task(
                            Task(
                                task_id=archive_id,
                                task_type_code=picked.task_code,
                                generation_id=None,
                                status="failed",
                                progress=0,
                                prompt=prompt_text,
                                image_path=None,
                                window_pk=picked.window_pk,
                                window_ip=picked.window_ip,
                            ),
                            insert_created_at=_archive_created_at,
                        )
                        await self.db.update_task(
                            archive_id,
                            status="failed",
                            error_message=f"[{_retry_attempt + 1}|{max_retries}]{e}",
                            result=err_result,
                            content_violation=_is_violation if _is_violation else None,
                            set_completed=True,
                        )
                    except Exception:
                        pass
                    try:
                        await self.db.update_task(
                            task_id, status="queued", progress=0, touch_created_at=True
                        )
                    except Exception:
                        pass
                    _need_retry = True
                    _retry_error_msg = str(e)
                    logger.warning(
                        "task will retry %d/%d: %s err=%s, enqueue for dispatch",
                        _retry_attempt + 1, max_retries, task_id, e,
                    )
                else:
                    await self.db.update_task(
                        task_id,
                        status="failed",
                        error_message=str(e),
                        result=err_result,
                        content_violation=_is_violation if _is_violation else None,
                        set_completed=True,
                    )
                # 某些错误不应计入“窗口连续错误”（例如：Sora create 400 invalid_request、未抓到 POST 等环境/请求错误）
                # 执行器侧会抛出带 no_penalty=true 的异常（或同名属性），这里做兼容判断。
                if not no_penalty and not picked.create_task_handler == "sora_wm_remove":
                    await self.db.mark_mapping_error(
                        picked.mapping_id,
                        threshold=picked.threshold,
                        cooldown_seconds=5400,
                        reset_on_threshold=False,
                    )
                    # 连续错误达到“关闭窗口阈值”的整数倍时，启动倒计时关闭窗口（不重置连续错误）
                    try:
                        st = await self.db.get_mapping_runtime_state(mapping_id=picked.mapping_id)
                        ce = int((st or {}).get("consecutive_errors") or 0)
                    except Exception:
                        ce = 0
                    close_thr = max(1, int(getattr(picked, "close_window_threshold", 1) or 1))
                    should_close = ce > 0 and (ce % close_thr == 0)
                    # 注意：仅对 Sora 窗口做处理；其它模拟执行器没有需要维护的浏览器会话
                    if should_close:
                        try:
                            sess = get_or_create_sora_session(
                                vendor=picked.browser_vendor,
                                base_url=picked.browser_base_url,
                                access_key=picked.browser_access_key,
                                space_id=picked.space_id,
                                window_key=picked.window_key,
                            )
                            sess._schedule_idle_close()
                        except Exception:
                            pass
                if not _need_retry:
                    logger.exception("task failed: %s err=%s", task_id, e)
            finally:
                # 专用窗口任务：无论成败都调度关闭窗口
                if _is_dedicated_window:
                    try:
                        sess = get_or_create_sora_session(
                            vendor=picked.browser_vendor,
                            base_url=picked.browser_base_url,
                            access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                        )
                        sess._schedule_idle_close()
                    except Exception:
                        pass
                if not _need_retry:
                    self._task_payloads.pop(task_id, None)
        finally:
            try:
                await self.db.release_mapping_slot(picked.mapping_id)
            except Exception:
                pass
            # 专用窗口任务：释放并发计数（重试时也先释放，重新派发时再获取）
            if _is_dedicated_window:
                async with self._dedicated_window_lock:
                    self._dedicated_window_inflight = max(0, self._dedicated_window_inflight - 1)
            self._dispatch_event.set()

            if _need_retry:
                try:
                    payload = self._task_payloads.get(task_id) or {}
                    _retry_gen_id = str(payload.get("generation_id") or "").strip() or None
                    _retry_head_url = str(payload.get("head_url") or "").strip() or None
                    _bind_window_pk: Optional[int] = None
                    if _retry_gen_id and _retry_head_url:
                        _bind_window_pk = picked.window_pk

                    self._ensure_dispatcher()
                    await self._refresh_queue_config()
                    _retry_enqueued = False
                    _retry_queue_size = 0
                    async with self._queue_lock:
                        if len(self._pending_queue) >= self._queue_max_size:
                            try:
                                await self.db.update_task(
                                    task_id,
                                    status="failed",
                                    error_message=f"任务重试时队列已满，请稍后重试。原错误: {_retry_error_msg}",
                                    set_completed=True,
                                )
                            except Exception:
                                pass
                            self._task_payloads.pop(task_id, None)
                            logger.warning(
                                "task retry dropped (queue full): %s attempt=%d/%d",
                                task_id,
                                _retry_attempt + 1,
                                picked.error_retry_count,
                            )
                        else:
                            self._pending_queue.append(
                                QueuedTask(
                                    task_id=task_id,
                                    task_type_code=picked.task_code,
                                    payload=payload,
                                    enqueued_at=time.monotonic(),
                                    retry_attempt=_retry_attempt + 1,
                                    required_window_pk=_bind_window_pk,
                                    is_dedicated_window=_is_dedicated_window,
                                )
                            )
                            _retry_queue_size = len(self._pending_queue)
                            _retry_enqueued = True
                    if _retry_enqueued:
                        self._dispatch_event.set()
                        logger.info(
                            "task retry enqueued: %s attempt=%d/%d queue_size=%d bind_window=%s",
                            task_id,
                            _retry_attempt + 1,
                            picked.error_retry_count,
                            _retry_queue_size,
                            _bind_window_pk,
                        )
                except Exception as retry_err:
                    try:
                        await self.db.update_task(
                            task_id,
                            status="failed",
                            error_message=f"retry exception ({_retry_attempt + 1}/{picked.error_retry_count}): {retry_err}. original error: {_retry_error_msg}",
                            set_completed=True,
                        )
                    except Exception:
                        pass
                    self._task_payloads.pop(task_id, None)
                    logger.exception("task retry error: %s err=%s", task_id, retry_err)

