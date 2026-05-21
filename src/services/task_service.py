"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import json
import random
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
from ..core.config import config as app_config
from .image_task_executor import simulate_image_task
from .playwright_broswer_context import (
    acquire_browser_open_slot,
    get_or_create_ctx as get_or_create_playwright_ctx,
)
from .video_task_executor import simulate_video_task
from .sora_task_executor import (
    get_or_create_sora_session,
    sora_gen_video,
    refresh_sora_balance_best_effort,
    force_refresh_sora_access_token,
    window_pool_guard_unknown_handler_page,
)
from .task_executor_types import NonPenalizedTaskError
from .window_human_activity import (
    random_human_activity_delay,
)


def _sora_task_error_needs_forced_access_token_refresh(exc: BaseException) -> bool:
    """sora_gen_video 失败时：在 exception 路径触发一次窗口内重抓 token，供后续队列重试用。"""
    msg = str(exc or "")
    ml = msg.lower()
    if "token_expired" in ml or "token is expired" in ml:
        return True
    return False


def _db_bool(value: Any, *, default: bool = False) -> bool:
    """Parse sqlite/mysql-ish boolean values without treating string "0" as True."""
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off", ""):
        return False
    return bool(value)


def _effective_browser_pure_mode_from_context(ctx: Dict[str, Any]) -> bool:
    """窗口池 browser_open 的 pure_mode：使用绑定 pure_mode 列；缺省保持旧行为 True。"""
    return _db_bool(ctx.get("pure_mode"), default=True)


from .sora_wm_remove_executor import sora_wm_remove
from .sora_plus_register_executor import sora_plus_register
from .grok_workflow_executor import (
    DEFAULT_GROK_TARGET,
    get_or_create_grok_session,
    grok_ref_url_count,
    grok_workflow,
)
from .veo_workflow_executor import (
    _veo_resolve_n_frames,
    _veo_payload_video_model_override,
    get_or_create_veo_session,
    refresh_veo_balance_via_extension,
    veo_fetch_access_tokens_via_extension,
    veo_workflow,
    _veo_project_page_url,
)
from .jimeng_task_executor import (
    DEFAULT_DREAMINA_TARGET,
    get_or_create_dreamina_session,
    refresh_dreamina_balance,
    refresh_dreamina_balance_best_effort,
    dreamina_workflow,
    _DREAMINA_MIN_CREDIT,
    _DREAMINA_GIFT_CREDIT,
)
from .gpt_task_executor import gpt_workflow, refresh_gpt_balance_via_extension, DEFAULT_GPT_TARGET


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
    pure_mode: bool = True
    error_retry_count: int = 0
    project_id: Optional[str] = None

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
    credit_threthold = 1;
    """与 pick 时 remaining_quota >= floor 及预扣额度对齐（见 _consume_quota_after_window_pick）。"""
    code = (task_type_code or "").strip()
    if code == "sora_gen_video":
        return 3, credit_threthold
    if code == "veo_workflow":
        if _veo_payload_video_model_override(payload or {}) is not None:
            return 160,160
        elif _veo_resolve_n_frames(payload or {}) > 1:
            return 30,credit_threthold
        else:
            return 10,credit_threthold
    if code == "grok_workflow":
        if _veo_resolve_n_frames(payload or {}) > 1:
            return 30,credit_threthold
        else:
            return 10,credit_threthold
    if code == "dreamina_workflow":
        credit_threthold = _DREAMINA_MIN_CREDIT - _DREAMINA_GIFT_CREDIT;
        return _DREAMINA_MIN_CREDIT,credit_threthold
    return 3,credit_threthold


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

        # ---- 窗口池（按任务类型 code 维护应预热的 mapping_id；不占 inflight_slots） ----
        self._window_pool_stop = asyncio.Event()
        self._window_pool_task: Optional[asyncio.Task] = None
        self._window_pool_lock = asyncio.Lock()
        self._window_pool_reconcile_serial = asyncio.Lock()
        self._window_pool_wake = asyncio.Event()
        self._window_pool_force_reconcile = False
        self._window_pool_targets: dict[str, set[int]] = {}
        # Cloudflare 巡检周期（较长，默认 30 分钟）
        self._window_pool_cf_interval: float = 1800.0
        # 与 DB 对齐窗口池目标的 reconcile 周期（较短，默认 10 分钟）
        self._window_pool_reconcile_interval: float = 600.0
        # supervisor 单次休眠上限，避免 stop 后长时间无响应
        self._window_pool_supervisor_poll_cap: float = 60.0
        # Dreamina 余额刷新独立任务：不放在 _window_pool_supervisor_loop，避免被 reconcile/wait 阻塞。
        self._dreamina_refresh_task: Optional[asyncio.Task] = None
        self._dreamina_refresh_wake = asyncio.Event()
        self._dreamina_refresh_timeout: float = 60.0
        self._dreamina_refresh_scan_interval: float = 300.0

    def set_browser_pool_limit(self, limit: int) -> None:
        """Hot-update scheduling candidate pool size."""
        try:
            self._browser_pool_limit = max(1, int(limit))
        except Exception:
            pass

    def start_window_pool_maintainer(self) -> None:
        """在进程内启动窗口池协程（幂等）。"""
        self.start_dreamina_balance_refresher()
        if self._window_pool_task is not None and not self._window_pool_task.done():
            return
        try:
            self._window_pool_stop.clear()
        except Exception:
            pass
        self._window_pool_task = asyncio.create_task(
            self._window_pool_supervisor_loop(), name="window_pool_maintainer"
        )

    def start_dreamina_balance_refresher(self) -> None:
        """启动 Dreamina 余额刷新独立协程（幂等）。"""
        if self._dreamina_refresh_task is not None and not self._dreamina_refresh_task.done():
            return
        try:
            self._window_pool_stop.clear()
            self._dreamina_refresh_wake.set()  # 启动后立即扫描一次。
        except Exception:
            pass
        self._dreamina_refresh_task = asyncio.create_task(
            self._dreamina_balance_refresher_loop(), name="dreamina_balance_refresher"
        )

    async def refresh_window_pool_targets_now(self) -> None:
        """任务类型窗口池开关等变更后立即与 DB 对齐（不等 supervisor 周期）。"""
        self.start_window_pool_maintainer()
        try:
            await self._window_pool_reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("window_pool refresh_window_pool_targets_now failed")

    async def stop_window_pool_maintainer(self) -> None:
        """停止窗口池协程并尽量关闭池内会话。"""
        self._window_pool_stop.set()
        rt = self._dreamina_refresh_task
        self._dreamina_refresh_task = None
        if rt is not None and not rt.done():
            rt.cancel()
            try:
                await rt
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        t = self._window_pool_task
        self._window_pool_task = None
        if t is not None and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        async with self._window_pool_lock:
            codes = list(self._window_pool_targets.keys())
            all_mids: set[int] = set()
            for c in codes:
                all_mids |= set(self._window_pool_targets.get(c, set()))
            self._window_pool_targets.clear()
        for mid in all_mids:
            try:
                await self._window_pool_close_mapping(mid)
            except Exception:
                pass

    def _signal_window_pool_replenish(self) -> None:
        """空闲关闭等导致缺窗时唤醒 supervisor 尽快 reconcile；正在 reconcile 时忽略。"""
        try:
            if self._window_pool_reconcile_serial.locked():
                return
        except Exception:
            return
        self.start_window_pool_maintainer()
        try:
            self._window_pool_wake.set()
        except Exception:
            pass

    async def _window_pool_wait_interruptible(self, timeout: float) -> bool:
        """休眠最多 timeout 秒；若 stop 则返回 True。期间收到 wake 则清除事件并在未占用 reconcile 锁时置 force。"""
        if timeout <= 0:
            return self._window_pool_stop.is_set()
        deadline = time.monotonic() + timeout
        while True:
            if self._window_pool_stop.is_set():
                return True
            if self._window_pool_wake.is_set():
                self._window_pool_wake.clear()
                try:
                    if not self._window_pool_reconcile_serial.locked():
                        self._window_pool_force_reconcile = True
                except Exception:
                    pass
                return False
            rem = deadline - time.monotonic()
            if rem <= 0:
                return False
            try:
                await asyncio.wait_for(self._window_pool_stop.wait(), timeout=min(1.0, rem))
                return True
            except asyncio.TimeoutError:
                pass

    def _window_pool_random_human_activity_delay(self) -> float:
        """下一轮窗口池拟人操作延迟：在 reconcile_interval 与 cf_interval 之间随机。"""
        return random_human_activity_delay(
            self._window_pool_reconcile_interval,
            self._window_pool_cf_interval,
        )

    async def _window_pool_supervisor_loop(self) -> None:
        # 首次 Cloudflare 巡检在启动后满 cf_interval 再执行，避免与首轮 reconcile 抢浏览器打开槽位
        last_cf = time.monotonic()
        # 首轮尽快 reconcile 一次以预热池；之后按 _window_pool_reconcile_interval
        last_reconcile = time.monotonic() - self._window_pool_reconcile_interval
        # 空闲窗口拟人操作：启动后按 [_window_pool_reconcile_interval, _window_pool_cf_interval] 随机延迟执行
        while not self._window_pool_stop.is_set():
            try:
                r_sec, c_sec = await self.db.get_window_pool_maintainer_intervals_seconds()
                self._window_pool_reconcile_interval = float(r_sec)
                self._window_pool_cf_interval = float(c_sec)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            now = time.monotonic()
            reconcile_due = self._window_pool_force_reconcile or (
                now - last_reconcile >= self._window_pool_reconcile_interval
            )
            if reconcile_due:
                self._window_pool_force_reconcile = False
                last_reconcile = now
                try:
                    await self._window_pool_reconcile_once()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception("window_pool reconcile: %s", e)
            now = time.monotonic()
            now = time.monotonic()
            due_r = max(0.0, last_reconcile + self._window_pool_reconcile_interval - now)
            due_c = max(0.0, last_cf + self._window_pool_cf_interval - now)
            wait = min(due_r, due_c, self._window_pool_supervisor_poll_cap)
            wait = max(0.1, wait)
            if await self._window_pool_wait_interruptible(wait):
                break

    async def _dreamina_balance_refresher_loop(self) -> None:
        """Dreamina 余额刷新独立循环。

        启动后立即扫描所有 enabled dreamina_workflow 窗口：
        - 已到期/即将到期（cooldown_until <= now + 1 minute）的先刷新；
        - 未到期的计算最近 cooldown_until，并睡到该时间前 1 分钟再刷新；
        - 不设 80 个上限，符合条件的有多少刷多少。
        """
        while not self._window_pool_stop.is_set():
            try:
                due_rows = await self._dreamina_refresh_list_due_candidates()
                if due_rows:
                    await self._dreamina_refresh_rows(due_rows)
                    continue
                next_wait = await self._dreamina_refresh_seconds_until_next_due()
                wait = min(max(1.0, next_wait), self._dreamina_refresh_scan_interval)
                self._dreamina_refresh_wake.clear()
                try:
                    await asyncio.wait_for(self._dreamina_refresh_wake.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("dreamina balance refresher loop: %s", e)
                try:
                    await asyncio.wait_for(self._window_pool_stop.wait(), timeout=30.0)
                    return
                except asyncio.TimeoutError:
                    pass

    async def _dreamina_refresh_rows(self, rows: list[Dict[str, Any]]) -> None:
        if not rows:
            return
        rows = sorted(rows, key=lambda r: str(r.get("cooldown_until") or ""))
        logger.info("dreamina balance refresh: due=%d", len(rows))
        ok = 0
        fail = 0
        for row in rows:
            if self._window_pool_stop.is_set():
                return
            try:
                picked = PickedWindow(
                    mapping_id=int(row.get("mapping_id") or row.get("id")),
                    window_pk=int(row.get("window_pk") or 0),
                    window_key=str(row.get("window_key") or ""),
                    task_code=str(row.get("task_code") or ""),
                    task_concurrency=int(row.get("task_concurrency") or 1),
                    threshold=int(row.get("continuous_error_threshold") or 3),
                    close_window_threshold=int(row.get("continuous_error_close_window_threshold") or 3),
                    timeout_seconds=int(row.get("timeout_seconds") or 1800),
                    create_task_handler=str(row.get("create_task_handler") or ""),
                    browser_vendor=str(row.get("browser_vendor") or "generic"),
                    browser_base_url=str(row.get("browser_base_url") or ""),
                    browser_access_key=row.get("browser_access_key"),
                    space_id=str(row.get("space_id") or ""),
                    sora_access_token=row.get("sora_access_token"),
                    sora_access_expires=row.get("sora_access_expires"),
                    default_target_url=row.get("default_target_url"),
                    window_ip=row.get("window_ip"),
                    headless=_db_bool(row.get("headless"), default=False),
                    pure_mode=_db_bool(row.get("pure_mode"), default=True),
                    error_retry_count=int(row.get("error_retry_count") or 0),
                )
                if not picked.sora_access_token:
                    continue
                await asyncio.wait_for(
                    refresh_dreamina_balance(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=self._dreamina_refresh_timeout,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                    ),
                    timeout=self._dreamina_refresh_timeout,
                )
                ok += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fail += 1
                logger.warning("dreamina balance refresh mapping=%s err=%s", row.get("mapping_id") or row.get("id"), e)
            try:
                await asyncio.wait_for(self._window_pool_stop.wait(), timeout=random.uniform(0.1, 0.5))
                return
            except asyncio.TimeoutError:
                pass
        logger.info("dreamina balance refresh done: ok=%d fail=%d", ok, fail)

    async def _dreamina_refresh_list_due_candidates(self) -> list[Dict[str, Any]]:
        return await self._dreamina_refresh_list_candidates(due_only=True)

    async def _dreamina_refresh_seconds_until_next_due(self) -> float:
        threshold = int(_DREAMINA_MIN_CREDIT - _DREAMINA_GIFT_CREDIT)
        async with self.db._read_conn() as db:  # type: ignore[attr-defined]
            import aiosqlite
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT CAST((julianday(MIN(m.cooldown_until)) - julianday(datetime('now','localtime', '+1 minute'))) * 86400.0 AS REAL) AS wait_seconds
                FROM task_type_windows m
                JOIN task_types t ON t.id = m.task_type_id
                JOIN windows w ON w.id = m.window_pk
                JOIN spaces s ON s.id = w.space_pk
                JOIN browsers b ON b.id = s.browser_id
                WHERE t.deleted = 0 AND t.enabled = 1
                  AND m.deleted = 0 AND m.enabled = 1
                  AND w.deleted = 0 AND w.enabled = 1
                  AND b.deleted = 0
                  AND t.create_task_handler = 'dreamina_workflow'
                  AND TRIM(COALESCE(m.sora_access_token, '')) <> ''
                  AND COALESCE(m.remaining_quota, 0) >= ?
                  AND m.cooldown_until IS NOT NULL
                  AND m.cooldown_until > datetime('now','localtime', '+1 minute')
                """,
                (threshold,),
            )
            row = await cur.fetchone()
            if not row or row["wait_seconds"] is None:
                return self._dreamina_refresh_scan_interval
            try:
                return max(1.0, float(row["wait_seconds"]))
            except Exception:
                return self._dreamina_refresh_scan_interval

    async def _dreamina_refresh_list_candidates(self, *, due_only: bool) -> list[Dict[str, Any]]:
        threshold = int(_DREAMINA_MIN_CREDIT - _DREAMINA_GIFT_CREDIT)
        async with self.db._read_conn() as db:  # type: ignore[attr-defined]
            import aiosqlite
            db.row_factory = aiosqlite.Row
            due_clause = "AND m.cooldown_until <= datetime('now','localtime', '+1 minute')" if due_only else ""
            cur = await db.execute(
                f"""
                SELECT
                  m.id AS mapping_id,
                  m.window_pk,
                  m.remaining_quota,
                  m.sora_remaining_count,
                  m.sora_access_token,
                  m.sora_access_expires,
                  m.cooldown_until,
                  m.headless,
                  m.pure_mode,
                  t.code AS task_code,
                  t.concurrency AS task_concurrency,
                  t.continuous_error_threshold,
                  t.continuous_error_close_window_threshold,
                  t.timeout_seconds,
                  t.create_task_handler,
                  t.error_retry_count,
                  t.default_target_url,
                  w.window_key,
                  w.proxy_addr AS window_ip,
                  s.space_id,
                  b.vendor AS browser_vendor,
                  b.lan_addr AS browser_base_url,
                  b.access_key AS browser_access_key
                FROM task_type_windows m
                JOIN task_types t ON t.id = m.task_type_id
                JOIN windows w ON w.id = m.window_pk
                JOIN spaces s ON s.id = w.space_pk
                JOIN browsers b ON b.id = s.browser_id
                WHERE t.deleted = 0 AND t.enabled = 1
                  AND m.deleted = 0 AND m.enabled = 1
                  AND w.deleted = 0 AND w.enabled = 1
                  AND b.deleted = 0
                  AND t.create_task_handler = 'dreamina_workflow'
                  AND TRIM(COALESCE(m.sora_access_token, '')) <> ''
                  AND COALESCE(m.remaining_quota, 0) >= ?
                  AND m.cooldown_until IS NOT NULL
                  {due_clause}
                ORDER BY m.cooldown_until ASC, m.remaining_quota ASC, m.updated_at ASC
                """,
                (threshold,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


    async def _window_pool_reconcile_once(self) -> None:
        async with self._window_pool_reconcile_serial:
            await self._window_pool_reconcile_once_impl()

    async def _window_pool_reconcile_once_impl(self) -> None:
        try:
            all_types = await self.db.list_task_types()
        except Exception as e:
            logger.warning("window_pool list_task_types: %s", e)
            return
        if self._window_pool_stop.is_set():
            return

        new_targets: dict[str, set[int]] = {}
        # 任务类型仍存在、但被禁用或关闭了窗口池时，只应从窗口池管理集合中移除，
        # 不能主动关闭已经由窗口池/用户打开的指纹浏览器窗口。
        #
        # 之前这里把这类 code 直接从 new_targets 里略过，后面的 diff 逻辑会把
        # prev[code] 全部视为「需要关闭」，导致后台保存“关闭窗口池”后整批窗口
        # 被 _window_pool_close_mapping 调度 idle close。
        inactive_existing_codes: set[str] = set()

        for t in all_types:
            if self._window_pool_stop.is_set():
                return
            code = (t.code or "").strip()
            if not code:
                continue
            if not t.enabled or not bool(getattr(t, "window_pool_enabled", False)):
                inactive_existing_codes.add(code)
                continue
            handler = (t.create_task_handler or "").strip()
            credit_threthold = 1;
            if handler in ("veo_workflow",):
                hi = await self.db.task_type_has_mapping_remaining_quota_above(code, 30)
                floor = 30 if hi else 10
            else:
                floor,credit_threthold = _remaining_quota_exclusive_floor_for_pick(code, None)
            try:
                ids = await self.db.list_window_pool_target_mapping_ids(
                    code, self._browser_pool_limit, floor,credit_threthold
                )
            except Exception as e:
                logger.warning("window_pool targets %s: %s", code, e)
                continue
            new_targets[code] = {int(x) for x in ids}

        async with self._window_pool_lock:
            prev = {k: set(v) for k, v in self._window_pool_targets.items()}
            self._window_pool_targets = {k: set(v) for k, v in new_targets.items()}

        to_close: list[int] = []
        for code, old_set in prev.items():
            if code not in new_targets:
                if code in inactive_existing_codes:
                    logger.info(
                        "window_pool disabled for task_type=%s; detach %d managed windows without closing",
                        code,
                        len(old_set),
                    )
                    continue
                to_close.extend(old_set)
            else:
                to_close.extend(old_set - new_targets[code])
        for mid in to_close:
            if self._window_pool_stop.is_set():
                return
            await self._window_pool_close_mapping(mid)
            await asyncio.sleep(0)

        to_open: list[tuple[str, int]] = []
        for code, new_set in new_targets.items():
            old_set = prev.get(code, set())
            for mid in new_set - old_set:
                to_open.append((code, mid))
        for code, mid in to_open:
            if self._window_pool_stop.is_set():
                return
            ok = await self._window_pool_open_mapping(mid)
            if not ok:
                async with self._window_pool_lock:
                    s = self._window_pool_targets.get(code)
                    if s is not None:
                        s.discard(mid)
                logger.warning(
                    "window_pool open mapping=%s failed; keep mapping enabled", mid
                )
            await asyncio.sleep(0)

    async def _window_pool_open_mapping(self, mapping_id: int) -> bool:
        if self._window_pool_stop.is_set():
            return True
        ctx = await self.db.get_task_type_window_context(mapping_id)
        if not ctx:
            return True
        handler = (ctx.get("create_task_handler") or "").strip()
        base_url = str(ctx.get("lan_addr") or "").strip()
        window_key = str(ctx.get("window_key") or "").strip()
        if not base_url or not window_key:
            return True
        vendor = str(ctx.get("vendor") or "generic")
        access_key = ctx.get("access_key")
        space_id = str(ctx.get("space_id") or "")
        headless = bool(ctx.get("headless"))
        pure_mode = _effective_browser_pure_mode_from_context(ctx)
        target_url = (str(ctx.get("default_target_url") or "").strip() or None)

        try:
            async with acquire_browser_open_slot(base_url):
                if handler == "veo_workflow":
                    picked_pid = await self.db.get_random_veo_flow_project_id(mapping_id)
                    tu = target_url or "https://labs.google/fx"
                    if picked_pid is not None:
                        tu = f"https://labs.google/fx/tools/flow/project/{picked_pid}"

                    sess = get_or_create_veo_session(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    sess.browser_headless = headless
                    sess.browser_pure_mode = pure_mode
                    sess.idle_close_disabled = True
                    sess._cancel_idle_close()

                    try:
                        # VEO 窗口池只打开/唤起目标窗口，不连接 CDP，降低 Playwright 暴露面。
                        await sess.pw_ctx.open_fingerprint_window_only(
                            args=[tu],
                            force_open=sess.browser_force_open,
                            headless=headless,
                            pure_mode=pure_mode,
                        )
                        await asyncio.sleep(3.0)
                    except Exception as e:
                        logger.warning("window_pool open VEO mapping=%s by open-only failed: %s", mapping_id, e)
                        return False

                    token_info = None
                    try:
                        token_info = await veo_fetch_access_tokens_via_extension(
                            sess=sess,
                            target_url=tu,
                            space_id=space_id,
                            window_key=window_key,
                            connect_wait_seconds=8.0,
                            token_timeout_seconds=45.0,
                            log_file=sess._log_file,
                        )
                    except Exception as e:
                        logger.warning("window_pool VEO extension token mapping=%s failed: %s", mapping_id, e)
                        try:
                            await self.db.update_task_type_window(mapping_id=mapping_id, enabled=False)
                        except Exception:
                            pass
                        return True

                    long_session_token = str((token_info or {}).get("session_token") or (token_info or {}).get("access_token") or "").strip()
                    if long_session_token:
                        await self.db.update_task_type_window(
                            mapping_id=mapping_id,
                            sora_access_token=long_session_token,
                            sora_access_expires=str((token_info or {}).get("expires") or "").strip() or None,
                        )
                    return True
                elif handler == "grok_workflow":
                    tu = target_url or DEFAULT_GROK_TARGET
                    gs = get_or_create_grok_session(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    gs.browser_headless = headless
                    gs.browser_pure_mode = pure_mode
                    gs.idle_close_disabled = True
                    gs._cancel_idle_close()
                    await gs.ensure_open(
                        args=gs.browser_open_args,
                        force_open=gs.browser_force_open,
                        headless=headless,
                        pure_mode=pure_mode,
                    )
                    await gs._bring_target_page_to_front(refresh_target=False, drafts_url=tu)
                    try:
                        await gs.disconnect_playwright_under_bring_lock()
                    except Exception:
                        pass
                    return True
                elif handler == "dreamina_workflow":
                    tu = target_url or DEFAULT_DREAMINA_TARGET
                    ds = get_or_create_dreamina_session(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    ds.browser_headless = headless
                    ds.browser_pure_mode = pure_mode
                    ds.idle_close_disabled = True
                    ds._cancel_idle_close()
                    await ds.ensure_open(
                        args=ds.browser_open_args,
                        force_open=ds.browser_force_open,
                        headless=headless,
                        pure_mode=pure_mode,
                    )
                    await ds._bring_target_page_to_front(refresh_target=False, drafts_url=tu)
                    try:
                        await ds.disconnect_playwright_under_bring_lock()
                    except Exception:
                        pass
                    return True
                elif handler == "gpt_workflow":
                    from .gpt_task_executor import DEFAULT_GPT_TARGET, gpt_fetch_access_token_in_window  # type: ignore

                    tu = target_url or DEFAULT_GPT_TARGET
                    sess = get_or_create_veo_session(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    sess.browser_headless = headless
                    sess.browser_pure_mode = pure_mode
                    sess.idle_close_disabled = True
                    sess._cancel_idle_close()
                    await sess.ensure_open(args=[], force_open=False, headless=headless, pure_mode=pure_mode)
                    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=tu)
                    try:
                        tok_info = await gpt_fetch_access_token_in_window(
                            browser_vendor=vendor,
                            browser_base_url=base_url,
                            browser_access_key=access_key,
                            space_id=space_id,
                            window_key=window_key,
                            target_url=tu,
                            headless=headless,
                            pure_mode=pure_mode,
                            timeout_seconds=45.0,
                        )
                        access_token = str((tok_info or {}).get("access_token") or "").strip()
                        if access_token:
                            await self.db.update_task_type_window(
                                mapping_id=mapping_id,
                                sora_access_token=access_token,
                                sora_access_expires=str((tok_info or {}).get("expires") or "").strip() or None,
                            )
                    except Exception as e:
                        logger.warning("window_pool gpt token refresh mapping=%s failed: %s", mapping_id, e)
                    try:
                        await sess.disconnect_playwright_under_bring_lock()
                    except Exception:
                        pass
                    return True
                elif handler in ("sora_gen_video", "sora_wm_remove", "sora_plus_register"):
                    tu = target_url or "https://sora.chatgpt.com/drafts"
                    sess = get_or_create_sora_session(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    tok = str(ctx.get("sora_access_token") or "").strip()
                    if tok:
                        sess.set_access_token(tok, str(ctx.get("sora_access_expires") or "").strip() or None)
                    sess.browser_headless = headless
                    sess.browser_pure_mode = pure_mode
                    sess.idle_close_disabled = True
                    sess._cancel_idle_close()
                    await sess.ensure_open(
                        args=sess.browser_open_args,
                        force_open=sess.browser_force_open,
                        headless=headless,
                        pure_mode=pure_mode,
                    )
                    await sess._bring_sora_drafts_to_front(refresh_target=False, drafts_url=tu)
                    try:
                        await sess.disconnect_playwright_under_bring_lock()
                    except Exception:
                        pass
                    return True
                else:
                    tu = target_url
                    if not tu:
                        return True
                    pw = get_or_create_playwright_ctx(
                        vendor=vendor,
                        base_url=base_url,
                        access_key=access_key,
                        space_id=space_id,
                        window_key=window_key,
                    )
                    await pw.ensure_open(
                        args=[],
                        force_open=False,
                        headless=headless,
                        require_page=False,
                        pure_mode=pure_mode,
                    )
                    try:
                        async with pw.driver_lock:
                            if pw.context is None:
                                return True
                            if pw.page is None:
                                try:
                                    pages = list(getattr(pw.context, "pages", []) or [])
                                except Exception:
                                    pages = []
                                pw.page = pages[0] if pages else await pw.context.new_page()
                            try:
                                await pw.page.goto(tu, wait_until="domcontentloaded", timeout=60_000)
                            except Exception:
                                pass
                    finally:
                        try:
                            await pw.disconnect_playwright_only_under_driver_lock()
                        except Exception:
                            pass
                    return True
        except Exception as e:
            logger.warning("window_pool open mapping=%s err=%s", mapping_id, e)
            return False

    async def _window_pool_close_mapping(self, mapping_id: int) -> None:
        ctx = await self.db.get_task_type_window_context(mapping_id)
        if not ctx:
            return
        handler = (ctx.get("create_task_handler") or "").strip()
        base_url = str(ctx.get("lan_addr") or "").strip()
        window_key = str(ctx.get("window_key") or "").strip()
        if not base_url or not window_key:
            return
        vendor = str(ctx.get("vendor") or "generic")
        access_key = ctx.get("access_key")
        space_id = str(ctx.get("space_id") or "")
        try:
            if handler == "veo_workflow":
                sess = get_or_create_veo_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                sess.idle_close_disabled = False
                sess._schedule_idle_close()
            elif handler == "grok_workflow":
                gs = get_or_create_grok_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                gs.idle_close_disabled = False
                gs._schedule_idle_close()
            elif handler == "dreamina_workflow":
                ds = get_or_create_dreamina_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                ds.idle_close_disabled = False
                ds._schedule_idle_close()
            elif handler in ("sora_gen_video", "sora_wm_remove", "sora_plus_register"):
                sess = get_or_create_sora_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                sess.idle_close_disabled = False
                sess._schedule_idle_close()
            else:
                pw = get_or_create_playwright_ctx(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await pw.close_and_drop()
        except Exception as e:
            logger.debug("window_pool close mapping=%s err=%s", mapping_id, e)

    async def _window_pool_drop_sessions_for_mapping(self, mapping_id: int) -> None:
        """CF 仍失败时丢弃会话，由下次 reconcile 重新打开。"""
        ctx = await self.db.get_task_type_window_context(mapping_id)
        if not ctx:
            return
        handler = (ctx.get("create_task_handler") or "").strip()
        base_url = str(ctx.get("lan_addr") or "").strip()
        window_key = str(ctx.get("window_key") or "").strip()
        if not base_url or not window_key:
            return
        vendor = str(ctx.get("vendor") or "generic")
        access_key = ctx.get("access_key")
        space_id = str(ctx.get("space_id") or "")
        try:
            if handler == "veo_workflow":
                sess = get_or_create_veo_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await sess.close_and_drop()
            elif handler == "grok_workflow":
                gs = get_or_create_grok_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await gs.close_and_drop()
            elif handler == "dreamina_workflow":
                ds = get_or_create_dreamina_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await ds.close_and_drop()
            elif handler in ("sora_gen_video", "sora_wm_remove", "sora_plus_register"):
                sess = get_or_create_sora_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await sess.close_and_drop()
            else:
                pw = get_or_create_playwright_ctx(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                await pw.close_and_drop()
        except Exception as e:
            logger.debug("window_pool drop mapping=%s err=%s", mapping_id, e)

    async def _window_pool_cloudflare_tick(self) -> None:
        async with self._window_pool_lock:
            snapshot = {k: set(v) for k, v in self._window_pool_targets.items()}
        for _code, mids in snapshot.items():
            for mid in mids:
                if self._window_pool_stop.is_set():
                    return
                try:
                    await self._window_pool_cloudflare_one(mid)
                except Exception as e:
                    logger.warning("window_pool cf mapping=%s err=%s", mid, e)

    async def _window_pool_cloudflare_one(self, mapping_id: int) -> None:
        ctx = await self.db.get_task_type_window_context(mapping_id)
        if not ctx:
            return
        handler = (ctx.get("create_task_handler") or "").strip()
        base_url = str(ctx.get("lan_addr") or "").strip()
        window_key = str(ctx.get("window_key") or "").strip()
        if not base_url or not window_key:
            return
        vendor = str(ctx.get("vendor") or "generic")
        access_key = ctx.get("access_key")
        space_id = str(ctx.get("space_id") or "")
        target_url = (str(ctx.get("default_target_url") or "").strip() or None)

        try:
            if handler == "veo_workflow":
                picked_pid = await self.db.get_random_veo_flow_project_id(mapping_id)
                tu = target_url or "https://labs.google/fx"
                if picked_pid is not None:
                    tu = f"https://labs.google/fx/tools/flow/project/{picked_pid}"
                sess = get_or_create_veo_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                if not sess.idle_close_disabled:
                    return
                wpk = int(ctx.get("window_pk") or 0)
                try:
                    gl_ms = int(float(ctx.get("task_timeout_seconds") or 120) * 1000)
                except Exception:
                    gl_ms = 120_000
                gl_ms = max(45_000, min(gl_ms, 240_000))
                page = getattr(sess.pw_ctx, "page", None)
                await sess.raise_if_cloudflare_page_nonpenalized(
                    page,
                    stage="window_pool",
                    target_url=tu,
                    window_pool_google_relogin_db=self.db if wpk > 0 else None,
                    window_pool_google_relogin_window_pk=wpk if wpk > 0 else None,
                    window_pool_google_relogin_timeout_ms=gl_ms,
                )
                try:
                    await sess.disconnect_playwright_under_bring_lock()
                except Exception:
                    pass
            elif handler == "grok_workflow":
                tu = target_url or DEFAULT_GROK_TARGET
                gs = get_or_create_grok_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                if not gs.idle_close_disabled:
                    return
                page = getattr(gs.pw_ctx, "page", None)
                await window_pool_guard_unknown_handler_page(page, stage="window_pool", target_url=tu)
                try:
                    await gs.disconnect_playwright_under_bring_lock()
                except Exception:
                    pass
            elif handler == "dreamina_workflow":
                tu = target_url or DEFAULT_DREAMINA_TARGET
                ds = get_or_create_dreamina_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                if not ds.idle_close_disabled:
                    return
                page = getattr(ds.pw_ctx, "page", None)
                await window_pool_guard_unknown_handler_page(page, stage="window_pool", target_url=tu)
                try:
                    await ds.disconnect_playwright_under_bring_lock()
                except Exception:
                    pass
            elif handler in ("sora_gen_video", "sora_wm_remove", "sora_plus_register"):
                tu = target_url or "https://sora.chatgpt.com/drafts"
                sess = get_or_create_sora_session(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                if not sess.idle_close_disabled:
                    return
                page = getattr(sess.pw_ctx, "page", None)
                await sess._raise_if_cloudflare_page_nonpenalized(
                    page, stage="window_pool", drafts_url=tu
                )
                try:
                    await sess.disconnect_playwright_under_bring_lock()
                except Exception:
                    pass
            else:
                tu = target_url
                if not tu:
                    return
                pw = get_or_create_playwright_ctx(
                    vendor=vendor,
                    base_url=base_url,
                    access_key=access_key,
                    space_id=space_id,
                    window_key=window_key,
                )
                page = getattr(pw, "page", None)
                await window_pool_guard_unknown_handler_page(
                    page, stage="window_pool", target_url=tu
                )
                try:
                    await pw.disconnect_playwright_only_under_driver_lock()
                except Exception:
                    pass
        except NonPenalizedTaskError:
            logger.warning(
                "window_pool cloudflare persists, reset session mapping_id=%s", mapping_id
            )
            await self._window_pool_drop_sessions_for_mapping(mapping_id)
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
            # 兜底：若创建任务失败，释放预占槽位避免泄漏，并撤销挑选时标记的 window_status=1
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
        elif handler == "grok_workflow":
            try:
                n = grok_ref_url_count(payload or {})
                if n > 1:
                    await self.db.consume_mapping_quota(picked.mapping_id, amount=20)
                elif n == 1:
                    await self.db.consume_mapping_quota(picked.mapping_id, amount=10)
            except Exception:
                pass

    async def _finalize_picked_window(
        self, r: Dict[str, Any], payload: Optional[Dict[str, Any]] = None
    ) -> Optional[PickedWindow]:
        """由 reserve / pick 返回的行构造 PickedWindow，并处理 window_key 缺失与预扣额度。"""
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
            pure_mode=_effective_browser_pure_mode_from_context(r),
            error_retry_count=int(r.get("error_retry_count") or 0),
            project_id=str(r.get("current_project_id") or 0)
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        return picked

    async def _window_pool_pin_selected_mapping(self, task_type_code: str, mapping_id: int) -> None:
        """显式选窗成功后钉入窗口池集合（与 DB 推导目标合并），便于 reconcile / CF 统一管理。"""
        code = (task_type_code or "").strip()
        if not code:
            return
        try:
            tt = await self.db.get_task_type_by_code(code)
        except Exception:
            return
        if not tt or not bool(getattr(tt, "window_pool_enabled", False)):
            return
        mid = int(mapping_id)
        async with self._window_pool_lock:
            self._window_pool_targets.setdefault(code, set()).add(mid)

    async def _pick_window(self, task_type_code: str, payload: Optional[Dict[str, Any]] = None) -> Optional[PickedWindow]:
        """从 DB 候选中挑选窗口，并在 DB 中原子预占并发槽位。

        说明：
        - 预占由 DB 字段 inflight_slots 完成（支持多进程/多实例，避免超卖）
        - 预占成功同时将 windows.window_status 置 1，使单浏览器窗口池上限在打开指纹前即计数
        - 挑选排序由 DB 决定（consecutive_errors 最低优先，其次 remaining_quota 最少优先）
        - 若任务类型开启窗口池：仅从 `_window_pool_targets` 内由 DB 单事务 `pick_and_reserve_window_from_pool` 原子挑选（与全局 pick 相同：+60s error_cooldown_until，避免高并发下多任务盯上同一 mapping）；池为空或无可用则返回 None（不回退全局 pick）
        """
        floor,credit_threthold = _remaining_quota_exclusive_floor_for_pick(task_type_code, payload)
        try:
            tt = await self.db.get_task_type_by_code(task_type_code)
        except Exception:
            tt = None
        if tt and bool(getattr(tt, "window_pool_enabled", False)):
            async with self._window_pool_lock:
                pool_ids = list(self._window_pool_targets.get(task_type_code, set()))
            if not pool_ids:
                return None
            r = await self.db.pick_and_reserve_window_from_pool(
                task_type_code,
                pool_ids,
                remaining_quota_exclusive_floor=floor,
                credit_threthold=credit_threthold
            )
            if not r:
                return None
            return await self._finalize_picked_window(r, payload)

        r = await self.db.pick_and_reserve_window_for_task(
            task_type_code=task_type_code,
            browser_pool_limit=self._browser_pool_limit,
            remaining_quota_exclusive_floor=floor,
            credit_threthold=credit_threthold
        )
        if not r:
            return None
        return await self._finalize_picked_window(r, payload)

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
            pure_mode=_effective_browser_pure_mode_from_context(r),
            error_retry_count=int(r.get("error_retry_count") or 0),
            project_id=str(r.get("current_project_id") or 0),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        await self._window_pool_pin_selected_mapping(task_type_code, mid)
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
            pure_mode=_effective_browser_pure_mode_from_context(r),
            error_retry_count=int(r.get("error_retry_count") or 0),
            project_id=str(r.get("current_project_id") or 0),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        await self._consume_quota_after_window_pick(picked, payload)
        await self._window_pool_pin_selected_mapping(task_type_code, mid)
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
                    project_id = picked.project_id
                    project_page = _veo_project_page_url(project_id=project_id, hint_url=picked.default_target_url)
                    picked.default_target_url = project_page;
                    result,project_page = await asyncio.wait_for(
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
                            pure_mode=picked.pure_mode,
                            db=self.db,
                            task_type_window_id=picked.mapping_id,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                    picked.default_target_url = project_page;
                    print(f"default_target_url:{project_page}");
                elif picked.create_task_handler == "grok_workflow":
                    grok_payload = dict(payload or {})
                    result = await asyncio.wait_for(
                        grok_workflow(
                            grok_payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                            default_target_url=picked.default_target_url,
                            headless=picked.headless,
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                            db=self.db,
                            task_type_window_id=picked.mapping_id,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.create_task_handler == "dreamina_workflow":
                    dreamina_payload = dict(payload or {})
                    result = await asyncio.wait_for(
                        dreamina_workflow(
                            dreamina_payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                            default_target_url=picked.default_target_url,
                            headless=picked.headless,
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                            pure_mode=picked.pure_mode,
                            db=self.db,
                            task_type_window_id=picked.mapping_id,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.create_task_handler == "gpt_workflow":
                    gpt_payload = dict(payload or {})
                    result = await asyncio.wait_for(
                        gpt_workflow(
                            gpt_payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                            default_target_url=picked.default_target_url,
                            headless=picked.headless,
                            pure_mode=picked.pure_mode,
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
                    await refresh_veo_balance_via_extension(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        force_refresh_token=False,
                    )
                elif picked.create_task_handler == "gpt_workflow":
                    await refresh_gpt_balance_via_extension(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        auto_triger_connection=False,
                    )
                elif picked.create_task_handler == "dreamina_workflow":
                    await refresh_dreamina_balance_best_effort(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        task_id=task_id,
                    )
                elif picked.create_task_handler == "grok_workflow":
                    pass
                elif picked.create_task_handler == "sora_gen_video":
                    await refresh_sora_balance_best_effort(
                        db=self.db,
                        picked=picked,
                        target_url=target_url,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        task_id=task_id,
                    )
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
                    await refresh_veo_balance_via_extension(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        auto_triger_connection=False,
                    )
                elif picked.create_task_handler == "dreamina_workflow":
                    await refresh_dreamina_balance_best_effort(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        task_id=task_id,
                    )
                elif picked.create_task_handler == "grok_workflow":
                    await refresh_gpt_balance_via_extension(
                        db=self.db,
                        picked=picked,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        auto_triger_connection=False,
                    )
                elif picked.create_task_handler == "sora_gen_video":
                    await refresh_sora_balance_best_effort(
                        db=self.db,
                        picked=picked,
                        target_url=target_url,
                        refresh_timeout_seconds=refresh_timeout_seconds,
                        signal_window_pool_replenish=self._signal_window_pool_replenish,
                        task_id=task_id,
                    )
                    if _sora_task_error_needs_forced_access_token_refresh(e):
                        await force_refresh_sora_access_token(
                            db=self.db,
                            picked=picked,
                            target_url=target_url,
                            refresh_timeout_seconds=refresh_timeout_seconds,
                            task_id=task_id,
                        )
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
                    or "参考图中包含未成年" in str(e)
                    or "分辨率过高" in str(e)
                    or "不能超过 4k" in _err_lower
                    or "不能超过4k" in _err_lower
                    or bool(getattr(e, "content_violation", False))
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
                        cooldown_seconds=3600,
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
                    # Sora / Veo 等真实浏览器会话：达阈值后调度空闲关闭，窗口池协程会再补开
                    if should_close:
                        try:
                            if (picked.create_task_handler or "").strip() == "veo_workflow":
                                v_sess = get_or_create_veo_session(
                                    vendor=picked.browser_vendor,
                                    base_url=picked.browser_base_url,
                                    access_key=picked.browser_access_key,
                                    space_id=picked.space_id,
                                    window_key=picked.window_key,
                                )
                                v_sess._schedule_idle_close()
                            elif (picked.create_task_handler or "").strip() == "grok_workflow":
                                g_sess = get_or_create_grok_session(
                                    vendor=picked.browser_vendor,
                                    base_url=picked.browser_base_url,
                                    access_key=picked.browser_access_key,
                                    space_id=picked.space_id,
                                    window_key=picked.window_key,
                                )
                                g_sess._schedule_idle_close()
                            elif (picked.create_task_handler or "").strip() == "dreamina_workflow":
                                d_sess = get_or_create_dreamina_session(
                                    vendor=picked.browser_vendor,
                                    base_url=picked.browser_base_url,
                                    access_key=picked.browser_access_key,
                                    space_id=picked.space_id,
                                    window_key=picked.window_key,
                                )
                                d_sess._schedule_idle_close()
                            else:
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
                        self._signal_window_pool_replenish()
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
                    self._signal_window_pool_replenish()
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

