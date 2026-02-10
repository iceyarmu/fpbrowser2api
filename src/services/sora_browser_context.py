"""Sora 浏览器上下文（窗口复用）与通用 API 能力。

拆分目的：
- `_SoraBrowserContext` 是多个功能的公共依赖（创建任务、刷新额度、获取邀请码、手动开关窗口等）
- 将其从“任务执行器”中抽离，便于未来新增其它任务类型/执行器时保持清晰边界

说明：
- 目前为了保持改动可控，本模块仍复用 `task_executor.py` 内的大量 Sora helper 函数（Playwright 抓包/请求等）。
  后续你想进一步瘦身时，可以再把这些 helper 迁移到 `sora_helpers.py` 一类模块中。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .fp_browser_client import FPBrowserClient

from .task_executor import (
    _append_log,
    _extract_task_obj,
    _normalize_cdp_endpoint,
    _normalize_progress,
    _pw_page_fetch_json,
    _pw_page_fetch_tx,
    _pw_pick_working_page_from_context,
    _safe_trim,
    _sora_api_get_video_drafts_pw,
    _sora_api_post_project_y_post_pw,
    _sora_backend_url_from_target,
    _sora_create_task_pw,
    _sora_extract_bearer_from_any_post_pw,
    _sora_generate_sentinel_token_in_fp_context_pw,
)

from .task_executor_types import ProgressCB


@dataclass
class _SoraWatcher:
    task_id: str
    deadline: float
    progress_cb: ProgressCB
    future: "asyncio.Future[Dict[str, Any]]"
    last_sent_progress: int = -1
    last_status: Any = None
    last_progress_pct: Optional[float] = None
    miss_pending_count: int = 0


@dataclass
class _SoraBrowserContext:
    cache_key: str
    vendor: str
    base_url: str
    access_key: Optional[str]
    space_id: str
    window_key: str
    fp_client: FPBrowserClient

    playwright: Any = None
    browser: Any = None
    context: Any = None
    page: Any = None
    cdp_endpoint: Optional[str] = None
    last_used_at: float = field(default_factory=lambda: time.time())
    # 创建任务必须串行（create_lock），页面操作互斥（driver_lock）
    create_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    driver_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    watchers: Dict[str, _SoraWatcher] = field(default_factory=dict)
    monitor_task: Optional[asyncio.Task] = None
    idle_close_task: Optional[asyncio.Task] = None
    # 手动“保持打开”：为 True 时，禁止一切 _schedule_idle_close 自动关闭
    idle_close_disabled: bool = False

    # 监控配置（会在 watch 时更新）
    pending_url_regex: Optional[str] = None
    monitor_log_path: Optional[str] = None
    poll_interval_seconds: float = 1.0
    sniff_timeout_seconds: float = 4.0
    idle_close_seconds: float = 30.0

    # browser_open 参数（会在每次 create 时更新，reopen 也使用最近一次）
    browser_open_args: list[str] = field(default_factory=list)
    browser_force_open: bool = False
    browser_headless: bool = False

    # API 模式所需的鉴权信息（从指纹浏览器 network 抓取）
    bearer_token: Optional[str] = None
    user_agent: Optional[str] = None
    oai_device_id: Optional[str] = None
    sentinel_token: Optional[str] = None
    invite_code: Optional[str] = None

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
    ) -> None:
        """确保窗口已打开且 Playwright 已通过 CDP 连接到指纹浏览器。"""
        self.last_used_at = time.time()

        # 判断窗口是否已经打开
        # - 情况1：本服务已建立 Playwright/CDP 连接（browser/page 已存在）
        # - 情况2：指纹浏览器软件端窗口已打开，但本服务未连接（需要先查 connection_info）
        if self.browser is not None and self.page is not None:
            return

        try:
            from playwright.async_api import async_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(
                f"Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；错误：{e}"
            )

        # 先查询“是否已打开”，避免窗口已打开时 /browser/open 返回错误导致无法继续连接
        raw_endpoint = ""
        try:
            conn = await self.fp_client.get_open_window_connection_info(
                vendor=self.vendor,
                base_url=self.base_url,
                access_key=self.access_key,
                window_key=self.window_key,
            )
            if conn:
                raw_endpoint = str(conn.get("http") or conn.get("ws") or "").strip()
        except Exception:
            # connection_info 查询失败则忽略，继续走 open 逻辑
            pass

        if not raw_endpoint:
            rsp = await self.fp_client.browser_open(
                vendor=self.vendor,
                base_url=self.base_url,
                access_key=self.access_key,
                space_id=self.space_id,
                window_key=self.window_key,
                args=args or [],
                force_open=bool(force_open),
                headless=bool(headless),
            )
            if (rsp or {}).get("code") != 0:
                # 兜底：部分服务在“窗口已打开”时会返回非 0，但此时仍可用 connection_info 取到 endpoint
                try:
                    conn = await self.fp_client.get_open_window_connection_info(
                        vendor=self.vendor,
                        base_url=self.base_url,
                        access_key=self.access_key,
                        window_key=self.window_key,
                    )
                    if conn:
                        raw_endpoint = str(conn.get("http") or conn.get("ws") or "").strip()
                except Exception:
                    pass
                if not raw_endpoint:
                    raise RuntimeError(f"browser_open 失败：{rsp}")

            if not raw_endpoint:
                data = (rsp or {}).get("data") or {}
                raw_endpoint = str(data.get("http") or data.get("ws") or "").strip()

        debugger_address = _normalize_cdp_endpoint(raw_endpoint)
        if not debugger_address:
            raise RuntimeError(f"无法获取 http/ws(CDP endpoint)：raw={raw_endpoint!r}")

        self.cdp_endpoint = debugger_address

        # 建立/复用 Playwright 连接
        if self.playwright is None:
            self.playwright = await async_playwright().start()

        try:
            self.browser = await self.playwright.chromium.connect_over_cdp(debugger_address)
        except Exception as e:
            # 连接失败时清理并抛出
            try:
                await self.playwright.stop()
            except Exception:
                pass
            self.playwright = None
            self.browser = None
            raise RuntimeError(f"连接指纹浏览器 CDP 失败：endpoint={debugger_address} err={e}") from e

        # 尽量复用现有 context/page（指纹浏览器通常已有默认 context）
        try:
            ctxs = list(getattr(self.browser, "contexts", []) or [])
        except Exception:
            ctxs = []
        if ctxs:
            # 选择一个“更像正常页面”的 context（避免落到扩展/devtools 专用 context）
            best_ctx = None
            best_score = -1
            for c in ctxs:
                try:
                    pages = list(getattr(c, "pages", []) or [])
                except Exception:
                    pages = []
                score = 0
                for p in pages:
                    try:
                        u = str(getattr(p, "url", "") or "")
                    except Exception:
                        u = ""
                    if u.startswith(("http://", "https://")):
                        score += 2
                    elif u.startswith("about:blank") or not u:
                        score += 1
                if score > best_score:
                    best_score = score
                    best_ctx = c
            self.context = best_ctx or ctxs[0]
        else:
            self.context = await self.browser.new_context()

        # 关键：不要盲选 pages[-1]，优先挑可导航的 http(s)/about:blank 页面，否则新建
        self.page = await _pw_pick_working_page_from_context(self.context)
        try:
            await self.page.bring_to_front()
        except Exception:
            pass

    async def _ensure_bearer_token(self, *, target_url: str) -> str:
        """确保 bearer_token 可用；必要时 goto 触发请求并从 headers 抓取。"""
        if self.page is None:
            raise RuntimeError("page 未初始化")

        if self.bearer_token:
            return str(self.bearer_token)

        log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
        # 尽量用目标页触发后端请求（Sora 页面加载通常会有带 Authorization 的 XHR）
        try:
            await self.page.goto(target_url, wait_until="domcontentloaded")
        except Exception:
            pass
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass

        info = await _sora_extract_bearer_from_any_post_pw(self.page, timeout_seconds=20.0, log_file=log_file)
        tok = str(info.get("token") or "").strip()
        if not tok:
            raise RuntimeError("未抓到 Bearer token（请确认窗口已登录 Sora）")
        self.bearer_token = tok
        try:
            self.user_agent = str(info.get("user_agent") or "").strip() or self.user_agent
        except Exception:
            pass
        return tok

    async def api_nf_check(self, *, target_url: str) -> Dict[str, Any]:
        """读取 Sora 余额：GET /backend/nf/check（返回 remaining_count/rate_limit_reached/access_resets_in_seconds）。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        self._schedule_idle_close()
        async with self.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = await self._ensure_bearer_token(target_url=target_url)
            url = _sora_backend_url_from_target(target_url, "/backend/nf/check")
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US"}
            tx = await _pw_page_fetch_json(self.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
            obj = tx.get("_json") or {}
            rate = (obj or {}).get("rate_limit_and_credit_balance") or {}

            remaining = int(rate.get("estimated_num_videos_remaining") or 0)
            resets = int(rate.get("access_resets_in_seconds") or 0)
            out: Dict[str, Any] = {
                "remaining_count": remaining,
                "rate_limit_reached": bool(rate.get("rate_limit_reached", False)),
                "access_resets_in_seconds": resets,
                "raw": obj,
            }

            # cooldown_until：将“重置 remaining_count 还需秒数”加到当前系统时间，得到下一次重置时间点。
            # 该字段由上层（刷新额度 handler / 任务完成回写处）写回 task_type_windows.cooldown_until。
            try:
                dt = datetime.now() + timedelta(seconds=max(0, int(resets or 0)))
                out["cooldown_until"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

            return out

    async def api_invite_mine(self, *, target_url: str) -> Dict[str, Any]:
        """读取邀请码：GET /backend/project_y/invite/mine（必要时尝试 bootstrap 激活）。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        self._schedule_idle_close()
        async with self.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = await self._ensure_bearer_token(target_url=target_url)
            url = _sora_backend_url_from_target(target_url, "/backend/project_y/invite/mine")
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US"}
            tx = await _pw_page_fetch_tx(self.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
            status = int(tx.get("status") or 0) if tx.get("status") is not None else 0
            body = str(tx.get("response_body") or "")
            obj: Any = None
            try:
                obj = json.loads(body) if body else None
            except Exception:
                obj = None

            # 401 时尝试 bootstrap 再重试一次（参考 token_manager.py）
            if status == 401:
                try:
                    boot_url = _sora_backend_url_from_target(target_url, "/backend/m/bootstrap")
                    await _pw_page_fetch_tx(self.page, url=boot_url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    tx2 = await _pw_page_fetch_json(self.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    obj = tx2.get("_json")
                except Exception:
                    pass

            data = obj if isinstance(obj, dict) else {}
            invite_code = (data or {}).get("invite_code")
            self.invite_code = str(invite_code).strip() if invite_code else None
            return {
                "supported": bool(self.invite_code),
                "invite_code": self.invite_code,
                "redeemed_count": int((data or {}).get("redeemed_count") or 0),
                "total_count": int((data or {}).get("total_count") or 0),
                "raw": data,
            }

    def _cancel_idle_close(self) -> None:
        t = self.idle_close_task
        self.idle_close_task = None
        if t and not t.done():
            # 关键：避免“自己取消自己”
            # idle_close_task 调用 close_and_drop -> close()，如果这里把当前任务 cancel 掉，
            # 会在后续 await（例如 browser_close）处立刻抛 CancelledError，表现为 close 卡住/不继续打印。
            try:
                cur = asyncio.current_task()
            except Exception:
                cur = None
            if cur is not None and t is cur:
                return
            t.cancel()

    def _schedule_idle_close(self) -> None:
        """当 ctx 没有任务执行时自动 close。"""
        # 被手动“保持打开”时：取消已有 idle_close 任务，且不再创建新的自动关闭任务
        if bool(self.idle_close_disabled):
            self._cancel_idle_close()
            return

        self._cancel_idle_close()

        async def _job():
            try:
                secs = max(0.0, float(self.idle_close_seconds))
                if secs <= 0:
                    return
                await asyncio.sleep(secs)
                # sleep 期间可能被切到“保持打开”
                if bool(self.idle_close_disabled):
                    return
                if self.watchers:
                    return
                if self.create_lock.locked():
                    return
                await self.close_and_drop()
            except asyncio.CancelledError:
                return
            except Exception:
                return

        self.idle_close_task = asyncio.create_task(_job())

    async def close_and_drop(self) -> None:
        await self.close()
        _drop_ctx(self.cache_key)

    async def create_task(
        self,
        *,
        prompt: str,
        target_url: str,
        create_button_text_regex: str,
        monitor_seconds: float,
        monitor_url_regex: str,
        monitor_log_path: Optional[str],
        first_image_url: Optional[str],
        orientation: str,
        n_frames: int,
        browser_open_args: list[str],
        browser_force_open: bool,
        browser_headless: bool,
    ) -> Tuple[str, Dict[str, Any]]:
        """串行创建任务：只有 create 拿到结果后才放行下一个。"""
        self.last_used_at = time.time()
        self._cancel_idle_close()

        self.browser_open_args = browser_open_args or []
        self.browser_force_open = bool(browser_force_open)
        self.browser_headless = bool(browser_headless)
        async with self.create_lock:
            try:
                await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
                async with self.driver_lock:
                    task_id, create_tx, auth_state = await _sora_create_task_pw(
                        page=self.page,
                        prompt=prompt,
                        target_url=target_url,
                        create_button_text_regex=create_button_text_regex,
                        monitor_seconds=monitor_seconds,
                        monitor_url_regex=monitor_url_regex,
                        monitor_log_path=monitor_log_path,
                        first_image_url=first_image_url,
                        orientation=orientation,
                        n_frames=n_frames,
                    )
                    # 保存 API 鉴权信息，供后续 pending 轮询使用（不写入日志/结果，避免泄露）
                    try:
                        self.bearer_token = auth_state.get("bearer_token")
                        self.sentinel_token = auth_state.get("sentinel_token")
                        self.user_agent = auth_state.get("user_agent")
                        self.oai_device_id = auth_state.get("oai_device_id")
                    except Exception:
                        pass
                    return task_id, create_tx
            finally:
                # create 结束后，如果当前没有任何任务在该 ctx 上跑，启动空闲自动回收
                if not self.watchers:
                    self._schedule_idle_close()

    async def watch_task_progress(
        self,
        *,
        task_id: str,
        progress_cb: ProgressCB,
        pending_url_regex: str,
        monitor_log_path: Optional[str],
        max_wait_seconds: float,
        poll_interval_seconds: float,
        sniff_timeout_seconds: float,
        idle_close_seconds: float,
    ) -> Dict[str, Any]:
        """并行等待任务进度：多个任务共享同一个后台轮询。"""
        self.last_used_at = time.time()
        self._cancel_idle_close()

        self.pending_url_regex = pending_url_regex
        self.monitor_log_path = monitor_log_path
        self.poll_interval_seconds = max(0.2, float(poll_interval_seconds))
        self.sniff_timeout_seconds = max(0.2, float(sniff_timeout_seconds))
        self.idle_close_seconds = max(0.0, float(idle_close_seconds))

        loop = asyncio.get_running_loop()
        fut: "asyncio.Future[Dict[str, Any]]" = loop.create_future()
        w = _SoraWatcher(
            task_id=str(task_id),
            deadline=time.time() + max(1.0, float(max_wait_seconds)),
            progress_cb=progress_cb,
            future=fut,
        )
        self.watchers[w.task_id] = w

        if self.monitor_task is None or self.monitor_task.done():
            self.monitor_task = asyncio.create_task(self._monitor_loop())

        try:
            return await fut
        finally:
            self.watchers.pop(w.task_id, None)
            if not self.watchers:
                self._schedule_idle_close()

    async def _monitor_loop(self) -> None:
        """单 ctx 单协程轮询：每次只短暂持有 driver_lock，从而不长期阻塞 create。"""
        try:
            while True:
                if not self.watchers:
                    return

                now = time.time()
                for tid, w in list(self.watchers.items()):
                    if now > w.deadline and not w.future.done():
                        w.future.set_exception(RuntimeError(f"进度监控超时：task_id={tid}"))
                        self.watchers.pop(tid, None)

                if not self.watchers:
                    return

                # 兜底：driver 被关闭时尝试重连（用最近一次 browser_open 参数）
                try:
                    await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
                except Exception as e:
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError(f"浏览器/driver 不可用：{e}"))
                        self.watchers.pop(tid, None)
                    return

                tx: Optional[Dict[str, Any]] = None
                # API 轮询 pending：不依赖页面是否自动发请求
                log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
                if not self.bearer_token:
                    # 没有 token 无法轮询，直接报错给所有 watcher
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError("缺少 bearer_token，无法轮询 pending（create 未成功抓取鉴权信息）"))
                        self.watchers.pop(tid, None)
                    return

                try:
                    pending_url = _sora_backend_url_from_target(getattr(self.page, "url", "") or "https://sora.chatgpt.com", "/backend/nf/pending/v2")
                except Exception:
                    pending_url = "https://sora.chatgpt.com/backend/nf/pending/v2"

                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {self.bearer_token}",
                    "OAI-Language": "en-US",
                    "OAI-Device-Id": str(self.oai_device_id or ""),
                }

                async with self.driver_lock:
                    try:
                        tx = await _pw_page_fetch_tx(self.page, url=pending_url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    except Exception as e:
                        _append_log(log_file, f"[sora][api] pending poll failed: {e}")
                        tx = None

                body = (tx or {}).get("response_body") or ""
                payload_obj: Any = None
                try:
                    payload_obj = json.loads(body) if body else None
                except Exception:
                    payload_obj = None

                index: Dict[str, Dict[str, Any]] = {}
                if isinstance(payload_obj, list):
                    for it in payload_obj:
                        if isinstance(it, dict) and it.get("id") is not None:
                            index[str(it.get("id"))] = it

                # 如果 pending 中找不到任务，按 generation_handler.py 的逻辑：去 drafts 里找（表示已完成或被转移）
                missing_tids: list[str] = []
                for tid, w in list(self.watchers.items()):
                    task_obj = index.get(str(tid)) if index else _extract_task_obj(payload_obj, str(tid))
                    if not task_obj:
                        w.miss_pending_count += 1
                        # 连续两次 pending 未命中就尝试 drafts（减少频率）
                        if w.miss_pending_count >= 2:
                            missing_tids.append(str(tid))
                        continue
                    w.miss_pending_count = 0

                    status = task_obj.get("status")
                    progress_pct = _normalize_progress(task_obj.get("progress_pct"))
                    w.last_status = status
                    w.last_progress_pct = progress_pct

                    if progress_pct is not None:
                        p_int = int(max(0.0, min(1.0, float(progress_pct))) * 100.0)
                        if p_int != w.last_sent_progress:
                            w.last_sent_progress = p_int
                            try:
                                await w.progress_cb(p_int, {"task_id": tid, "status": status})
                            except Exception:
                                pass

                    if progress_pct is not None and float(progress_pct) >= 1.0:
                        if not w.future.done():
                            w.future.set_result({"task_id": tid, "status": status, "progress_pct": progress_pct, "done": True})
                        self.watchers.pop(tid, None)

                if missing_tids:
                    # drafts 查询（一次查询覆盖多个 tid）
                    try:
                        log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
                        drafts = await _sora_api_get_video_drafts_pw(
                            self.page,
                            target_url=str(getattr(self.page, "url", "") or "https://sora.chatgpt.com/drafts"),
                            bearer_token=str(self.bearer_token or ""),
                            limit=15,
                            log_file=log_file,
                        )
                        items = drafts.get("items", []) if isinstance(drafts, dict) else []
                    except Exception as e:
                        items = []
                        try:
                            _append_log(log_file, f"[sora][drafts] fetch failed: {e}")
                        except Exception:
                            pass

                    if isinstance(items, list) and items:
                        by_task: Dict[str, Dict[str, Any]] = {}
                        for it in items:
                            if isinstance(it, dict) and it.get("task_id") is not None:
                                by_task[str(it.get("task_id"))] = it
                        for tid in list(missing_tids):
                            it = by_task.get(str(tid))
                            if not it:
                                continue
                            # 找到 drafts 记录即认为任务已结束（成功/违规/失败由上层 finalize 决定）
                            if tid in self.watchers:
                                w = self.watchers.get(tid)
                                if w and not w.future.done():
                                    w.future.set_result(
                                        {"task_id": tid, "status": "completed", "progress_pct": 1.0, "done": True, "draft": it}
                                    )
                                self.watchers.pop(tid, None)

                if not self.watchers:
                    return

                await asyncio.sleep(float(self.poll_interval_seconds))
        finally:
            if not self.watchers:
                self._schedule_idle_close()

    async def finalize_video_and_publish(
        self,
        *,
        task_id: str,
        prompt: str,
        target_url: str,
        drafts_limit: int = 100,
    ) -> Dict[str, Any]:
        """任务完成后：从 drafts 找到对应视频 → 发布草稿（去水印）→ 返回 {post_id, urls, draft}。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        async with self.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            if not self.bearer_token:
                raise RuntimeError("缺少 bearer_token，无法查询 drafts/发布")
            if not self.sentinel_token:
                # 兜底：现生成一次 sentinel（复用指纹浏览器 context）
                self.sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(self.page, device_id=self.oai_device_id, log_file=log_file)
            if not self.sentinel_token:
                raise RuntimeError("缺少 sentinel_token，无法发布草稿")

            def _get_item_task_id(it: Dict[str, Any]) -> str:
                # 兼容不同字段命名/嵌套结构
                try:
                    v = it.get("task_id")
                    if v:
                        return str(v)
                except Exception:
                    pass
                try:
                    v = it.get("taskId")
                    if v:
                        return str(v)
                except Exception:
                    pass
                try:
                    t = it.get("task")
                    if isinstance(t, dict) and t.get("id"):
                        return str(t.get("id"))
                except Exception:
                    pass
                return ""

            # drafts 可能会延迟入库：轮询最多 60s
            drafts_wait_seconds = 60.0
            drafts_poll_interval = 3.0
            deadline = time.time() + drafts_wait_seconds
            draft_item: Optional[Dict[str, Any]] = None
            last_items_sample: list[str] = []
            attempt = 0
            while time.time() < deadline and draft_item is None:
                # 防止在 finalize 过程中被 idle_close 回收
                self._schedule_idle_close()
                attempt += 1
                drafts = await _sora_api_get_video_drafts_pw(
                    self.page,
                    target_url=target_url,
                    bearer_token=str(self.bearer_token),
                    limit=int(drafts_limit),
                    log_file=log_file,
                )
                items = drafts.get("items", []) if isinstance(drafts, dict) else []
                if not isinstance(items, list):
                    items = []

                # 采样一下 drafts 中的 task_id 方便排查
                last_items_sample = []
                for it in items[:20]:
                    if isinstance(it, dict):
                        tid = _get_item_task_id(it)
                        if tid:
                            last_items_sample.append(tid)

                for it in items:
                    if not isinstance(it, dict):
                        continue
                    if _get_item_task_id(it) == str(task_id):
                        draft_item = it
                        break

                _append_log(
                    log_file,
                    f"[sora][drafts] poll attempt={attempt} found={bool(draft_item)} items={len(items)} "
                    f"sample_task_ids={_safe_trim(','.join(last_items_sample), 260)!r}",
                )

                if draft_item is not None:
                    break
                await asyncio.sleep(float(drafts_poll_interval))

            if not draft_item:
                raise RuntimeError(
                    f"草稿箱未找到任务对应视频（已轮询 {drafts_wait_seconds:.0f}s）：task_id={task_id} "
                    f"sample_task_ids={_safe_trim(','.join(last_items_sample), 260)}"
                )

            generation_id = str(draft_item.get("id") or "").strip()
            if not generation_id:
                raise RuntimeError("草稿箱记录缺少 generation_id（draft_item.id）")

            # 发布草稿（去水印）
            post_resp = await _sora_api_post_project_y_post_pw(
                self.page,
                target_url=target_url,
                bearer_token=str(self.bearer_token),
                sentinel_token=str(self.sentinel_token),
                generation_id=generation_id,
                log_file=log_file,
            )
            post_id = ""
            try:
                post_id = str(((post_resp or {}).get("post") or {}).get("id") or "").strip()
            except Exception:
                post_id = ""
            if not post_id:
                raise RuntimeError(f"发布草稿失败：未返回 post_id resp={_safe_trim(json.dumps(post_resp, ensure_ascii=False), 600)}")

            share_url = f"https://sora.chatgpt.com/p/{post_id}"
            watermark_free_url = f"https://oscdn2.dyysy.com/MP4/{post_id}.mp4"

            return {
                "task_id": str(task_id),
                "generation_id": generation_id,
                "post_id": post_id,
                "share_url": share_url,
                "watermark_free_url": watermark_free_url,
                "draft": draft_item,
            }

    async def close(self) -> None:
        """关闭窗口与 driver（谨慎：会影响同窗口后续复用）。"""
        self._cancel_idle_close()
        t = self.monitor_task
        self.monitor_task = None
        if t and not t.done():
            t.cancel()

        try:
            await self.fp_client.browser_close(
                vendor=self.vendor,
                base_url=self.base_url,
                access_key=self.access_key,
                window_key=self.window_key,
            )
        except Exception as e:
            # 不要吞掉：close 卡住/超时会让人误判“ctx.close_and_drop 没返回”
            try:
                print(f"browser_close failed: {e}")
            except Exception:
                pass

        # 断开 Playwright 连接（如果指纹浏览器已关闭，这里也会自然失败，吞掉即可）
        br = self.browser
        self.browser = None
        self.context = None
        self.page = None
        try:
            if br is not None:
                await br.close()
        except Exception:
            pass

        pw = self.playwright
        self.playwright = None
        try:
            if pw is not None:
                await pw.stop()
        except Exception:
            pass


_CTX_LOCK = threading.Lock()
_SORA_CTXS: Dict[str, _SoraBrowserContext] = {}


def _ctx_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return "|".join([(vendor or "").strip().lower(), (base_url or "").strip().lower(), (space_id or "").strip(), (window_key or "").strip()])


def _drop_ctx(cache_key: str) -> None:
    k = (cache_key or "").strip()
    if not k:
        return
    with _CTX_LOCK:
        _SORA_CTXS.pop(k, None)


def _get_or_create_ctx(
    *,
    vendor: str,
    base_url: str,
    access_key: Optional[str],
    space_id: str,
    window_key: str,
) -> _SoraBrowserContext:
    k = _ctx_key(vendor, base_url, space_id, window_key)
    with _CTX_LOCK:
        ctx = _SORA_CTXS.get(k)
        if ctx is None:
            ctx = _SoraBrowserContext(
                cache_key=k,
                vendor=(vendor or "roxy").strip().lower(),
                base_url=(base_url or "").strip().rstrip("/"),
                access_key=access_key,
                space_id=(space_id or "").strip(),
                window_key=(window_key or "").strip(),
                fp_client=FPBrowserClient(),
            )
            _SORA_CTXS[k] = ctx
        else:
            ctx.access_key = access_key
        return ctx

