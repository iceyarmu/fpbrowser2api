"""Dreamina (dreamina.capcut.com) Seedance 视频生成执行器。

在已登录 dreamina.capcut.com 的指纹窗口内用 fetch(..., credentials: 'include') 调用官方接口，
复用浏览器 Cookie（ByteDance/TikTok 会话 Cookie）。

支持：
- 文生视频（t2v）
- 图生视频（i2v，payload.first_image_url）

入口：`dreamina_workflow`（由 `task_service._run_task` 调用）。

注意：API 端点基于逆向工程，如有变化请更新 _DREAMINA_SUBMIT_API / _DREAMINA_QUERY_API_TMPL。
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .playwright_broswer_context import (
    acquire_browser_open_slot,
    append_log,
    get_or_create_ctx as get_or_create_playwright_ctx,
    page_fetch_json,
    safe_trim,
)
from .veo_workflow_executor import (
    _build_debug_progress_panel_script,
    _short_err_msg,
    _veo_resolve_orientation_str,
)
from .task_executor_types import NonPenalizedTaskError, ProgressCB

# ---- API 端点（逆向工程，如有变化请更新） ----
_DREAMINA_BASE = "https://dreamina.capcut.com"
_DREAMINA_SUBMIT_API = f"{_DREAMINA_BASE}/api/v1/video/generate"
_DREAMINA_QUERY_API_TMPL = f"{_DREAMINA_BASE}/api/v1/task/{{task_id}}"

DEFAULT_DREAMINA_TARGET = "https://dreamina.capcut.com/ai-tool/video/generate"

# 模型名称（可通过 payload.model_name 覆盖）
_MODEL_T2V_PRO = "seedance_1_0_pro_t2v_250428"
_MODEL_I2V_PRO = "seedance_1_0_pro_i2v_250428"
_MODEL_T2V_LITE = "seedance_1_0_lite_t2v_250428"
_MODEL_I2V_LITE = "seedance_1_0_lite_i2v_250428"

# 任务状态码
_STATUS_SUCCESS = "success"
_STATUS_FAILED = "failed"
_STATUS_PROCESSING = "processing"
_STATUS_PENDING = "pending"

_DREAMINA_SESSIONS: Dict[str, "DreaminaSession"] = {}


def _dreamina_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return f"dreamina|{vendor}|{base_url}|{space_id}|{window_key}"


def _one_str(v: Any) -> str:
    return str(v or "").strip()


def _drop_dreamina_session(cache_key: str) -> None:
    k = (cache_key or "").strip()
    if k:
        _DREAMINA_SESSIONS.pop(k, None)


# ---- 参数解析 ----

def _dreamina_resolve_aspect_ratio(payload: Dict[str, Any]) -> str:
    p = payload or {}
    o = _veo_resolve_orientation_str(p)
    if o == "portrait":
        return "9:16"
    if o == "landscape":
        return "16:9"
    raw = _one_str(
        p.get("aspect_ratio") or p.get("size_ratio") or p.get("ratio") or p.get("size")
    )
    if raw in ("16:9", "9:16", "1:1", "4:3", "3:4", "21:9"):
        return raw
    return "16:9"


def _dreamina_resolve_duration(payload: Dict[str, Any]) -> int:
    p = payload or {}
    for k in ("duration", "seconds", "时长", "video_length"):
        try:
            v = int(float(p.get(k) or 0))
            if v > 0:
                return max(5, min(10, v))
        except (TypeError, ValueError):
            pass
    return 5


def _dreamina_resolve_model(payload: Dict[str, Any], has_image: bool) -> str:
    p = payload or {}
    explicit = _one_str(p.get("model_name") or p.get("model"))
    if explicit:
        return explicit
    quality = _one_str(p.get("quality") or p.get("tier") or "").lower()
    lite = quality in ("lite", "fast", "low", "标准")
    if has_image:
        return _MODEL_I2V_LITE if lite else _MODEL_I2V_PRO
    return _MODEL_T2V_LITE if lite else _MODEL_T2V_PRO


# ---- Cookie 注入 ----

async def _dreamina_merge_optional_cookies(page: Any, cookie_str: Optional[str], log_file: Path) -> None:
    """将 mapping 的 sora_access_token 字段（格式：name=value; name2=value2）注入浏览器 Context。"""
    raw = _one_str(cookie_str)
    if not raw:
        return
    ctx = getattr(page, "context", None)
    if ctx is None:
        return
    add = getattr(ctx, "add_cookies", None)
    if not callable(add):
        return
    cookies: List[Dict[str, Any]] = []
    for part in raw.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        name, _, value = part.partition("=")
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append({
            "name": name,
            "value": value,
            "domain": ".capcut.com",
            "path": "/",
            "secure": True,
            "sameSite": "Lax",
        })
    if not cookies:
        return
    try:
        await add(cookies)
        append_log(log_file, f"[dreamina] merged {len(cookies)} cookies for .capcut.com")
    except Exception as e:
        append_log(log_file, f"[dreamina] cookie merge failed (non-fatal): {e}")


# ---- API 调用 ----

def _dreamina_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
    }


async def _dreamina_submit_video(
    page: Any,
    *,
    prompt: str,
    model_name: str,
    aspect_ratio: str,
    duration: int,
    first_image_url: Optional[str],
    negative_prompt: str,
    log_file: Path,
) -> str:
    """提交视频生成任务，返回 task_id。"""
    body: Dict[str, Any] = {
        "prompt": prompt,
        "model_name": model_name,
        "aspect_ratio": aspect_ratio,
        "duration": duration,
        "negative_prompt": negative_prompt,
    }
    if first_image_url:
        body["first_frame_image"] = first_image_url

    tx = await page_fetch_json(
        page,
        url=_DREAMINA_SUBMIT_API,
        method="POST",
        headers=_dreamina_headers(),
        json_data=body,
        log_file=log_file,
    )
    obj = tx.get("_json")
    if not isinstance(obj, dict):
        raise NonPenalizedTaskError(
            f"Dreamina 提交失败：响应非 JSON body={safe_trim(str(tx), 400)}",
            status_code=502,
        )
    code = obj.get("code")
    if code != 0:
        msg = _one_str(obj.get("message") or obj.get("msg") or "")
        raise NonPenalizedTaskError(
            f"Dreamina 提交失败 code={code} msg={safe_trim(msg, 200)} body={safe_trim(json.dumps(obj, ensure_ascii=False), 400)}",
            status_code=502,
        )
    data = obj.get("data") or {}
    task_id = _one_str(data.get("task_id") or data.get("taskId") or data.get("id"))
    if not task_id:
        raise NonPenalizedTaskError(
            f"Dreamina 提交失败：无 task_id body={safe_trim(json.dumps(obj, ensure_ascii=False), 400)}",
            status_code=502,
        )
    return task_id


async def _dreamina_query_task(
    page: Any,
    *,
    task_id: str,
    log_file: Path,
) -> Dict[str, Any]:
    """查询任务状态，返回 data dict。"""
    url = _DREAMINA_QUERY_API_TMPL.format(task_id=task_id)
    tx = await page_fetch_json(
        page,
        url=url,
        method="GET",
        headers=_dreamina_headers(),
        json_data=None,
        log_file=log_file,
    )
    obj = tx.get("_json")
    if not isinstance(obj, dict):
        raise RuntimeError(f"Dreamina 查询失败：响应非 JSON body={safe_trim(str(tx), 400)}")
    code = obj.get("code")
    if code != 0:
        msg = _one_str(obj.get("message") or obj.get("msg") or "")
        raise RuntimeError(f"Dreamina 查询失败 code={code} msg={safe_trim(msg, 200)}")
    return obj.get("data") or {}


async def _dreamina_poll_until_done(
    page: Any,
    *,
    task_id: str,
    log_file: Path,
    timeout_seconds: float,
    progress_cb: ProgressCB,
    poll_interval: float = 3.0,
) -> str:
    """轮询直到任务完成，返回 video_url。"""
    deadline = time.time() + timeout_seconds
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            data = await _dreamina_query_task(page, task_id=task_id, log_file=log_file)
        except Exception as e:
            append_log(log_file, f"[dreamina] poll attempt={attempt} err={e}")
            await asyncio.sleep(poll_interval)
            continue

        status = _one_str(data.get("status") or data.get("state") or "").lower()
        append_log(log_file, f"[dreamina] poll attempt={attempt} status={status!r}")

        if status in (_STATUS_SUCCESS, "succeed", "done", "completed"):
            video_url = _one_str(
                data.get("video_url")
                or data.get("videoUrl")
                or data.get("url")
                or (data.get("video") or {}).get("url") if isinstance(data.get("video"), dict) else None
            )
            if not video_url:
                raise NonPenalizedTaskError(
                    f"Dreamina 任务完成但无 video_url data={safe_trim(json.dumps(data, ensure_ascii=False), 400)}",
                    status_code=502,
                )
            return video_url

        if status in (_STATUS_FAILED, "fail", "error"):
            err_msg = _one_str(data.get("error_message") or data.get("message") or data.get("msg") or "")
            raise NonPenalizedTaskError(
                f"Dreamina 任务失败 status={status} msg={safe_trim(err_msg, 200)}",
                status_code=502,
            )

        # 进度上报（30%~90% 区间）
        pct_raw = data.get("progress") or data.get("percent")
        try:
            pct = int(float(pct_raw or 0))
        except (TypeError, ValueError):
            pct = 0
        mapped = 30 + int(pct * 0.6) if pct > 0 else min(30 + attempt * 2, 88)
        await progress_cb(mapped, {"stage": "polling", "task_id": task_id, "status": status, "attempt": attempt})

        remain = deadline - time.time()
        if remain <= 0:
            break
        await asyncio.sleep(min(poll_interval, max(0.5, remain)))

    raise NonPenalizedTaskError(
        f"Dreamina 任务超时（{timeout_seconds:.0f}s）task_id={task_id}",
        status_code=504,
    )


# ---- Session 类 ----

class DreaminaSession:
    """按 window 缓存：复用指纹浏览器与 Playwright CDP（与 GrokSession 对齐）。"""

    def __init__(self, cache_key: str, pw_ctx: Any) -> None:
        self.cache_key = cache_key
        self.pw_ctx = pw_ctx

        self.last_used_at: float = time.time()
        self.create_lock = asyncio.Lock()
        self._bring_drafts_lock = asyncio.Lock()

        self.idle_close_task: Optional[asyncio.Task] = None
        self.idle_close_disabled: bool = False

        self.monitor_log_path: Optional[str] = None
        self.idle_close_seconds: float = 30.0

        self.browser_open_args: list[str] = []
        self.browser_force_open: bool = False
        self.browser_headless: bool = False
        self.browser_pure_mode: bool = True

        self.debug_panel_seq: int = 0
        self.debug_panel_entries: list[Dict[str, str]] = []

    @property
    def _log_file(self) -> Path:
        if self.monitor_log_path:
            return Path(self.monitor_log_path)
        return Path(__file__).resolve().parents[2] / "logs.txt"

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
        acquire_bring_lock: bool = False,
        pure_mode: Optional[bool] = None,
    ) -> None:
        self.last_used_at = time.time()
        pm = self.browser_pure_mode if pure_mode is None else bool(pure_mode)

        async def _inner() -> None:
            await self.pw_ctx.ensure_open(
                args=args, force_open=force_open, headless=headless, require_page=False, pure_mode=pm
            )

        if acquire_bring_lock:
            async with self._bring_drafts_lock:
                await _inner()
        else:
            await _inner()

    async def disconnect_playwright_under_bring_lock(self) -> None:
        async with self._bring_drafts_lock:
            async with self.pw_ctx.driver_lock:
                await self.pw_ctx.disconnect_playwright_only()

    async def _push_debug_progress(self, page: Any, text: str, *, level: str = "info") -> None:
        if page is None:
            return
        try:
            msg = str(text or "").strip()
        except Exception:
            msg = ""
        if not msg:
            return
        self.debug_panel_seq += 1
        now_str = time.strftime("%H:%M:%S")
        self.debug_panel_entries.append(
            {"idx": str(self.debug_panel_seq), "ts": now_str, "level": str(level or "info"), "text": msg}
        )
        if len(self.debug_panel_entries) > 80:
            self.debug_panel_entries = self.debug_panel_entries[-80:]
        payload = {
            "title": "Dreamina 调试进度",
            "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
            "entries": list(self.debug_panel_entries),
        }
        script = _build_debug_progress_panel_script()
        try:
            await page.evaluate(script, payload)
        except Exception:
            pass

    @staticmethod
    def _is_page_closed(p: Any) -> bool:
        try:
            return bool(getattr(p, "is_closed", lambda: False)())
        except Exception:
            return True

    async def _bring_target_page_to_front(
        self,
        refresh_target: bool = True,
        *,
        drafts_url: str,
        acquire_bring_lock: bool = True,
    ) -> None:
        """将 Dreamina 目标页置前（与 GrokSession._bring_target_page_to_front 同源）。"""
        try:
            target_host = urlparse(drafts_url).netloc.strip().lower()
        except Exception:
            target_host = ""

        async def _inner() -> None:
            ctx0 = getattr(self.pw_ctx, "context", None)
            br0 = getattr(self.pw_ctx, "browser", None)
            try:
                ctxs = list(getattr(br0, "contexts", []) or [])
            except Exception:
                ctxs = []
            if ctx0 is not None and ctx0 not in ctxs:
                ctxs.insert(0, ctx0)
            if not ctxs:
                return

            def _safe_url(p: Any) -> str:
                try:
                    return str(getattr(p, "url", "") or "").strip()
                except Exception:
                    return ""

            def _url_matches(u: str) -> bool:
                if u.startswith(drafts_url):
                    return True
                try:
                    h = (urlparse(u).netloc or "").strip().lower()
                except Exception:
                    h = ""
                return bool(target_host and h == target_host)

            # 找目标页
            target_page = None
            cur = getattr(self.pw_ctx, "page", None)
            if cur is not None and not self._is_page_closed(cur) and _url_matches(_safe_url(cur)):
                target_page = cur

            if target_page is None:
                for c in ctxs:
                    try:
                        pages = list(getattr(c, "pages", []) or [])
                    except Exception:
                        pages = []
                    for p in pages:
                        if self._is_page_closed(p):
                            continue
                        if _url_matches(_safe_url(p)):
                            target_page = p
                            break
                    if target_page:
                        break

            if target_page is None:
                ctx_pref = ctx0 or (ctxs[0] if ctxs else None)
                if ctx_pref is None:
                    return
                try:
                    target_page = await ctx_pref.new_page()
                    await target_page.goto(drafts_url, wait_until="domcontentloaded")
                except Exception:
                    return

            # 关闭其它页面
            for c in ctxs:
                try:
                    pages = list(getattr(c, "pages", []) or [])
                except Exception:
                    pages = []
                for p in pages:
                    if p is target_page:
                        continue
                    try:
                        await p.close()
                    except Exception:
                        pass

            try:
                self.pw_ctx.page = target_page
            except Exception:
                pass
            try:
                await target_page.bring_to_front()
            except Exception:
                pass

            if refresh_target:
                try:
                    await target_page.goto(drafts_url, wait_until="domcontentloaded")
                    await self._push_debug_progress(target_page, "Dreamina 目标页面刷新完成", level="ok")
                except Exception:
                    await self._push_debug_progress(target_page, "Dreamina 目标页面刷新失败（将继续）", level="warn")
                try:
                    await target_page.evaluate("() => { try { window.focus(); } catch(e) {} }")
                except Exception:
                    pass
                await asyncio.sleep(2.0)

            # 检查是否在 Dreamina 域名下
            try:
                u = _safe_url(target_page).lower()
                if "capcut.com" not in u and "dreamina" not in u:
                    raise NonPenalizedTaskError(
                        "当前页面不在 Dreamina 域名（capcut.com）下，请先在指纹窗口登录 Dreamina",
                        status_code=401,
                    )
            except NonPenalizedTaskError:
                raise
            except Exception:
                pass

        if acquire_bring_lock:
            async with self._bring_drafts_lock:
                await _inner()
        else:
            await _inner()

    def _cancel_idle_close(self) -> None:
        t = self.idle_close_task
        self.idle_close_task = None
        if t and not t.done():
            try:
                cur = asyncio.current_task()
            except Exception:
                cur = None
            if cur is not None and t is cur:
                return
            t.cancel()

    def _schedule_idle_close(self) -> None:
        if bool(self.idle_close_disabled):
            self._cancel_idle_close()
            return
        self._cancel_idle_close()

        async def _job() -> None:
            try:
                secs = max(0.0, float(self.idle_close_seconds))
                if secs <= 0:
                    return
                await asyncio.sleep(secs)
                if bool(self.idle_close_disabled):
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
        _drop_dreamina_session(self.cache_key)

    async def close(self) -> None:
        self._cancel_idle_close()
        await self.pw_ctx.close_and_drop()


def get_or_create_dreamina_session(
    *,
    vendor: str,
    base_url: str,
    access_key: Optional[str],
    space_id: str,
    window_key: str,
) -> DreaminaSession:
    k = _dreamina_key(vendor, base_url, space_id, window_key)
    sess = _DREAMINA_SESSIONS.get(k)
    if sess is None:
        pw_ctx = get_or_create_playwright_ctx(
            vendor=vendor,
            base_url=base_url,
            access_key=access_key,
            space_id=space_id,
            window_key=window_key,
        )
        sess = DreaminaSession(cache_key=k, pw_ctx=pw_ctx)
        _DREAMINA_SESSIONS[k] = sess
    else:
        sess.pw_ctx.access_key = access_key
    return sess


# ---- 主入口 ----

async def dreamina_workflow(
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
    default_target_url: Optional[str] = None,
    headless: bool = False,
    access_token: Optional[str] = None,
    access_expires: Optional[str] = None,
    db: Any = None,
    task_type_window_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Dreamina Seedance 视频生成：指纹窗口内 fetch（Cookie 鉴权）。"""
    del db, task_type_window_id, access_expires

    p = dict(payload or {})
    prompt = _one_str(p.get("prompt"))
    if not prompt:
        raise NonPenalizedTaskError("payload.prompt 不能为空", status_code=400)

    first_image_url = _one_str(p.get("first_image_url") or p.get("image_url") or p.get("first_frame_image")) or None
    negative_prompt = _one_str(p.get("negative_prompt") or "")
    aspect_ratio = _dreamina_resolve_aspect_ratio(p)
    duration = _dreamina_resolve_duration(p)
    model_name = _dreamina_resolve_model(p, has_image=bool(first_image_url))
    monitor_log_path = _one_str(p.get("monitor_log_path")) or None
    target_page = (
        _one_str(default_target_url)
        or _one_str(p.get("dreamina_url") or p.get("target_url"))
        or DEFAULT_DREAMINA_TARGET
    )

    sess = get_or_create_dreamina_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    sess.browser_headless = headless
    sess.monitor_log_path = monitor_log_path
    sess.idle_close_seconds = float(p.get("ctx_idle_close_seconds") or 30.0)

    log_file = sess._log_file
    started = time.time()

    video_mode = "i2v" if first_image_url else "t2v"
    await progress_cb(
        2,
        {
            "stage": "init",
            "workflow_kind": "video",
            "video_mode": video_mode,
            "prompt": safe_trim(prompt, 200),
            "model_name": model_name,
            "aspect_ratio": aspect_ratio,
            "duration": duration,
        },
    )

    async with sess.create_lock:
        try:
            sess.idle_close_disabled = True
            sess._cancel_idle_close()
            await sess.ensure_open(
                args=sess.browser_open_args,
                force_open=sess.browser_force_open,
                headless=headless,
            )
            append_log(log_file, f"[dreamina] ensure_open ok target={safe_trim(target_page, 120)!r}")

            await sess._bring_target_page_to_front(refresh_target=True, drafts_url=target_page)
            await progress_cb(8, {"stage": "navigate", "url": target_page})
            append_log(log_file, "[dreamina] _bring_target_page_to_front completed")

            page = sess.pw_ctx.page
            if page is None:
                raise NonPenalizedTaskError("Dreamina 目标页面未初始化（pw_ctx.page 为空）", status_code=502)

            try:
                await asyncio.sleep(float(p.get("dreamina_post_nav_sleep_seconds") or 1.5))
            except Exception:
                pass

            # 可选 Cookie 注入
            cookie_raw = _one_str(p.get("dreamina_cookies") or p.get("access_token") or "") or _one_str(access_token or "")
            if cookie_raw:
                await _dreamina_merge_optional_cookies(page, cookie_raw, log_file)
                await progress_cb(9, {"stage": "cookies", "applied": True})
            else:
                await progress_cb(9, {"stage": "cookies", "applied": False})

            # 提交任务
            await progress_cb(10, {"stage": "submit", "model_name": model_name})
            task_id = await _dreamina_submit_video(
                page,
                prompt=prompt,
                model_name=model_name,
                aspect_ratio=aspect_ratio,
                duration=duration,
                first_image_url=first_image_url,
                negative_prompt=negative_prompt,
                log_file=log_file,
            )
            append_log(log_file, f"[dreamina] submitted task_id={safe_trim(task_id, 64)!r}")
            await progress_cb(20, {"stage": "submitted", "task_id": task_id})

            # 轮询
            poll_timeout = max(30.0, timeout_seconds - (time.time() - started) - 5.0)
            video_url = await _dreamina_poll_until_done(
                page,
                task_id=task_id,
                log_file=log_file,
                timeout_seconds=poll_timeout,
                progress_cb=progress_cb,
                poll_interval=float(p.get("dreamina_poll_interval") or 3.0),
            )
            append_log(log_file, f"[dreamina] video_url={safe_trim(video_url, 120)!r}")

            elapsed_ms = int(max(0.0, (time.time() - started) * 1000.0))
            await progress_cb(100, {"stage": "done", "video_url": video_url, "elapsed_ms": elapsed_ms})

            return {
                "type": "dreamina_workflow_video",
                "message": "Dreamina 图生视频完成" if first_image_url else "Dreamina 文生视频完成",
                "video_url": video_url,
                "workflow_kind": "video",
                "video_mode": video_mode,
                "model_name": model_name,
                "aspect_ratio": aspect_ratio,
                "duration": duration,
                "task_id": task_id,
                "elapsed_ms": elapsed_ms,
            }
        finally:
            try:
                await sess.disconnect_playwright_under_bring_lock()
            except Exception:
                pass


async def dreamina_admin_open_connect_page(
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    headless: bool = False,
    default_target_url: Optional[str] = None,
    pure_mode: bool = True,
    timeout_seconds: float = 120.0,
) -> Dict[str, Any]:
    """管理台：打开 Dreamina 页面并断开 CDP，供用户手动登录。"""
    del timeout_seconds
    sess = get_or_create_dreamina_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    sess.browser_headless = headless
    sess.browser_pure_mode = pure_mode
    sess.idle_close_disabled = True
    sess._cancel_idle_close()
    url = _one_str(default_target_url) or DEFAULT_DREAMINA_TARGET
    await sess.ensure_open(
        args=sess.browser_open_args,
        force_open=sess.browser_force_open,
        headless=headless,
        pure_mode=pure_mode,
    )
    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=url)
    try:
        await sess.disconnect_playwright_under_bring_lock()
    except Exception:
        pass
    return {
        "message": "已打开 Dreamina 页面并断开 CDP；请在本窗口登录 ByteDance/TikTok 账号。视频任务默认使用窗口 Cookie；可选在 mapping 的 access_token 列保存 Cookie 串（格式：name=value; name2=value2），执行前会注入到 .capcut.com 域。",
        "url": url,
    }
