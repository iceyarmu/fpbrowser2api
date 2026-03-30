"""VEO 视频生成工作流执行器。

职责边界：
- `playwright_broswer_context.py`：通用"指纹浏览器自动化层"（开窗/连CDP/挑页/page内fetch）
- `veo_workflow_executor.py`：VEO 站点侧逻辑（打开页面、提交生成任务、轮询进度、获取结果）

入口：
- `veo_workflow`（由 `task_service.py` 发起调用）
"""

from __future__ import annotations

import asyncio
import base64
import random
import re
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

from ..core.config import config as app_config
from ..core.database import Database
from .playwright_broswer_context import (
    PlaywrightBrowserContext,
    acquire_browser_open_slot,
    append_log,
    get_or_create_ctx as get_or_create_playwright_ctx,
    page_fetch_json,
    page_fetch_tx,
    page_resolve_redirect_url,
    pick_working_page_from_context,
    safe_trim,
)
from .sora_task_executor import (
    _download_bytes_local_async,
    _pick_n_frames,
    _prepare_first_frame_image_for_upload_async,
)
from .oss_uploader import build_veo_upsample_object_key, oss_config_from_setting_section, upload_bytes_to_oss
from .task_executor_types import NonPenalizedTaskError, ProgressCB


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _short_err_msg(err: Any, *, max_len: int = 120) -> str:
    try:
        s = str(err or "").strip()
    except Exception:
        s = ""
    if not s:
        return ""
    if len(s) <= max_len:
        return s
    return s[: max(10, max_len - 3)] + "..."


def _build_debug_progress_panel_script() -> str:
    """返回调试进度面板注入脚本（单实例，重复调用只更新内容）。"""
    return r"""
(payload) => {
  try {
    const PANEL_ID = "__veo_debug_progress_panel__";
    const STYLE_ID = "__veo_debug_progress_panel_style__";
    const safe = (v) => (v === null || v === undefined) ? "" : String(v);
    const data = {
      title: safe(payload && payload.title),
      updatedAt: safe(payload && payload.updatedAt),
      entries: Array.isArray(payload && payload.entries) ? payload.entries.map((x) => ({
        idx: safe(x && x.idx),
        ts: safe(x && x.ts),
        level: safe(x && x.level),
        text: safe(x && x.text),
      })) : [],
    };

    if (!document.getElementById(STYLE_ID)) {
      const style = document.createElement("style");
      style.id = STYLE_ID;
      style.textContent = `
#${PANEL_ID}{
position:fixed;top:16px;right:16px;z-index:2147483647;width:420px;
background:rgba(15,23,42,.95);color:#e5e7eb;border:1px solid rgba(148,163,184,.35);
border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.35);font-size:12px;font-family:Arial,sans-serif;
}
#${PANEL_ID}.min{width:220px}
#${PANEL_ID} .hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(148,163,184,.25)}
#${PANEL_ID} .ttl{font-weight:700;color:#f8fafc}
#${PANEL_ID} .btn{background:#334155;color:#f8fafc;border:0;border-radius:8px;padding:4px 8px;cursor:pointer}
#${PANEL_ID} .bd{padding:10px 12px;max-height:55vh;overflow:auto}
#${PANEL_ID} .it{padding:7px 8px;margin-bottom:6px;border-radius:8px;background:rgba(30,41,59,.75)}
#${PANEL_ID} .it.ok{border-left:3px solid #22c55e}
#${PANEL_ID} .it.warn{border-left:3px solid #f59e0b}
#${PANEL_ID} .it.err{border-left:3px solid #ef4444}
#${PANEL_ID} .meta{font-size:11px;color:#94a3b8;margin-bottom:3px}
#${PANEL_ID} .txt{white-space:pre-wrap;word-break:break-word;line-height:1.4}
#${PANEL_ID} .fts{padding:8px 12px;border-top:1px solid rgba(148,163,184,.2);color:#94a3b8}
      `.trim();
      document.documentElement.appendChild(style);
    }

    let panel = document.getElementById(PANEL_ID);
    if (!panel) {
      panel = document.createElement("div");
      panel.id = PANEL_ID;
      panel.innerHTML = `
        <div class="hdr">
          <span class="ttl"></span>
          <button class="btn tg" type="button">收起</button>
        </div>
        <div class="bd"></div>
        <div class="fts"></div>
      `;
      document.documentElement.appendChild(panel);
    }

    const ttl = panel.querySelector(".ttl");
    const bd = panel.querySelector(".bd");
    const fts = panel.querySelector(".fts");
    const tg = panel.querySelector(".tg");
    if (!ttl || !bd || !fts || !tg) return;

    ttl.textContent = data.title || "VEO 调试进度";
    bd.innerHTML = "";
    for (const e of data.entries) {
      const item = document.createElement("div");
      const lv = (e.level || "info").toLowerCase();
      let cls = "it";
      if (lv === "ok" || lv === "success") cls += " ok";
      else if (lv === "warn" || lv === "warning") cls += " warn";
      else if (lv === "err" || lv === "error" || lv === "fail") cls += " err";
      item.className = cls;

      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = `#${safe(e.idx)}  ${safe(e.ts)}  [${lv || "info"}]`;

      const txt = document.createElement("div");
      txt.className = "txt";
      txt.textContent = safe(e.text);

      item.appendChild(meta);
      item.appendChild(txt);
      bd.appendChild(item);
    }

    fts.textContent = data.updatedAt ? `更新时间: ${data.updatedAt}` : "";
    try {
      bd.scrollTop = bd.scrollHeight;
    } catch (_e1) {}

    tg.onclick = () => {
      const min = panel.classList.toggle("min");
      bd.style.display = min ? "none" : "block";
      fts.style.display = min ? "none" : "block";
      tg.textContent = min ? "展开" : "收起";
    };
  } catch (_e) {
    // 忽略注入失败，避免影响主流程。
  }
}
"""


# ---------------------------------------------------------------------------
# VEO Session：按 window 维度缓存
# ---------------------------------------------------------------------------
_VEO_SESSIONS: Dict[str, "VeoSession"] = {}


def _veo_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return f"veo|{vendor}|{base_url}|{space_id}|{window_key}"


def _drop_veo_session(cache_key: str) -> None:
    k = (cache_key or "").strip()
    if not k:
        return
    _VEO_SESSIONS.pop(k, None)


class VeoSession:
    """按 window 维度缓存的 VEO 会话。

    复用同一个指纹浏览器窗口与 Playwright CDP 连接。
    """

    def __init__(self, cache_key: str, pw_ctx: PlaywrightBrowserContext) -> None:
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
    ) -> None:
        """确保指纹浏览器窗口已打开、CDP 已连接。"""
        self.last_used_at = time.time()
        # 串行化窗口 open/close：避免并发 ensure_open 与 Cloudflare 自愈重启产生竞态。
        async def _inner() -> None:
            await self.pw_ctx.ensure_open(args=args, force_open=force_open, headless=headless, require_page=False)
        if acquire_bring_lock:
            async with self._bring_drafts_lock:
                await _inner()
        else:
            await _inner()

    async def disconnect_playwright_under_bring_lock(self) -> None:
        """先占 _bring_drafts_lock，再占 pw_ctx.driver_lock，再断开 CDP（与 bring 及仅持 driver_lock 的页面逻辑互斥）。"""
        async with self._bring_drafts_lock:
            async with self.pw_ctx.driver_lock:
                await self.pw_ctx.disconnect_playwright_only()

    async def navigate_to(self, url: str, *, timeout_ms: int = 60_000) -> None:
        """导航到指定 URL。"""
        if self.pw_ctx.page is None:
            raise RuntimeError("page 未初始化，请先调用 ensure_open")
        try:
            await self.pw_ctx.page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception as e:
            raise NonPenalizedTaskError(f"打开 VEO 页面失败：{e}", status_code=400) from e

    async def page_fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """在浏览器页面上下文中发起 fetch 请求（走指纹浏览器网络栈）。"""
        if self.pw_ctx.page is None:
            raise RuntimeError("page 未初始化，请先调用 ensure_open")
        return await page_fetch_json(
            self.pw_ctx.page,
            url=url,
            method=method,
            headers=headers or {"Accept": "application/json", "Content-Type": "application/json"},
            json_data=json_data,
            log_file=self._log_file,
        )

    # ------------------------------------------------------------------
    # 调试面板
    # ------------------------------------------------------------------
    async def _push_debug_progress(self, page: Any, text: str, *, level: str = "info") -> None:
        """向页面插件弹窗写入调试步骤；同一页面始终复用单个面板。"""
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
            {
                "idx": str(self.debug_panel_seq),
                "ts": now_str,
                "level": str(level or "info"),
                "text": msg,
            }
        )
        if len(self.debug_panel_entries) > 80:
            self.debug_panel_entries = self.debug_panel_entries[-80:]
        payload = {
            "title": "VEO 调试进度",
            "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
            "entries": list(self.debug_panel_entries),
        }
        script = _build_debug_progress_panel_script()
        try:
            await page.evaluate(script, payload)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Cloudflare 检测 & 自愈
    # ------------------------------------------------------------------
    async def _is_cloudflare_page(self, page, *, deep: bool = False) -> bool:
        """判断当前页面是否为 Cloudflare 拦截/挑战页。"""
        if page is None:
            return False
        try:
            u = str(getattr(page, "url", "") or "").strip()
        except Exception:
            u = ""
        ul = u.lower()
        if "/cdn-cgi/" in ul:
            return True
        try:
            title = await page.title()
        except Exception:
            title = ""
        tl = (title or "").strip().lower()
        if "just a moment" in tl or "attention required" in tl:
            return True
        if not deep:
            return False
        try:
            html = await page.content()
        except Exception:
            html = ""
        hl = (html or "").lower()
        if "cloudflare" in hl and ("just a moment" in hl or "/cdn-cgi/" in hl or "cf-ray" in hl):
            return True
        if ("turnstile" in hl or "cf-challenge" in hl) and ("/cdn-cgi/" in hl or "cloudflare" in hl):
            return True
        return False

    async def raise_if_cloudflare_page_nonpenalized(
        self, page, *, stage: str, target_url: str
    ) -> None:
        """与 Sora `_raise_if_cloudflare_page_nonpenalized` 同类：bring 目标页 + 等待/重启，仍判定 CF 则抛 NonPenalizedTaskError（用于窗口池巡检）。"""
        async with self._bring_drafts_lock:
            if page is None:
                return
            try:
                th = (urlparse(target_url).netloc or "").strip().lower()
            except Exception:
                th = ""
            cur = page
            for _ in range(2):
                if cur is None:
                    return
                if not await self._is_cloudflare_page(cur, deep=False):
                    return
                await self._bring_target_page_to_front(
                    refresh_target=False, drafts_url=target_url, acquire_bring_lock=False
                )
                cur = self.pw_ctx.page
                if cur is None:
                    return
            if cur is not None and await self._is_cloudflare_page(cur, deep=False):
                still = await self._wait_cloudflare_auto_pass(
                    cur,
                    max_wait_seconds=25.0,
                    max_success_clicks=2,
                )
                if still and await self._is_cloudflare_page(cur, deep=True):
                    await self._restart_window_and_restore_single_drafts(
                        drafts_url=target_url, target_host=th
                    )
                    cur = self.pw_ctx.page
            if cur is not None and await self._is_cloudflare_page(cur, deep=True):
                raise NonPenalizedTaskError(
                    f"当前页面为 Cloudflare 验证/拦截页，无法继续：{stage}",
                    status_code=503,
                )

    async def _try_click_cloudflare_checkbox(self, page) -> bool:
        """尝试点击 Cloudflare Turnstile challenge 的 checkbox。

        策略：
        1. frame.locator()：CDP 原生可穿透 shadow-root，尝试直接点击
        2. 坐标法：frame_element().bounding_box() 拿到 iframe 屏幕位置，
           模拟拟人化鼠标移动后 mouse.click() 点击 checkbox 坐标
        """
        log_file = (
            Path(self.monitor_log_path)
            if self.monitor_log_path
            else (Path(__file__).resolve().parents[2] / "logs.txt")
        )

        def _log(msg: str) -> None:
            try:
                append_log(log_file, f"[cf_checkbox] {msg}")
            except Exception:
                pass

        try:
            cf_frame = None
            try:
                for i, f in enumerate(page.frames):
                    fu = str(getattr(f, "url", "") or "")
                    if "challenges.cloudflare.com" in fu or "/cdn-cgi/" in fu:
                        cf_frame = f
                        _log(f"找到 cf_frame: frame[{i}]")
                        break
            except Exception as e:
                _log(f"遍历 page.frames 异常: {e}")

            if cf_frame is None:
                await self._push_debug_progress(page, "未发现 Cloudflare checkbox iframe", level="warn")
                return False

            # 策略1：frame.locator()（CDP 可穿 closed shadow-root）
            try:
                loc = cf_frame.locator("input[type='checkbox']")
                cnt = await loc.count()
                _log(f"策略1 locator count={cnt}")
                if cnt > 0:
                    await self._push_debug_progress(page, "发现了 checkbox（locator）", level="info")
                    await loc.first.click(force=True, timeout=1500)
                    _log("策略1 locator click 成功")
                    await self._push_debug_progress(page, "点击 checkbox 成功（locator）", level="ok")
                    return True
            except Exception as e:
                _log(f"策略1 locator 失败: {e}")
                await self._push_debug_progress(page, f"点击 checkbox 失败（locator）：{_short_err_msg(e)}", level="warn")

            # 策略2：坐标法 + 拟人化鼠标移动
            try:
                iframe_handle = await cf_frame.frame_element()
                box = await iframe_handle.bounding_box()
                if not (box and box.get("width", 0) > 0 and box.get("height", 0) > 0):
                    _log(f"策略2 bounding_box 无效: {box}")
                    return False

                target_x = box["x"] + 26.0
                target_y = box["y"] + box["height"] / 2.0
                _log(f"策略2 目标坐标: ({target_x:.1f}, {target_y:.1f})")
                await self._push_debug_progress(page, "发现了 checkbox（坐标法）", level="info")

                start_x = target_x + random.uniform(-80, 120)
                start_y = target_y + random.uniform(-50, 50)
                await page.mouse.move(start_x, start_y)
                await asyncio.sleep(random.uniform(0.08, 0.20))
                await page.mouse.move(target_x, target_y, steps=random.randint(6, 12))
                await asyncio.sleep(random.uniform(0.04, 0.12))
                await page.mouse.click(target_x, target_y)
                _log("策略2 坐标点击完成")
                await self._push_debug_progress(page, "点击 checkbox 成功（坐标法）", level="ok")
                return True
            except Exception as e:
                _log(f"策略2 坐标法异常: {e}")
                await self._push_debug_progress(page, f"点击 checkbox 失败（坐标法）：{_short_err_msg(e)}", level="warn")

        except Exception as e:
            _log(f"顶层异常: {e}")

        _log("所有策略均未成功点击 checkbox")
        await self._push_debug_progress(page, "checkbox 点击失败：所有策略已尝试", level="error")
        return False

    async def _wait_cloudflare_auto_pass(
        self,
        page,
        *,
        max_wait_seconds: float = 10.0,
        max_success_clicks: int = 2,
    ) -> bool:
        """等待 Cloudflare 可能自动放行，同时尝试点击 Turnstile checkbox。

        返回：
        - True: 超时后仍像 Cloudflare（可考虑重启）
        - False: 已不再像 Cloudflare（无需重启）
        """
        try:
            deadline = time.time() + max(0.0, float(max_wait_seconds))
        except Exception:
            deadline = time.time() + 10.0
        await asyncio.sleep(5.0)
        try:
            max_click_success = max(0, int(max_success_clicks))
        except Exception:
            max_click_success = 2
        poll_after_click = 6.0
        poll_idle = 1.0
        await self._push_debug_progress(page, "检测到 Cloudflare，开始等待自动放行并尝试点击 checkbox", level="warn")
        reported_click_fail = False
        consecutive_not_cf = 0
        clicked_success_count = 0
        while time.time() < deadline:
            try:
                is_closed = bool(getattr(page, "is_closed", lambda: False)())
            except Exception:
                is_closed = False
            if is_closed:
                return True

            try:
                still_cf = await self._is_cloudflare_page(page, deep=True)
            except Exception:
                still_cf = True
            if not still_cf:
                consecutive_not_cf += 1
                if consecutive_not_cf >= 2:
                    await self._push_debug_progress(page, "Cloudflare 已放行", level="ok")
                    return False
                await self._push_debug_progress(page, "Cloudflare 疑似已放行，进行二次确认", level="info")
                remain = deadline - time.time()
                if remain <= 0:
                    break
                try:
                    await asyncio.sleep(min(poll_idle, max(0.1, remain)))
                except Exception:
                    break
                continue
            consecutive_not_cf = 0

            clicked = False
            try:
                clicked = await self._try_click_cloudflare_checkbox(page)
            except Exception:
                pass
            if clicked:
                clicked_success_count += 1
                await self._push_debug_progress(
                    page,
                    f"checkbox 已点击（第 {clicked_success_count} 次），等待 Cloudflare 验证结果",
                    level="info",
                )
                if max_click_success > 0 and clicked_success_count >= max_click_success:
                    await self._push_debug_progress(
                        page,
                        f"checkbox 成功点击已达上限（{max_click_success} 次），提前结束等待",
                        level="warn",
                    )
                    try:
                        await asyncio.sleep(1.0)
                    except Exception:
                        pass
                    try:
                        still_cf_after_limit = await self._is_cloudflare_page(page, deep=True)
                    except Exception:
                        still_cf_after_limit = True
                    if not still_cf_after_limit:
                        await self._push_debug_progress(page, "Cloudflare 已放行", level="ok")
                        return False
                    return True
            elif not reported_click_fail:
                await self._push_debug_progress(page, "尚未成功点击 checkbox，继续重试", level="warn")
                reported_click_fail = True

            remain = deadline - time.time()
            if remain <= 0:
                break
            sleep_sec = poll_after_click if clicked else poll_idle
            try:
                await asyncio.sleep(min(sleep_sec, max(0.1, remain)))
            except Exception:
                break
        return True

    async def _restart_window_and_restore_single_drafts(self, *, drafts_url: str, target_host: str) -> Any:
        """关闭并重开指纹浏览器窗口（仅打开窗口，不连接 CDP/不查找页面）。"""
        log_file = (
            Path(self.monitor_log_path)
            if self.monitor_log_path
            else (Path(__file__).resolve().parents[2] / "logs.txt")
        )
        try:
            append_log(log_file, "[veo][drafts] detected cloudflare interstitial, restarting fp window once")
        except Exception:
            pass

        try:
            await self.pw_ctx.close()
        except Exception:
            pass
        try:
            await asyncio.sleep(0.5)
        except Exception:
            pass

        try:
            append_log(log_file, "[veo][drafts] reopen window only: skip cdp connect/page probing")
        except Exception:
            pass

        async with acquire_browser_open_slot(self.pw_ctx.base_url):
            try:
                rsp = await self.pw_ctx.fp_client.browser_open(
                    vendor=self.pw_ctx.vendor,
                    base_url=self.pw_ctx.base_url,
                    access_key=self.pw_ctx.access_key,
                    space_id=self.pw_ctx.space_id,
                    window_key=self.pw_ctx.window_key,
                    args=self.browser_open_args,
                    force_open=self.browser_force_open,
                    headless=self.browser_headless,
                )
                try:
                    code = int((rsp or {}).get("code", -1))
                except Exception:
                    code = -1
                try:
                    append_log(log_file, f"[veo][drafts] browser_open result code={code}")
                except Exception:
                    pass
            except Exception as e:
                try:
                    append_log(log_file, f"[veo][drafts] browser_open failed: {e}")
                except Exception:
                    pass

            try:
                self.pw_ctx.browser = None
                self.pw_ctx.context = None
                self.pw_ctx.page = None
                self.pw_ctx.cdp_endpoint = None
            except Exception:
                pass

            try:
                await asyncio.sleep(20.0)
            except Exception:
                pass

        try:
            await self.pw_ctx.ensure_open(
                args=self.browser_open_args,
                force_open=False,
                headless=self.browser_headless,
                require_page=False,
            )
        except Exception as e:
            try:
                append_log(log_file, f"[veo][drafts] CDP reconnect after restart failed: {e}")
            except Exception:
                pass
        return None

    # ------------------------------------------------------------------
    # Login 按钮检测 & 点击
    # ------------------------------------------------------------------
    async def _maybe_click_login_button_if_prompted(self, page) -> tuple:
        has_login_button = False
        """尝试点击页面上的 Log in 按钮/链接（不依赖固定提示文案）。"""
        if page is None:
            return False, has_login_button

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=4000)
        except Exception:
            pass

        scopes: list[Any] = [page]
        try:
            for fr in list(getattr(page, "frames", []) or []):
                if fr is not page and fr not in scopes:
                    scopes.append(fr)
        except Exception:
            pass

        login_name_re = re.compile(r"log\s*in", re.IGNORECASE)

        try:
            for sc in scopes:
                try:
                    if hasattr(sc, "get_by_role"):
                        btn_cnt = await sc.get_by_role("button", name=login_name_re).count()
                        link_cnt = await sc.get_by_role("link", name=login_name_re).count()
                        if (btn_cnt + link_cnt) > 0:
                            has_login_button = True
                            break
                    loc_probe = sc.locator('button, a, [role="button"], [role="link"]').filter(has_text=login_name_re)
                    if (await loc_probe.count()) > 0:
                        has_login_button = True
                        break
                except Exception:
                    continue
        except Exception:
            has_login_button = False

        if not has_login_button:
            await self._push_debug_progress(page, "未发现 Log in 按钮/链接", level="info")
            return False, has_login_button
        await self._push_debug_progress(page, "发现 Log in 按钮/链接，准备点击", level="info")

        for sc in scopes:
            try:
                scope_name = "page" if sc is page else "frame"
                if hasattr(sc, "get_by_role"):
                    try:
                        btn = sc.get_by_role("button", name=login_name_re)
                        await btn.first.click(timeout=3000)
                        await self._push_debug_progress(page, f"点击 Log in 成功（button/{scope_name}）", level="ok")
                        return True, has_login_button
                    except Exception as e:
                        await self._push_debug_progress(page, f"点击 Log in 失败（button/{scope_name}）：{_short_err_msg(e)}", level="warn")
                    try:
                        link = sc.get_by_role("link", name=login_name_re)
                        await link.first.click(timeout=3000)
                        await self._push_debug_progress(page, f"点击 Log in 成功（link/{scope_name}）", level="ok")
                        return True, has_login_button
                    except Exception as e:
                        await self._push_debug_progress(page, f"点击 Log in 失败（link/{scope_name}）：{_short_err_msg(e)}", level="warn")

                try:
                    loc2 = sc.locator('button, a, [role="button"], [role="link"]').filter(has_text=login_name_re)
                    await loc2.first.click(timeout=3000)
                    await self._push_debug_progress(page, f"点击 Log in 成功（text fallback/{scope_name}）", level="ok")
                    return True, has_login_button
                except Exception as e:
                    await self._push_debug_progress(page, f"点击 Log in 失败（text fallback/{scope_name}）：{_short_err_msg(e)}", level="warn")
            except Exception:
                continue

        await self._push_debug_progress(page, "点击 Log in 失败（全部策略）", level="error")
        return False, has_login_button

    # ------------------------------------------------------------------
    # 核心：将目标页面置前（完整照搬 sora 的 _bring_sora_drafts_to_front）
    # ------------------------------------------------------------------
    async def _bring_target_page_to_front(
        self,
        refresh_target=True,
        *,
        drafts_url: str,
        acquire_bring_lock: bool = True,
    ) -> None:
        """将目标页面置前，并尽量确保整个指纹浏览器实例只保留一个目标页面。

        需求背景：指纹浏览器可能开了多个标签页/窗口；即使 ensure_open 选中了可用 page，也不一定是目标页面。
        这里确保 drafts_url 在每次 ensure_open 后都会被 bring_to_front，
        且会关闭同一个指纹浏览器（同一 CDP 连接）内除 drafts_url 外的其它页面（包括其它站点、about:blank、新窗口、重复 drafts 等），
        尽量只保留一个目标页面以节省内存。

        acquire_bring_lock：为 False 时表示调用方已持有 ``_bring_drafts_lock``（避免与外层 ``async with`` 死锁）。
        """
        try:
            target_host = urlparse(drafts_url).netloc.strip().lower()
        except Exception:
            target_host = ""

        async def _inner() -> None:
            def _is_page_closed(p: Any) -> bool:
                try:
                    return bool(getattr(p, "is_closed", lambda: False)())
                except Exception:
                    return False

            def _safe_page_url(p: Any) -> str:
                try:
                    return str(getattr(p, "url", "") or "").strip()
                except Exception:
                    return ""

            def _safe_page_host(u: str) -> str:
                try:
                    return (urlparse(u).netloc or "").strip().lower()
                except Exception:
                    return ""

            async def _snapshot_contexts_pages() -> tuple[list[Any], list[tuple[Any, Any, str]]]:
                """返回 (contexts, open_pages[(ctx,page,url)])。"""
                ctx0 = getattr(self.pw_ctx, "context", None)
                br0 = getattr(self.pw_ctx, "browser", None)
                try:
                    ctxs0 = list(getattr(br0, "contexts", []) or [])
                except Exception:
                    ctxs0 = []
                if ctx0 is not None and ctx0 not in ctxs0:
                    ctxs0.insert(0, ctx0)
                open_pages0: list[tuple[Any, Any, str]] = []
                for c0 in (ctxs0 or []):
                    try:
                        pages0 = list(getattr(c0, "pages", []) or [])
                    except Exception:
                        pages0 = []
                    for p0 in pages0:
                        if _is_page_closed(p0):
                            continue
                        u0 = _safe_page_url(p0)
                        open_pages0.append((c0, p0, u0))
                return ctxs0, open_pages0

            async def _keep_only_one_drafts_page(keep_page: Any) -> Any:
                """关闭其它所有页面/多余 contexts，仅保留 keep_page。返回 keep_page。"""
                ctxs1, open_pages1 = await _snapshot_contexts_pages()
                keep_ctx = None
                for c1, p1, _u1 in open_pages1:
                    if p1 is keep_page:
                        keep_ctx = c1
                        break
                if keep_ctx is None:
                    try:
                        maybe_ctx = getattr(keep_page, "context", None)
                        keep_ctx = maybe_ctx() if callable(maybe_ctx) else maybe_ctx
                    except Exception:
                        keep_ctx = None

                for _c1, p1, _u1 in open_pages1:
                    if p1 is keep_page:
                        continue
                    try:
                        await p1.close()
                    except Exception:
                        pass

                if keep_ctx is not None:
                    for c1 in ctxs1:
                        if c1 is keep_ctx:
                            continue
                        try:
                            await c1.close()
                        except Exception:
                            pass

                try:
                    if keep_ctx is not None:
                        self.pw_ctx.context = keep_ctx
                except Exception:
                    pass
                return keep_page

            ctxs, open_pages = await _snapshot_contexts_pages()
            if not ctxs:
                return

            drafts_page = None
            cur_page = getattr(self.pw_ctx, "page", None)
            if cur_page is not None and not _is_page_closed(cur_page):
                cur_u0 = _safe_page_url(cur_page)
                if cur_u0.startswith(drafts_url):
                    drafts_page = cur_page

            if drafts_page is None:
                for _c, p, u in open_pages:
                    if u.startswith(drafts_url):
                        drafts_page = p
                        break

            if drafts_page is None:
                ctx_pref = getattr(self.pw_ctx, "context", None) or (ctxs[0] if ctxs else None)
                if ctx_pref is None:
                    return
                try:
                    drafts_page = await ctx_pref.new_page()
                except Exception:
                    return
                try:
                    await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                except Exception:
                    pass

            await self._push_debug_progress(drafts_page, "已选定目标页面，准备清理其它页面", level="info")

            drafts_page = await _keep_only_one_drafts_page(drafts_page)

            try:
                self.pw_ctx.page = drafts_page
            except Exception:
                pass
            try:
                await drafts_page.bring_to_front()
            except Exception:
                pass
            try:
                cur_u = str(getattr(drafts_page, "url", "") or "").strip()
            except Exception:
                cur_u = ""

            if refresh_target:
                try:
                    await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                    await self._push_debug_progress(drafts_page, "目标页面刷新完成", level="ok")
                except Exception:
                    await self._push_debug_progress(drafts_page, "目标页面刷新失败（将继续流程）", level="warn")
                    pass

                try:
                    await drafts_page.evaluate("() => { try { window.focus(); } catch(e) {} }")
                except Exception:
                    pass

                await asyncio.sleep(3.0)

            # 若出现未登录提示，尽量先触发登录
            try:
                try:
                    page_html = await drafts_page.content()
                    if "Something went wrong. Please try again in a few minutes." in (page_html or ""):
                        await self._push_debug_progress(
                            drafts_page,
                            "检测到 Something went wrong 提示，先刷新目标页面",
                            level="warn",
                        )
                        try:
                            await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                        except Exception:
                            pass
                        try:
                            await asyncio.sleep(2.0)
                        except Exception:
                            pass
                except Exception:
                    pass

                clicked, has_login_button = await self._maybe_click_login_button_if_prompted(drafts_page)
                if clicked:
                    try:
                        await asyncio.sleep(3.0)
                    except Exception:
                        pass

                    try:
                        await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                    except Exception:
                        pass

                if not clicked and has_login_button:
                    await self._push_debug_progress(drafts_page, "重新再试一次点击login", level="ok")
                    try:
                        await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                    except Exception:
                        pass

                    clicked, has_login_button = await self._maybe_click_login_button_if_prompted(drafts_page)
                    if clicked:
                        await self._push_debug_progress(drafts_page, "重新再试一次点击login成功", level="ok")
                    else:
                        await self._push_debug_progress(drafts_page, "重新再试一次点击login失败", level="error")
            except Exception:
                pass

            # Cloudflare interstitial 自愈
            try:
                maybe_cf = await self._is_cloudflare_page(drafts_page, deep=False)
                if maybe_cf:
                    try:
                        await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                    except Exception:
                        pass
                    await asyncio.sleep(3.0)

                    await self._push_debug_progress(drafts_page, "页面疑似 Cloudflare，进入自愈流程", level="warn")
                    still_cf_after_wait = await self._wait_cloudflare_auto_pass(
                        drafts_page,
                        max_wait_seconds=45.0,
                        max_success_clicks=3,
                    )
                    if still_cf_after_wait and await self._is_cloudflare_page(drafts_page, deep=True):
                        await self._push_debug_progress(drafts_page, "Cloudflare 持续存在，准备重启窗口", level="warn")
                        await self._restart_window_and_restore_single_drafts(
                            drafts_url=drafts_url, target_host=target_host
                        )
                        try:
                            ctx_new = getattr(self.pw_ctx, "context", None)
                            br_new = getattr(self.pw_ctx, "browser", None)
                            ctxs_new: list[Any] = []
                            try:
                                ctxs_new = list(getattr(br_new, "contexts", []) or [])
                            except Exception:
                                pass
                            if ctx_new is not None and ctx_new not in ctxs_new:
                                ctxs_new.insert(0, ctx_new)

                            all_pages_new: list[tuple[Any, str]] = []
                            for c_n in ctxs_new:
                                try:
                                    ps = list(getattr(c_n, "pages", []) or [])
                                except Exception:
                                    ps = []
                                for p_n in ps:
                                    try:
                                        closed = bool(getattr(p_n, "is_closed", lambda: False)())
                                    except Exception:
                                        closed = False
                                    if closed:
                                        continue
                                    try:
                                        u_n = str(getattr(p_n, "url", "") or "").strip()
                                    except Exception:
                                        u_n = ""
                                    all_pages_new.append((p_n, u_n))

                            target_page_new: Any = None
                            for p_n, u_n in all_pages_new:
                                if u_n.startswith(drafts_url):
                                    target_page_new = p_n
                                    break
                            if target_page_new is None:
                                for p_n, u_n in all_pages_new:
                                    try:
                                        h_n = (urlparse(u_n).netloc or "").strip().lower()
                                    except Exception:
                                        h_n = ""
                                    if h_n == target_host:
                                        target_page_new = p_n
                                        break

                            if target_page_new is not None:
                                for p_n, _u_n in all_pages_new:
                                    if p_n is target_page_new:
                                        continue
                                    try:
                                        await p_n.close()
                                    except Exception:
                                        pass
                                try:
                                    self.pw_ctx.page = target_page_new
                                    drafts_page = target_page_new
                                except Exception:
                                    pass
                                try:
                                    await target_page_new.bring_to_front()
                                except Exception:
                                    pass
                                await self._push_debug_progress(
                                    drafts_page, "重启后已恢复目标页面并置前", level="ok"
                                )
                        except Exception:
                            pass
            except Exception:
                pass

        if acquire_bring_lock:
            async with self._bring_drafts_lock:
                await _inner()
        else:
            await _inner()

    # ------------------------------------------------------------------
    # idle close / close_and_drop
    # ------------------------------------------------------------------
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

        async def _job():
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
        _drop_veo_session(self.cache_key)

    async def close(self) -> None:
        self._cancel_idle_close()
        await self.pw_ctx.close_and_drop()


def get_or_create_veo_session(
    *,
    vendor: str,
    base_url: str,
    access_key: Optional[str],
    space_id: str,
    window_key: str,
) -> VeoSession:
    """获取/创建 VEO 会话（按 window 维度缓存，避免重复开浏览器）。"""
    k = _veo_key(vendor, base_url, space_id, window_key)
    sess = _VEO_SESSIONS.get(k)
    if sess is None:
        pw_ctx = get_or_create_playwright_ctx(
            vendor=vendor,
            base_url=base_url,
            access_key=access_key,
            space_id=space_id,
            window_key=window_key,
        )
        sess = VeoSession(cache_key=k, pw_ctx=pw_ctx)
        _VEO_SESSIONS[k] = sess
    else:
        sess.pw_ctx.access_key = access_key
    return sess


VEO_FLOW_OPEN_ACCOUNT_DEFAULT_URL = "https://labs.google/fx/ja/tools/flow"


async def veo_flow_open_account(
    progress_cb: ProgressCB,
    *,
    db: Database,
    window_pk: int,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
    flow_url: Optional[str] = None,
    headless: bool = False,
) -> Dict[str, Any]:
    """按窗口凭据完成 Google 登录，打开 Google Flow，再断开本地 CDP（保留指纹浏览器窗口）。"""
    from .sora_plus_register_executor import (
        _do_login_flow,
        _is_already_logged_in,
        _pick_platform_domain_page,
        _resolve_window_platform_credentials,
    )

    creds = await _resolve_window_platform_credentials(db, window_pk=int(window_pk))
    platform_url = str(creds["platform_url"] or "").strip()
    platform_username = str(creds["platform_username"] or "").strip()
    platform_password = str(creds["platform_password"] or "").strip()
    platform_efa = str(creds["platform_efa"] or "").strip()

    target_flow = str(flow_url or "").strip() or VEO_FLOW_OPEN_ACCOUNT_DEFAULT_URL
    timeout_ms = int(max(10_000, min(float(timeout_seconds) * 1000, 120_000)))

    await progress_cb(1, {"stage": "resolve_credentials", "window_pk": int(window_pk)})

    sess = get_or_create_veo_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    sess.browser_headless = headless
    sess.idle_close_disabled = True
    try:
        sess._cancel_idle_close()
    except Exception:
        pass

    await sess.ensure_open(args=[], force_open=False, headless=headless)

    ctx = sess.pw_ctx
    async with ctx.driver_lock:
        if ctx.context is None:
            raise RuntimeError("浏览器上下文不可用：context is None")
        page = await _pick_platform_domain_page(ctx.context, platform_url=platform_url)
        ctx.page = page

        await page.goto(platform_url, wait_until="domcontentloaded", timeout=timeout_ms)
        await progress_cb(10, {"stage": "page_loaded", "url": platform_url})

        current_url = str(page.url or "").strip()
        if _is_already_logged_in(current_url):
            await progress_cb(55, {"stage": "already_logged_in", "current_url": current_url})
        else:
            await progress_cb(12, {"stage": "need_login", "current_url": current_url})
            await _do_login_flow(
                page,
                platform_username=platform_username,
                platform_password=platform_password,
                platform_efa=platform_efa,
                timeout_ms=timeout_ms,
                progress_cb=progress_cb,
            )

        await page.goto(target_flow, wait_until="domcontentloaded", timeout=timeout_ms)
        await progress_cb(90, {"stage": "flow_opened", "url": target_flow})

    try:
        br = getattr(ctx, "browser", None)
        pw = getattr(ctx, "playwright", None)
        try:
            ctx.browser = None
            ctx.context = None
            ctx.page = None
            ctx.cdp_endpoint = None
        except Exception:
            pass
        try:
            if br is not None:
                await br.close()
        except Exception:
            pass
        try:
            if pw is not None:
                await pw.stop()
        except Exception:
            pass
        try:
            ctx.playwright = None
        except Exception:
            pass
        await progress_cb(99, {"stage": "cdp_disconnected"})
    except Exception:
        pass

    return {
        "ok": True,
        "stage": "flow_ready",
        "platform_url": platform_url,
        "platform_username": platform_username,
        "flow_url": target_flow,
        "message": "已登录 Google 并打开 Flow，已断开 CDP",
    }


# ---------------------------------------------------------------------------
# Google Labs / Flow：余额与档位（与 flow2api flow_client.get_credits 对齐）
# ---------------------------------------------------------------------------
FLOW_LABS_CREDITS_URL = "https://aisandbox-pa.googleapis.com/v1/credits"
# 刷新 VEO 额度时：新开标签页读取「Next update: Apr 22」或「Next update: Tomorrow」作为额度重置日
VEO_ONE_GOOGLE_AI_ACTIVITY_URL = "https://one.google.com/ai/activity?g1_landing_page=0"
_VEO_NEXT_UPDATE_TOMORROW_RE = re.compile(
    r"Next\s+update\s*:\s*tomorrow\b",
    re.IGNORECASE,
)
_VEO_NEXT_UPDATE_RE = re.compile(
    r"Next\s+update\s*:\s*([A-Za-z]{3,9})\s+(\d{1,2})(?:\s*,\s*(\d{4}))?",
    re.IGNORECASE,
)
_VEO_MONTH_PREFIX_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
FLOW_VIDEO_SUBMIT_T2V_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"
FLOW_FLOW_UPLOAD_IMAGE_URL = "https://aisandbox-pa.googleapis.com/v1/flow/uploadImage"
FLOW_VIDEO_SUBMIT_I2V_START_IMAGE_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartImage"
FLOW_VIDEO_SUBMIT_I2V_START_END_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartAndEndImage"
FLOW_VIDEO_POLL_URL = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"
FLOW_FLOW_UPSAMPLE_IMAGE_URL = "https://aisandbox-pa.googleapis.com/v1/flow/upsampleImage"
VEO_RECAPTCHA_SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"

# 与 flow2api generation_handler gemini-3.1-flash-image-*（NARWHAL）一致
IMAGE_ASPECT_RATIO_LANDSCAPE = "IMAGE_ASPECT_RATIO_LANDSCAPE"
IMAGE_ASPECT_RATIO_PORTRAIT = "IMAGE_ASPECT_RATIO_PORTRAIT"
VEO_IMAGE_MODEL_NARWHAL = "NARWHAL"
# 与 flow2api generation_handler gemini-3.0-pro-image-*（GEM_PIX_2）一致
VEO_IMAGE_MODEL_GEM_PIX_2 = "GEM_PIX_2"
UPSAMPLE_IMAGE_RESOLUTION_2K = "UPSAMPLE_IMAGE_RESOLUTION_2K"

PAYGATE_TIER_NOT_PAID = "PAYGATE_TIER_NOT_PAID"
PAYGATE_TIER_ONE = "PAYGATE_TIER_ONE"
PAYGATE_TIER_TWO = "PAYGATE_TIER_TWO"


def _veo_normalize_user_paygate_tier(user_paygate_tier: Optional[str]) -> str:
    normalized = (user_paygate_tier or "").strip()
    if normalized in {PAYGATE_TIER_NOT_PAID, PAYGATE_TIER_ONE, PAYGATE_TIER_TWO}:
        return normalized
    return PAYGATE_TIER_NOT_PAID


def _veo_adjust_model_key_for_tier(model_key: str, tier: str) -> str:
    """与 flow2api generation_handler._handle_video_generation 中 tier/ultra 规则对齐。"""
    mk = (model_key or "").strip()
    if not mk:
        return mk
    if tier == PAYGATE_TIER_TWO:
        if "ultra" not in mk:
            if "_fl" in mk:
                mk = mk.replace("_fl", "_ultra_fl")
            else:
                mk = mk + "_ultra"
    elif tier in (PAYGATE_TIER_ONE, PAYGATE_TIER_NOT_PAID):
        if "ultra" in mk:
            mk = mk.replace("_ultra_fl", "_fl").replace("_ultra", "")
    return mk


def _veo_extract_project_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/tools/flow/project/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def _veo_labs_fx_prefix_url(hint_url: str) -> str:
    """用于 _bring_target_page_to_front：任意 labs 子路径均以 {origin}/fx 为前缀。"""
    h = (hint_url or "").strip() or "https://labs.google/fx"
    try:
        p = urlparse(h)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/fx"
    except Exception:
        pass
    return "https://labs.google/fx"


def _veo_project_page_url(*, project_id: str, hint_url: str) -> str:
    """构建 Flow 项目页 URL（保留 hint 中的语言前缀，如 /fx/zh/tools/flow/...）。"""
    pid = (project_id or "").strip()
    if not pid:
        return (hint_url or "").strip() or "https://labs.google/fx"
    hint = (hint_url or "").strip() or "https://labs.google/fx"
    try:
        p = urlparse(hint)
        origin = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else "https://labs.google"
    except Exception:
        origin = "https://labs.google"
    m = re.search(r"/fx/([a-z]{2})/tools/flow", hint, re.I)
    if m:
        return f"{origin}/fx/{m.group(1)}/tools/flow/project/{pid}"
    return f"{origin}/fx/tools/flow/project/{pid}"


def _veo_payload_looks_like_i2v(payload: Dict[str, Any]) -> bool:
    vt = str(payload.get("video_type") or payload.get("veo_video_type") or "").strip().lower()
    if vt in ("i2v", "image_to_video", "img2vid", "img2video"):
        return True
    if str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip():
        return True
    if str(payload.get("image_url") or payload.get("imageUrl") or "").strip():
        return True
    imgs = payload.get("images")
    if isinstance(imgs, list) and len(imgs) > 0:
        return True
    last_u = str(payload.get("last_image_url") or payload.get("lastImageUrl") or "").strip()
    if last_u:
        return True
    return False


def _veo_pick_orientation_from_ratio(ratio: Optional[str]) -> Optional[str]:
    """与 `sora_task_executor._pick_orientation_from_ratio` 一致：16:9→landscape，9:16→portrait。"""
    if not ratio:
        return None
    s = str(ratio).strip().lower().replace("：", ":")
    if "16:9" in s:
        return "landscape"
    if "9:16" in s:
        return "portrait"
    return None


def _veo_parse_pixel_pair_from_payload(payload: Dict[str, Any]) -> Optional[tuple[int, int]]:
    """从 payload 的宽高字段解析像素尺寸（宽×高）。"""
    pairs = (
        ("width", "height"),
        ("video_width", "video_height"),
        ("w", "h"),
    )
    for wk, hk in pairs:
        try:
            if payload.get(wk) is None or payload.get(hk) is None:
                continue
            w = int(float(payload.get(wk)))
            h = int(float(payload.get(hk)))
            if w > 0 and h > 0:
                return w, h
        except Exception:
            continue
    return None


def _veo_orientation_from_pixel_dimensions(w: int, h: int) -> Optional[str]:
    if w > h:
        return "landscape"
    if h > w:
        return "portrait"
    return None


def _veo_try_parse_wh_in_ratio_string(ratio: str) -> Optional[tuple[int, int]]:
    """比例串中的 `1920x1080` / `1080*1920` / `1080×1920` → (宽, 高)。"""
    s = str(ratio or "").strip()
    if not s:
        return None
    m = re.search(r"(\d+)\s*[xX*×]\s*(\d+)", s)
    if not m:
        return None
    try:
        a = int(m.group(1))
        b = int(m.group(2))
        if a > 0 and b > 0:
            return a, b
    except Exception:
        pass
    return None


def _veo_resolve_orientation_str(payload: Dict[str, Any]) -> Optional[str]:
    """横竖屏语义 `portrait` / `landscape`，与 Sora 一致优先「比例/宽高」，再 `orientation` / 显式 API 字段。

    顺序：
    1. `width`×`height`（及 video_width / w、h 等）
    2. `size_ratio` / `aspect_ratio` / `ratio` / `尺寸` 中的 `WxH` 子串
    3. 同上字段中的 `16:9` / `9:16`（与 Sora `_pick_orientation_from_ratio` 一致）
    4. `orientation`
    5. `video_aspect_ratio` / `aspectRatio` / `veo_aspect_ratio`（含横竖、PORTRAIT/LANDSCAPE）
    """
    payload = payload or {}

    wh = _veo_parse_pixel_pair_from_payload(payload)
    if wh:
        o = _veo_orientation_from_pixel_dimensions(wh[0], wh[1])
        if o:
            return o

    ratio = str(
        payload.get("size_ratio") or payload.get("aspect_ratio") or payload.get("ratio") or payload.get("尺寸") or ""
    ).strip() or None
    if ratio:
        wh2 = _veo_try_parse_wh_in_ratio_string(ratio)
        if wh2:
            o2 = _veo_orientation_from_pixel_dimensions(wh2[0], wh2[1])
            if o2:
                return o2
        lo = _veo_pick_orientation_from_ratio(ratio)
        if lo:
            return lo

    ori = str(payload.get("orientation") or "").strip().lower()
    if ori in ("portrait", "landscape"):
        return ori

    raw_ar = str(
        payload.get("video_aspect_ratio") or payload.get("aspectRatio") or payload.get("veo_aspect_ratio") or ""
    ).strip()
    if raw_ar:
        u = raw_ar.upper()
        if "PORTRAIT" in u or "竖" in raw_ar:
            return "portrait"
        if "LANDSCAPE" in u or "横" in raw_ar:
            return "landscape"

    return None


VIDEO_ASPECT_RATIO_LANDSCAPE = "VIDEO_ASPECT_RATIO_LANDSCAPE"
VIDEO_ASPECT_RATIO_PORTRAIT = "VIDEO_ASPECT_RATIO_PORTRAIT"

# 与 flow2api generation_handler MODEL_CONFIG 中 veo_3_1_i2v_s_fast_*_fl 对齐
VEO_I2V_MODEL_LANDSCAPE_FL = "veo_3_1_i2v_s_fast_fl"
VEO_I2V_MODEL_PORTRAIT_FL = "veo_3_1_i2v_s_fast_portrait_fl"


def _veo_resolve_i2v_aspect_ratio(payload: Dict[str, Any]) -> str:
    """I2V 的 aisandbox aspectRatio：与 T2V 相同规则，默认横屏。"""
    o = _veo_resolve_orientation_str(payload)
    if o == "portrait":
        return VIDEO_ASPECT_RATIO_PORTRAIT
    if o == "landscape":
        return VIDEO_ASPECT_RATIO_LANDSCAPE
    return VIDEO_ASPECT_RATIO_LANDSCAPE


def _veo_extract_url_from_image_item(item: Any) -> Optional[str]:
    if isinstance(item, str):
        u = item.strip()
        return u or None
    if isinstance(item, dict):
        for k in ("url", "image_url", "imageUrl", "src", "first_image_url", "firstImageUrl"):
            v = str(item.get(k) or "").strip()
            if v:
                return v
    return None


def _veo_collect_i2v_image_urls(payload: Dict[str, Any]) -> List[str]:
    """解析 1～2 张图 URL：优先 `images` 数组（顺序=首帧、尾帧），否则 first_image_url / image_url + last/end。"""
    payload = payload or {}
    imgs = payload.get("images")
    if isinstance(imgs, list) and len(imgs) > 0:
        if len(imgs) > 2:
            raise NonPenalizedTaskError("图生视频最多支持 2 张图片（首帧与尾帧）", status_code=400)
        out: List[str] = []
        for it in imgs:
            u = _veo_extract_url_from_image_item(it)
            if not u:
                raise NonPenalizedTaskError("images 数组中存在无法解析的图片地址", status_code=400)
            out.append(u)
        return out

    first = str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip()
    if not first:
        first = str(payload.get("image_url") or payload.get("imageUrl") or "").strip()
    last = str(
        payload.get("last_image_url")
        or payload.get("lastImageUrl")
        or payload.get("end_image_url")
        or payload.get("endImageUrl")
        or ""
    ).strip()
    if last and not first:
        raise NonPenalizedTaskError(
            "图生视频不能只提供尾图：请提供首图 first_image_url（或 images[0]），"
            "顺序为 [首帧, 尾帧]",
            status_code=400,
        )
    urls: List[str] = []
    if first:
        urls.append(first)
    if last:
        urls.append(last)
    return urls


def _veo_strip_i2v_fl_for_single_frame(model_key: str) -> str:
    """与 flow2api generation_handler 单首帧分支一致：去掉 model_key 中的 _fl 后缀（含 _fl_ 在中间的情况）。"""
    mk = str(model_key or "")
    actual = mk.replace("_fl_", "_")
    if actual.endswith("_fl"):
        actual = actual[:-3]
    return actual


def _veo_detect_image_mime_type(image_bytes: bytes) -> str:
    if len(image_bytes) < 12:
        return "image/jpeg"
    if image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    if image_bytes[:4] == b"\x89PNG":
        return "image/png"
    if image_bytes[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if image_bytes[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if image_bytes[:2] == b"BM":
        return "image/bmp"
    if image_bytes[:6] == b"\x00\x00\x00\x0cjP":
        return "image/jp2"
    return "image/jpeg"


async def _veo_download_image_bytes_for_i2v(
    url: str,
    *,
    label: str,
    timeout_seconds: float,
    user_agent: Optional[str],
) -> bytes:
    try:
        img_bytes, img_headers = await _download_bytes_local_async(
            url, timeout_seconds=timeout_seconds, user_agent=user_agent
        )
    except Exception as e:
        raise NonPenalizedTaskError(
            f"{label}下载失败（请检查地址是否可访问）：url={safe_trim(str(url), 400)!r} err={e}",
            status_code=400,
        ) from e
    if not img_bytes:
        raise NonPenalizedTaskError(
            f"{label}下载结果为空：url={safe_trim(str(url), 400)!r}",
            status_code=400,
        )
    ct = ""
    try:
        ct = str((img_headers or {}).get("content-type") or (img_headers or {}).get("Content-Type") or "").lower()
    except Exception:
        ct = ""
    if ("text/html" in ct) or ("application/json" in ct):
        raise NonPenalizedTaskError(
            f"{label}响应疑似非图片（Content-Type={safe_trim(ct, 120)!r}）：url={safe_trim(str(url), 400)!r}",
            status_code=400,
        )
    prepared, _fn, _mt = await _prepare_first_frame_image_for_upload_async(img_bytes)
    return prepared


async def _veo_flow_upload_image_in_window(
    *,
    page: Any,
    access_token: str,
    project_id: str,
    image_bytes: bytes,
    log_file: Path,
) -> str:
    """在指纹浏览器页面内调用 aisandbox `flow/uploadImage`（与 flow2api flow_client.upload_image 新版接口一致）。"""
    mime_type = _veo_detect_image_mime_type(image_bytes)
    ext = "png" if "png" in mime_type else "jpg"
    upload_file_name = f"fpbrowser2api_veo_{int(time.time() * 1000)}.{ext}"
    image_base64 = base64.b64encode(image_bytes).decode("utf-8")
    json_data: Dict[str, Any] = {
        "clientContext": {"tool": "PINHOLE", "projectId": str(project_id)},
        "fileName": upload_file_name,
        "imageBytes": image_base64,
        "isHidden": False,
        "isUserUploaded": True,
        "mimeType": mime_type,
    }
    tx = await page_fetch_json(
        page,
        url=FLOW_FLOW_UPLOAD_IMAGE_URL,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        json_data=json_data,
        log_file=log_file,
    )
    st = tx.get("status")
    if st is not None and int(st) >= 400:
        body = safe_trim(str(tx.get("response_body") or ""), 500)
        raise NonPenalizedTaskError(f"上传参考图失败：HTTP {st} {body}", status_code=502)
    resp = tx.get("_json")
    if not isinstance(resp, dict):
        raise NonPenalizedTaskError("上传参考图返回格式异常", status_code=502)
    media_id = (resp.get("media") or {}).get("name") or (resp.get("mediaGenerationId") or {}).get(
        "mediaGenerationId"
    )
    if not media_id:
        raise NonPenalizedTaskError(
            f"上传参考图未返回 mediaId：{safe_trim(str(resp), 300)}",
            status_code=502,
        )
    return str(media_id)


async def _veo_poll_operations_until_video_url(
    *,
    page: Any,
    access_token: str,
    log_file: Path,
    progress_cb: ProgressCB,
    operations: List[Dict[str, Any]],
    max_wait_seconds: float,
    poll_interval_seconds: float,
    poll_progress_base: int = 25,
    sess: Optional[Any] = None,
) -> str:
    """轮询 batchCheckAsyncVideoGenerationStatus，成功则返回 fifeUrl。"""
    max_attempts = max(3, int(max_wait_seconds / max(0.5, poll_interval_seconds)) + 3)
    consecutive_poll_errors = 0
    video_url: Optional[str] = None

    for attempt in range(max_attempts):
        sess._cancel_idle_close()
        await asyncio.sleep(poll_interval_seconds)
        pct = poll_progress_base + min(70, int((attempt + 1) / max(1, max_attempts) * 70))
        try:
            ptx = await page_fetch_json(
                page,
                url=FLOW_VIDEO_POLL_URL,
                method="POST",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
                json_data={"operations": operations},
                log_file=log_file,
            )
        except Exception as e:
            consecutive_poll_errors += 1
            append_log(log_file, f"[veo] poll error: {e}")
            if consecutive_poll_errors >= 3:
                raise NonPenalizedTaskError(f"视频状态轮询失败: {_short_err_msg(e)}", status_code=502) from e
            await progress_cb(pct, {"stage": "polling", "error": _short_err_msg(e)})
            continue

        consecutive_poll_errors = 0
        pst = ptx.get("status")
        if pst is not None and int(pst) >= 400:
            consecutive_poll_errors += 1
            body = safe_trim(str(ptx.get("response_body") or ""), 400)
            append_log(log_file, f"[veo] poll HTTP {pst} {body}")
            if consecutive_poll_errors >= 3:
                raise NonPenalizedTaskError(f"视频状态查询被拒绝: HTTP {pst}", status_code=502)
            continue

        checked = ptx.get("_json")
        checked_ops = checked.get("operations") if isinstance(checked, dict) else None
        if not isinstance(checked_ops, list) or len(checked_ops) == 0:
            await progress_cb(pct, {"stage": "polling", "upstream_status": None})
            continue

        op0 = checked_ops[0]
        status = op0.get("status")
        await progress_cb(pct, {"stage": "polling", "upstream_status": status})

        if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
            meta = (op0.get("operation") or {}).get("metadata") or {}
            vinfo = meta.get("video") or {}
            video_url = str(vinfo.get("fifeUrl") or "").strip() or None
            if not video_url:
                raise NonPenalizedTaskError("上游返回成功但缺少视频 fifeUrl", status_code=502)
            break

        if status == "MEDIA_GENERATION_STATUS_FAILED":
            err = ((op0.get("operation") or {}).get("error") or {})
            msg = str(err.get("message") or "未知错误")
            code = str(err.get("code") or "")
            raise NonPenalizedTaskError(f"视频生成失败: {msg}" + (f" ({code})" if code else ""), status_code=502)

        if isinstance(status, str) and status.startswith("MEDIA_GENERATION_STATUS_ERROR"):
            raise NonPenalizedTaskError(f"视频生成错误状态: {status}", status_code=502)

    else:
        raise NonPenalizedTaskError(
            f"视频生成超时（已轮询约 {max_attempts} 次，间隔 {poll_interval_seconds}s）",
            status_code=504,
        )

    return str(video_url)


def _veo_resolve_t2v_model(payload: Dict[str, Any]) -> tuple[str, str]:
    """返回 (videoModelKey, aspectRatio)。横竖屏与 Sora 一致：优先宽高/比例字段，再 model 显式指定。

    与 `sora_gen_video` 对齐的输入：`size_ratio` / `aspect_ratio` / `ratio` / `尺寸`、`width`×`height`、
    比例串内 `WxH`、`16:9`/`9:16`、`orientation`；另支持 `video_aspect_ratio` 等 API 字段。
    若以上均未判定，则回退解析 `model` / `videoModelKey` 是否含 `t2v_fast_portrait`。
    默认横屏 `veo_3_1_t2v_fast`。
    """
    o = _veo_resolve_orientation_str(payload)
    if o is None:
        raw = (
            str(
                payload.get("model")
                or payload.get("veo_model")
                or payload.get("video_model")
                or payload.get("videoModelKey")
                or ""
            )
            .strip()
            .lower()
        )
        if "t2v_fast_portrait" in raw or raw == "veo_3_1_t2v_fast_portrait":
            o = "portrait"

    if o == "portrait":
        return "veo_3_1_t2v_fast_portrait", VIDEO_ASPECT_RATIO_PORTRAIT
    return "veo_3_1_t2v_fast", VIDEO_ASPECT_RATIO_LANDSCAPE


def _veo_generate_session_id() -> str:
    return f";{int(time.time() * 1000)}"


async def _veo_try_recaptcha_token_in_page(page: Any, *, action: str, log_file: Path) -> Optional[str]:
    """在已打开的 Labs 页面尝试 grecaptcha.enterprise.execute（与 flow2api 站点 key / action 一致）。"""
    try:
        res = await page.evaluate(
            """async ({ siteKey, action }) => {
              try {
                if (window.grecaptcha && window.grecaptcha.enterprise) {
                  await new Promise((resolve) => {
                    try {
                      window.grecaptcha.enterprise.ready(() => resolve());
                    } catch (e) {
                      resolve();
                    }
                  });
                  const t = await window.grecaptcha.enterprise.execute(siteKey, { action });
                  return (t && String(t)) || null;
                }
              } catch (e) {
                return { __err: String(e && e.message ? e.message : e) };
              }
              return null;
            }""",
            {"siteKey": VEO_RECAPTCHA_SITE_KEY, "action": action},
        )
        if isinstance(res, dict) and res.get("__err"):
            append_log(log_file, f"[veo][recaptcha] enterprise.execute error: {res.get('__err')}")
            return None
        s = str(res or "").strip()
        return s or None
    except Exception as e:
        append_log(log_file, f"[veo][recaptcha] page.evaluate failed: {e}")
        return None


def veo_format_paygate_tier_label(tier: Optional[str]) -> str:
    """将 userPaygateTier 转为可读套餐名（与 flow2api manage.html formatAccountType 一致）。"""
    t = str(tier or "").strip()
    if not t or t == "PAYGATE_TIER_NOT_PAID":
        return "Google Labs · 普通"
    if t == "PAYGATE_TIER_ONE":
        return "Google Labs · Pro"
    if t == "PAYGATE_TIER_TWO":
        return "Google Labs · Ult"
    return f"Google Labs · {t}"


def _veo_normalize_credits_payload(data: Any) -> Dict[str, Any]:
    """解析 aisandbox /v1/credits 的 JSON（与 flow2api get_credits 字段一致）。"""
    if not isinstance(data, dict):
        raise RuntimeError("credits 返回格式异常")
    try:
        credits_i = int(data.get("credits") if data.get("credits") is not None else 0)
    except Exception:
        credits_i = 0
    tier_raw = data.get("userPaygateTier") or data.get("user_paygate_tier")
    tier_s = str(tier_raw).strip() if tier_raw is not None else None
    if tier_s == "":
        tier_s = None
    return {"credits": credits_i, "user_paygate_tier": tier_s, "raw": data}


def _veo_month_token_to_num(month_tok: str) -> Optional[int]:
    t = (month_tok or "").strip().lower()
    if len(t) < 3:
        return None
    return _VEO_MONTH_PREFIX_TO_NUM.get(t[:3])


def _veo_local_next_1305_datetime() -> datetime:
    """本地「下一次 13:05」：当前时刻若已过当天 13:05 则为明天 13:05，否则为今天 13:05。"""
    now = datetime.now()
    today_0105 = now.replace(hour=1, minute=5, second=0, microsecond=0)
    if now > today_0105:
        nd = now.date() + timedelta(days=1)
        return datetime(nd.year, nd.month, nd.day, 1, 5, 0)
    return today_0105


def _veo_next_update_text_to_cooldown_str(page_text: str) -> Optional[str]:
    """从页面文本解析「Next update: Tomorrow」等为本地下一次 13:05（见 _veo_local_next_1305_datetime），
    或「Next update: Apr 22」等与当年月日组合为本地该日 13:05（若解析日为今天则同样按下一次 13:05 规则）。"""
    if not (page_text or "").strip():
        return None
    if _VEO_NEXT_UPDATE_TOMORROW_RE.search(page_text):
        dt_local = _veo_local_next_1305_datetime()
        return dt_local.strftime("%Y-%m-%d %H:%M:%S")
    m = _VEO_NEXT_UPDATE_RE.search(page_text)
    if not m:
        return None
    mon = _veo_month_token_to_num(m.group(1) or "")
    if mon is None:
        return None
    try:
        dnum = int(m.group(2))
    except Exception:
        return None
    if dnum < 1 or dnum > 31:
        return None

    now = datetime.now()
    year_s = (m.group(3) or "").strip()
    if year_s:
        try:
            y = int(year_s)
        except Exception:
            return None
        try:
            final_d = date(y, mon, dnum)
        except ValueError:
            return None
    else:
        y = now.year
        try:
            cand = date(y, mon, dnum)
        except ValueError:
            return None
        if cand < now.date():
            y += 1
        try:
            final_d = date(y, mon, dnum)
        except ValueError:
            return None

    if final_d == now.date():
        dt_local = _veo_local_next_1305_datetime()
    else:
        dt_local = datetime(final_d.year, final_d.month, final_d.day, 13, 5, 0)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


async def _veo_scrape_one_google_next_update_cooldown(
    sess: "VeoSession",
    *,
    log_file: Path,
    goto_timeout_ms: int = 90_000,
    settle_seconds: float = 4.0,
) -> Optional[str]:
    """新开标签页打开 one.google.com AI 活动页，读取 Next update 作为 cooldown_until（与 Sora nf_check 格式一致）。"""
    ctx = getattr(sess.pw_ctx, "context", None)
    if ctx is None:
        append_log(log_file, "[veo][activity] 无 browser context，跳过 Next update")
        return None

    page = None
    try:
        page = await ctx.new_page()
        await page.goto(
            VEO_ONE_GOOGLE_AI_ACTIVITY_URL,
            wait_until="domcontentloaded",
            timeout=int(goto_timeout_ms),
        )
        await asyncio.sleep(max(0.0, float(settle_seconds)))
        text = ""
        try:
            text = await page.inner_text("body")
        except Exception:
            try:
                text = await page.content()
            except Exception as e2:
                append_log(log_file, f"[veo][activity] 读取页面正文失败: {e2}")
                return None

        cu = _veo_next_update_text_to_cooldown_str(text)
        if cu:
            append_log(log_file, f"[veo][activity] Next update -> cooldown_until={cu}")
        else:
            append_log(log_file, "[veo][activity] 未匹配到 Next update（可能未登录或文案变更）")
        return cu
    except Exception as e:
        append_log(log_file, f"[veo][activity] 打开活动页失败: {e}")
        return None
    finally:
        if page is not None:
            try:
                await page.close()
            except Exception:
                pass


async def veo_fetch_next_update_cooldown_from_one_google_activity(
    *,
    sess: "VeoSession",
    target_url: str,
) -> Optional[str]:
    """在指纹浏览器中另开标签页打开 Google AI 活动页，从正文匹配「Next update: Apr 22」等文案，解析为额度重置时间字符串（与 sora nf_check 的 cooldown_until 格式一致：本地日期 0 点）。

    需已能正常使用该窗口（与 veo_fetch_credits_in_window 相同的前置：开窗、labs 目标页上下文）；失败返回 None，不抛错。
    """
    try:
        sess._cancel_idle_close()
        log_file = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
        async with sess._bring_drafts_lock:
            await sess.ensure_open(
                args=sess.browser_open_args,
                force_open=sess.browser_force_open,
                headless=sess.browser_headless,
                acquire_bring_lock=False,
            )
            await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url, acquire_bring_lock=False)
            if sess.pw_ctx.page is None:
                append_log(log_file, "[veo][activity] page 未初始化，跳过 Next update")
                return None
            cu = await _veo_scrape_one_google_next_update_cooldown(sess, log_file=log_file)
            await sess.pw_ctx.disconnect_playwright_only()
            return cu
    except Exception as e:
        try:
            lf = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            append_log(lf, f"[veo][activity] veo_fetch_next_update_cooldown_from_one_google_activity 失败: {e}")
        except Exception:
            pass
        return None


async def veo_fetch_credits_in_window(
    *,
    sess: "VeoSession",
    target_url: str,
    access_token: str,
) -> Dict[str, Any]:
    """在指纹浏览器页面上下文中 GET aisandbox /v1/credits（与 Sora 的 page_fetch_json 一致）。

    使用窗口所在页面的 fetch（credentials: include + Bearer），走指纹浏览器网络栈/代理。
    """
    tok = str(access_token or "").strip()
    if not tok:
        raise RuntimeError("缺少 access_token（请先获取并保存 access_token）")

    
    sess._cancel_idle_close()

    async with sess._bring_drafts_lock:
        await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless, acquire_bring_lock=False)
        await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url, acquire_bring_lock=False)
        if sess.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")

        log_file = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

        tx = await page_fetch_json(
            sess.pw_ctx.page,
            url=FLOW_LABS_CREDITS_URL,
            method="GET",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {tok}",
            },
            json_data=None,
            log_file=log_file,
        )
        status = tx.get("status")
        await sess.pw_ctx.disconnect_playwright_only()
        if status is not None and int(status) >= 400:
            body = safe_trim(str(tx.get("response_body") or ""), 400)
            raise RuntimeError(f"查询 credits 失败：HTTP {status} {body}")

        return _veo_normalize_credits_payload(tx.get("_json"))


# ---------------------------------------------------------------------------
# 获取 access_token（从指纹浏览器读取 __Secure-next-auth.session-token 并转换）
# ---------------------------------------------------------------------------
async def veo_fetch_access_token_in_window(
    *,
    sess: "VeoSession",
    target_url: str,
) -> Dict[str, Any]:
    """在指纹浏览器窗口中读取 __Secure-next-auth.session-token cookie，
    并通过 /fx/api/auth/session 端点获取 access_token。

    流程：
    1. 打开/复用指纹浏览器窗口，导航到 target_url
    2. 通过 Playwright context.cookies() 读取 __Secure-next-auth.session-token
    3. 在页面上下文中 fetch /fx/api/auth/session（credentials: include，自动携带 cookie）
    4. 解析返回的 accessToken / expires / user 信息
    """
    
    sess._cancel_idle_close()

    async with sess._bring_drafts_lock:
        if sess.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")
        await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless, acquire_bring_lock=False)
        await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url, acquire_bring_lock=False)

        log_file = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

        # ── Step 1：从浏览器 cookie 中读取 __Secure-next-auth.session-token ──
        session_token = None
        try:
            context = sess.pw_ctx.context
            if context:
                try:
                    parsed = urlparse(target_url)
                    cookie_url = f"{parsed.scheme}://{parsed.netloc}"
                except Exception:
                    cookie_url = target_url
                cookies = await context.cookies(cookie_url)
                for cookie in (cookies or []):
                    if cookie.get("name") == "__Secure-next-auth.session-token":
                        session_token = str(cookie.get("value") or "").strip() or None
                        break
        except Exception as e:
            append_log(log_file, f"[veo][token] context.cookies() failed: {e}")

        if not session_token:
            # HttpOnly cookie 无法通过 document.cookie 读取，但仍尝试兜底
            try:
                session_token = await sess.pw_ctx.page.evaluate("""
                    () => {
                        try {
                            for (const part of document.cookie.split(';')) {
                                const p = part.trim();
                                if (p.startsWith('__Secure-next-auth.session-token=')) {
                                    return p.split('=').slice(1).join('=');
                                }
                            }
                        } catch(e) {}
                        return null;
                    }
                """)
                if session_token:
                    session_token = str(session_token).strip() or None
            except Exception as e:
                append_log(log_file, f"[veo][token] document.cookie fallback failed: {e}")

        if not session_token:
            await sess.pw_ctx.disconnect_playwright_only()
            raise RuntimeError(
                "未找到 __Secure-next-auth.session-token cookie（请确认窗口已登录 Google/VEO）"
            )

        append_log(log_file, f"[veo][token] session_token found, length={len(session_token)}")

        # ── Step 2：通过 /fx/api/auth/session 端点将 ST 转换为 AT ──
        # 参考 flow2api: flow_client.st_to_at()
        #   GET https://labs.google/fx/api/auth/session  (Cookie: __Secure-next-auth.session-token={st})
        #   返回: {"access_token": "AT", "expires": "...", "user": {"email": "..."}}
        # 这里用 page_fetch_json (credentials: include) 自动携带 cookie，效果等价。
        try:
            parsed = urlparse(target_url)
            auth_session_url = f"{parsed.scheme}://{parsed.netloc}/fx/api/auth/session"
        except Exception:
            auth_session_url = "https://labs.google/fx/api/auth/session"

        access_token = None
        expires = None
        email = None

        try:
            tx = await page_fetch_json(
                sess.pw_ctx.page,
                url=auth_session_url,
                method="GET",
                headers={"Accept": "application/json"},
                json_data=None,
                log_file=log_file,
            )
            data = tx.get("_json")
            if isinstance(data, dict):
                # flow2api 返回 snake_case "access_token"
                access_token = (
                    str(data.get("access_token") or data.get("accessToken") or "").strip() or None
                )
                expires = str(data.get("expires") or "").strip() or None
                user = data.get("user") if isinstance(data.get("user"), dict) else {}
                email = str((user or {}).get("email") or "").strip() or None
        except Exception as e:
            append_log(log_file, f"[veo][token] auth/session fetch failed: {e}")

        await sess.pw_ctx.disconnect_playwright_only()

        if access_token:
            append_log(log_file, f"[veo][token] access_token obtained via auth/session, email={email}")
            return {
                "access_token": access_token,
                "expires": expires,
                "email": email,
                "session_token": session_token,
            }

        # auth/session 未返回有效 access_token 时，直接返回 session_token（ST）作为凭证
        # 与 flow2api 一致：ST 本身可用于后续 API 调用（通过 Cookie 方式认证）
        append_log(log_file, "[veo][token] auth/session did not return access_token, using session_token directly")
        return {
            "access_token": session_token,
            "expires": None,
            "email": None,
            "session_token": session_token,
        }


def _veo_trpc_create_project_url(target_url: str) -> str:
    raw = (target_url or "").strip() or "https://labs.google/fx"
    try:
        p = urlparse(raw)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/fx/api/trpc/project.createProject"
    except Exception:
        pass
    return "https://labs.google/fx/api/trpc/project.createProject"


def _parse_trpc_create_project_response(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    cur: Any = obj
    if isinstance(cur, list) and cur:
        cur = cur[0]
    if not isinstance(cur, dict):
        return None

    def _dig(d: Any, *keys: str) -> Any:
        x = d
        for k in keys:
            if not isinstance(x, dict):
                return None
            x = x.get(k)
        return x

    r = _dig(cur, "result", "data", "json", "result")
    if isinstance(r, dict):
        pid = r.get("projectId") or r.get("project_id")
        if pid:
            s = str(pid).strip()
            return s or None

    j = _dig(cur, "result", "data", "json")
    if isinstance(j, dict):
        pid2 = j.get("projectId") or j.get("project_id")
        if pid2:
            s2 = str(pid2).strip()
            return s2 or None

    pid3 = cur.get("projectId") or cur.get("project_id")
    if pid3:
        s3 = str(pid3).strip()
        return s3 or None
    return None


async def veo_create_flow_project_in_window(
    *,
    sess: "VeoSession",
    target_url: str,
    title: str,
    tool_name: str = "PINHOLE",
) -> str:
    """在指纹浏览器页面内调用 Flow `project.createProject`（与 flow2api flow_client.create_project 等价，走 Cookie）。"""
    title = str(title or "").strip()
    if not title:
        raise RuntimeError("项目标题不能为空")
    tn = str(tool_name or "PINHOLE").strip() or "PINHOLE"

    await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless)
    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url)
    sess._cancel_idle_close()

    async with sess._bring_drafts_lock:
        if sess.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")

        log_file = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
        url = _veo_trpc_create_project_url(target_url)
        json_data = {"json": {"projectTitle": title, "toolName": tn}}

        tx = await page_fetch_json(
            sess.pw_ctx.page,
            url=url,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json_data=json_data,
            log_file=log_file,
        )
        st = tx.get("status")
        if st is not None and int(st) >= 400:
            body = safe_trim(str(tx.get("response_body") or ""), 500)
            raise RuntimeError(f"createProject 失败：HTTP {st} {body}")

        pid = _parse_trpc_create_project_response(tx.get("_json"))
        if not pid:
            raise RuntimeError(f"createProject 响应无效：{safe_trim(str(tx.get('response_body') or ''), 400)}")

        append_log(log_file, f"[veo][project] created title={title!r} project_id={pid}")
        return pid


def _veo_resolve_n_frames(payload: Dict[str, Any]) -> int:
    """与 Sora 一致读取时长字段；显式为 1 时表示单帧 → 走文生图/图生图，其它值交给 `_pick_n_frames`（视频帧数语义）。"""
    duration_v = payload.get("n_frames") or payload.get("duration_frames") or payload.get("duration") or payload.get("时长")
    try:
        iv = int(float(duration_v))
    except Exception:
        iv = 0
    if iv == 1:
        return 1
    return _pick_n_frames(duration_v)


def _veo_resolve_image_aspect_ratio(payload: Dict[str, Any]) -> str:
    """文生图/图生图：横竖屏规则与 I2V/T2V 一致，默认横版。"""
    o = _veo_resolve_orientation_str(payload)
    if o == "portrait":
        return IMAGE_ASPECT_RATIO_PORTRAIT
    return IMAGE_ASPECT_RATIO_LANDSCAPE


def _veo_flow_media_batch_generate_images_url(project_id: str) -> str:
    return f"https://aisandbox-pa.googleapis.com/v1/projects/{str(project_id).strip()}/flowMedia:batchGenerateImages"


def _veo_truthy_payload_flag(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "on")


def _veo_resolve_image_model_name(payload: Dict[str, Any]) -> str:
    """文生图/图生图模型：默认 NARWHAL；`use_gem_pix_2` 或显式模型名为 GEM_PIX_2 时用 GEM_PIX_2（对齐 flow2api）。"""
    for key in ("veo_image_model", "image_model_name", "imageModelName"):
        raw = payload.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        s = str(raw).strip().upper().replace("-", "_")
        if s in ("GEM_PIX_2", "GEMPIX2", "GEM_PIX2"):
            return VEO_IMAGE_MODEL_GEM_PIX_2
        if s in ("NARWHAL",):
            return VEO_IMAGE_MODEL_NARWHAL
    if _veo_truthy_payload_flag(payload.get("use_gem_pix_2") or payload.get("veo_use_gem_pix_2")):
        return VEO_IMAGE_MODEL_GEM_PIX_2
    return VEO_IMAGE_MODEL_NARWHAL


def _veo_resolve_image_output_resolution(payload: Dict[str, Any]) -> tuple[str, bool]:
    """返回 (展示用标签 '1K'|'2K', 是否需要调用 flow/upsampleImage)。默认 1K，不放大。"""
    raw = payload.get("resolution") or payload.get("image_resolution") or payload.get("veo_image_resolution")
    if raw is None or str(raw).strip() == "":
        return ("1K", False)
    s = str(raw).strip().lower().replace(" ", "")
    if s in ("2k", "2048", "2k_output", "uhd_2k"):
        return ("2K", True)
    return ("1K", False)


def _veo_parse_upsample_encoded_image(resp: Any) -> str:
    if not isinstance(resp, dict):
        raise NonPenalizedTaskError("图片放大返回格式异常", status_code=502)
    enc = resp.get("encodedImage")
    if enc is None:
        enc = ""
    out = str(enc).strip()
    if not out:
        raise NonPenalizedTaskError(
            f"图片放大未返回 encodedImage：{safe_trim(str(resp), 320)}",
            status_code=502,
        )
    return out


async def _veo_flow_upsample_image_in_window(
    *,
    page: Any,
    access_token: str,
    project_id: str,
    media_id: str,
    user_paygate_tier: str,
    generation_session_id: str,
    log_file: Path,
    max_retries: int,
    bring_prefix: str,
    sess: "VeoSession",
    target_bring_acquire_lock: bool = True,
) -> str:
    """页面内调用 `flow/upsampleImage`（对齐 flow2api `FlowClient.upsample_image`），返回 base64 图片数据。"""
    last_err: Optional[str] = None
    n = max(1, min(5, int(max_retries)))
    for attempt in range(n):
        recaptcha_token = await _veo_try_recaptcha_token_in_page(
            page, action="IMAGE_GENERATION", log_file=log_file
        )
        if not recaptcha_token:
            last_err = "无法获取 reCAPTCHA token（放大）"
            append_log(log_file, f"[veo][image][upsample] attempt {attempt + 1}: no recaptcha")
            if attempt + 1 < n:
                await asyncio.sleep(2.0)
                try:
                    await sess._bring_target_page_to_front(
                        refresh_target=False,
                        drafts_url=bring_prefix,
                        acquire_bring_lock=target_bring_acquire_lock,
                    )
                except Exception:
                    pass
            continue

        upsample_session_id = generation_session_id or _veo_generate_session_id()
        json_data: Dict[str, Any] = {
            "mediaId": str(media_id).strip(),
            "targetResolution": UPSAMPLE_IMAGE_RESOLUTION_2K,
            "clientContext": {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                },
                "sessionId": upsample_session_id,
                "projectId": str(project_id).strip(),
                "tool": "PINHOLE",
                "userPaygateTier": _veo_normalize_user_paygate_tier(user_paygate_tier),
            },
        }
        try:
            tx = await page_fetch_json(
                page,
                url=FLOW_FLOW_UPSAMPLE_IMAGE_URL,
                method="POST",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
                json_data=json_data,
                log_file=log_file,
            )
        except Exception as e:
            last_err = _short_err_msg(e, max_len=200)
            append_log(log_file, f"[veo][image][upsample] fetch error: {e}")
            continue

        st = tx.get("status")
        if st is not None and int(st) >= 400:
            body = safe_trim(str(tx.get("response_body") or ""), 500)
            last_err = f"HTTP {st} {body}"
            append_log(log_file, f"[veo][image][upsample] rejected: {last_err}")
            if attempt + 1 < n:
                await asyncio.sleep(1.5)
            continue

        resp = tx.get("_json")
        try:
            b64 = _veo_parse_upsample_encoded_image(resp)
        except NonPenalizedTaskError as e:
            last_err = str(e)
            append_log(log_file, f"[veo][image][upsample] bad response: {last_err}")
            continue

        append_log(
            log_file,
            f"[veo][image][upsample] ok mediaId={safe_trim(str(media_id), 60)!r} b64_len={len(b64)}",
        )
        return b64

    raise NonPenalizedTaskError(last_err or "图片放大失败", status_code=502)


def _veo_parse_batch_generate_images_fife_url(resp: Any) -> tuple[str, Optional[str]]:
    if not isinstance(resp, dict):
        raise NonPenalizedTaskError("图片生成返回格式异常", status_code=502)
    media = resp.get("media")
    m0: Optional[Dict[str, Any]] = None
    if isinstance(media, list) and len(media) > 0 and isinstance(media[0], dict):
        m0 = media[0]
    if m0 is None:
        reqs = resp.get("responses")
        if isinstance(reqs, list) and len(reqs) > 0 and isinstance(reqs[0], dict):
            media2 = reqs[0].get("media")
            if isinstance(media2, list) and len(media2) > 0 and isinstance(media2[0], dict):
                m0 = media2[0]
    if m0 is None:
        raise NonPenalizedTaskError(
            f"图片生成结果为空：{safe_trim(str(resp), 320)}",
            status_code=502,
        )
    img_block = m0.get("image")
    if not isinstance(img_block, dict):
        img_block = {}
    gen = img_block.get("generatedImage")
    if not isinstance(gen, dict):
        gen = {}
    fife = str(gen.get("fifeUrl") or "").strip()
    if not fife:
        raise NonPenalizedTaskError(
            f"图片生成未返回 fifeUrl：{safe_trim(str(resp), 400)}",
            status_code=502,
        )
    name_v = m0.get("name")
    mid = str(name_v).strip() if name_v else None
    return fife, mid


async def _veo_execute_image_mode(
    *,
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    prompt: str,
    project_id: str,
    access_token: str,
    log_file: Path,
    started_at: float,
    max_submit_retries: int,
    download_timeout: float,
    sess: "VeoSession",
    bring_prefix: str,
    user_paygate_tier: str,
) -> Dict[str, Any]:
    """n_frames==1：页面内调用 `flowMedia:batchGenerateImages`（对齐 flow2api `FlowClient.generate_image`）。

    payload：`use_gem_pix_2` / `veo_use_gem_pix_2` 为真或 `image_model_name`/`veo_image_model` 为 GEM_PIX_2 时使用 GEM_PIX_2，否则 NARWHAL。
    `resolution` / `image_resolution` / `veo_image_resolution` 为 2k 时在生成成功后调用 `flow/upsampleImage`，最终 `share_url` 为 data URL（与 flow2api 无缓存时一致）；`origin_image_url` 仍为 1K 的 fife 直链。
    """
    image_aspect = _veo_resolve_image_aspect_ratio(payload)
    model_name = _veo_resolve_image_model_name(payload)
    res_label, want_2k = _veo_resolve_image_output_resolution(payload)
    want_i2i = _veo_payload_looks_like_i2v(payload)
    image_inputs: List[Dict[str, Any]] = []
    raw_i2i_bytes: Optional[bytes] = None

    if want_i2i:
        i2i_urls = _veo_collect_i2v_image_urls(payload)
        if len(i2i_urls) < 1:
            raise NonPenalizedTaskError(
                "图生图需要提供至少一张参考图（first_image_url / image_url / images 等）",
                status_code=400,
            )
        first_u = i2i_urls[0]
        await progress_cb(8, {"stage": "download_images", "count": 1, "workflow_kind": "image"})
        raw_i2i_bytes = await _veo_download_image_bytes_for_i2v(
            first_u,
            label="参考图",
            timeout_seconds=download_timeout,
            user_agent=None,
        )
    else:
        await progress_cb(8, {"stage": "text_to_image", "workflow_kind": "image"})
        append_log(log_file, "[veo][image] t2i (no reference images)")

    recaptcha_override = str(
        payload.get("recaptcha_token")
        or payload.get("veo_recaptcha_token")
        or payload.get("recaptchaContextToken")
        or ""
    ).strip() or None

    async def _do_veo_image_page_work() -> Dict[str, Any]:
        nonlocal res_label, recaptcha_override

        await sess.ensure_open(
            args=sess.browser_open_args,
            force_open=sess.browser_force_open,
            headless=sess.browser_headless,
            acquire_bring_lock=False,
        )
        await sess._bring_target_page_to_front(
            refresh_target=False,
            drafts_url=bring_prefix,
            acquire_bring_lock=False,
        )
        page = sess.pw_ctx.page
        if page is None:
            raise RuntimeError("page 未初始化")

        if want_i2i:
            rb = raw_i2i_bytes
            if rb is None:
                raise RuntimeError("图生图参考图数据缺失")
            await progress_cb(
                12,
                {"stage": "upload_image", "index": 1, "total": 1, "workflow_kind": "image"},
            )
            mid = await _veo_flow_upload_image_in_window(
                page=page,
                access_token=str(access_token),
                project_id=project_id,
                image_bytes=rb,
                log_file=log_file,
            )
            image_inputs.append({"name": mid, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"})
            append_log(log_file, f"[veo][image] i2i uploaded reference mediaId={safe_trim(mid, 80)!r}")

        submit_url = _veo_flow_media_batch_generate_images_url(project_id)
        last_submit_err: Optional[str] = None

        for attempt in range(max_submit_retries):
            sess._cancel_idle_close()
            recaptcha_token = recaptcha_override
            if not recaptcha_token:
                recaptcha_token = await _veo_try_recaptcha_token_in_page(
                    page, action="IMAGE_GENERATION", log_file=log_file
                )
            if not recaptcha_token:
                last_submit_err = "无法获取 reCAPTCHA token（可在 payload 传入 recaptcha_token / veo_recaptcha_token 覆盖）"
                append_log(log_file, f"[veo][image] submit attempt {attempt + 1}: no recaptcha token")
                if attempt + 1 < max_submit_retries:
                    await asyncio.sleep(2.0)
                    try:
                        await sess._bring_target_page_to_front(
                            refresh_target=False,
                            drafts_url=bring_prefix,
                            acquire_bring_lock=False,
                        )
                    except Exception:
                        pass
                continue

            session_id = _veo_generate_session_id()
            client_context: Dict[str, Any] = {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                },
                "sessionId": session_id,
                "projectId": str(project_id),
                "tool": "PINHOLE",
            }
            request_data: Dict[str, Any] = {
                "clientContext": client_context,
                "seed": random.randint(1, 999999),
                "imageModelName": model_name,
                "imageAspectRatio": image_aspect,
                "structuredPrompt": {"parts": [{"text": prompt}]},
                "imageInputs": image_inputs,
            }
            json_data: Dict[str, Any] = {
                "clientContext": client_context,
                "mediaGenerationContext": {"batchId": str(uuid.uuid4())},
                "useNewMedia": True,
                "requests": [request_data],
            }
    
            await progress_cb(
                18,
                {
                    "stage": "submit_image_task",
                    "attempt": attempt + 1,
                    "workflow_kind": "image",
                    "image_mode": "i2i" if want_i2i else "t2i",
                },
            )
    
            try:
                tx = await page_fetch_json(
                    page,
                    url=submit_url,
                    method="POST",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {access_token}",
                    },
                    json_data=json_data,
                    log_file=log_file,
                )
            except Exception as e:
                last_submit_err = _short_err_msg(e, max_len=200)
                append_log(log_file, f"[veo][image] submit fetch error: {e}")
                continue
    
            st = tx.get("status")
            if st is not None and int(st) >= 400:
                body = safe_trim(str(tx.get("response_body") or ""), 500)
                last_submit_err = f"HTTP {st} {body}"
                append_log(log_file, f"[veo][image] submit rejected: {last_submit_err}")
                recaptcha_override = None
                continue
    
            resp = tx.get("_json")
            try:
                fife_url, media_name = _veo_parse_batch_generate_images_fife_url(resp)
            except NonPenalizedTaskError as e:
                last_submit_err = str(e)
                append_log(log_file, f"[veo][image] bad response: {last_submit_err}")
                recaptcha_override = None
                continue
    
            append_log(
                log_file,
                f"[veo][image] submit ok fifeUrl={safe_trim(fife_url, 120)!r} media={safe_trim(str(media_name or ''), 60)!r}",
            )
    
            share_url = fife_url
            origin_image_url = fife_url
            upsample_ok = False
            upsample_err: Optional[str] = None
    
            if want_2k:
                sess._cancel_idle_close()
                if not media_name:
                    upsample_err = "上游未返回 media name，无法放大，已返回 1K 原图"
                    append_log(log_file, f"[veo][image] 2K requested but no mediaId: {upsample_err}")
                    res_label = "1K"
                else:
                    await progress_cb(
                        72,
                        {
                            "stage": "upsample_image",
                            "workflow_kind": "image",
                            "target_resolution": "2K",
                            "origin_image_url": fife_url,
                        },
                    )
                    upsample_retries = max(1, min(5, int(payload.get("veo_image_upsample_max_retries") or 3)))
                    try:
                        b64 = await _veo_flow_upsample_image_in_window(
                            page=page,
                            access_token=str(access_token),
                            project_id=str(project_id),
                            media_id=str(media_name),
                            user_paygate_tier=user_paygate_tier,
                            generation_session_id=str(session_id),
                            log_file=log_file,
                            max_retries=upsample_retries,
                            bring_prefix=bring_prefix,
                            sess=sess,
                            target_bring_acquire_lock=False,
                        )
                        sess._cancel_idle_close()
                        if b64:
                            oss_cfg = oss_config_from_setting_section(
                                (app_config.get_raw_config() or {}).get("oss")
                            )
                            if oss_cfg.enabled:
                                try:
                                    raw_jpeg = base64.b64decode(b64, validate=False)
                                    if not raw_jpeg:
                                        raise ValueError("base64 解码后为空")
                                    object_key = build_veo_upsample_object_key(
                                        project_id=str(project_id),
                                        media_name=str(media_name) if media_name else None,
                                    )
                                    share_url = await asyncio.to_thread(
                                        upload_bytes_to_oss,
                                        cfg=oss_cfg,
                                        data=raw_jpeg,
                                        object_key=object_key,
                                        content_type="image/jpeg",
                                    )
                                    upsample_ok = True
                                    res_label = "2K"
                                    append_log(
                                        log_file,
                                        f"[veo][image] upsample done, uploaded to oss object_key={object_key!r}",
                                    )
                                except Exception as e:
                                    upsample_err = _short_err_msg(e, max_len=240)
                                    append_log(
                                        log_file,
                                        f"[veo][image] upsample ok but oss upload failed, fallback 1K: {e}",
                                    )
                                    share_url = fife_url
                                    res_label = "1K"
                                    upsample_ok = False
                            else:
                                share_url = f"data:image/jpeg;base64,{b64}"
                                upsample_ok = True
                                res_label = "2K"
                                append_log(
                                    log_file,
                                    "[veo][image] upsample done, share_url is data:image/jpeg;base64,... (oss disabled)",
                                )
                        else:
                            upsample_err = "放大返回空数据，已返回 1K 原图"
                    except Exception as e:
                        upsample_err = _short_err_msg(e, max_len=240)
                        append_log(log_file, f"[veo][image] upsample failed, fallback 1K: {e}")
                        share_url = fife_url
                        res_label = "1K"
            sess._cancel_idle_close()
            elapsed_ms = int(max(0.0, (time.time() - started_at) * 1000.0))
            await progress_cb(
                100,
                {
                    "stage": "done",
                    "elapsed_ms": elapsed_ms,
                    "image_url": share_url,
                    "workflow_kind": "image",
                    "image_resolution": res_label,
                    "origin_image_url": origin_image_url,
                },
            )
            append_log(
                log_file,
                f"[veo][image] workflow {'I2I' if want_i2i else 'T2I'} done elapsed_ms={elapsed_ms} resolution={res_label}",
            )
            out: Dict[str, Any] = {
                "type": "veo_workflow_image",
                "message": "VEO 图生图完成" if want_i2i else "VEO 文生图完成",
                "share_url": share_url,
                "workflow_kind": "image",
                "image_mode": "i2i" if want_i2i else "t2i",
                "model_name": model_name,
                "image_aspect_ratio": image_aspect,
                "image_resolution": res_label,
                "project_id": str(project_id),
                "elapsed_ms": elapsed_ms,
                "n_frames": 1,
            }
            if want_2k or upsample_ok:
                out["upsample_url"] = share_url
            if upsample_err:
                out["upsample_error"] = upsample_err
            if media_name:
                out["generated_media_id"] = media_name
            return out

        raise NonPenalizedTaskError(last_submit_err or "图片生成提交失败", status_code=502)

    async with sess._bring_drafts_lock:
        try:
            return await _do_veo_image_page_work()
        finally:
            try:
                await sess.pw_ctx.disconnect_playwright_only()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 入口函数
# ---------------------------------------------------------------------------
async def veo_workflow(
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
    access_token: Optional[str] = None,
    access_expires: Optional[str] = None,
    headless: bool = False,
    db: Any = None,
    task_type_window_id: Optional[int] = None,
) -> Dict[str, Any]:
    """VEO：指纹浏览器页面内 fetch aisandbox API。

    - 视频：`n_frames`（或 duration / duration_frames / 时长）经 `_pick_n_frames` 归一后 **>1**（如 300/450）
      时走文生视频 / 图生视频，轮询 `batchCheckAsyncVideoGenerationStatus`。
    - 图片：当上述字段 **显式为 1** 时走文生图 / 图生图：`flow/uploadImage`（图生图仅首张）
      + `projects/{id}/flowMedia:batchGenerateImages`；默认模型 NARWHAL，`use_gem_pix_2`（或 `image_model_name`=`GEM_PIX_2`）时用 GEM_PIX_2（与 flow2api 一致）。
      横竖版对应 `IMAGE_ASPECT_RATIO_LANDSCAPE` / `IMAGE_ASPECT_RATIO_PORTRAIT`。
      `resolution` / `veo_image_resolution` 等为 **2k** 时在生成后调用 `flow/upsampleImage`：`share_url` 为 2K 的 `data:image/jpeg;base64,...`，`origin_image_url` 为 1K fife 直链；放大失败时回退为 1K 并写入 `upsample_error`。

    project_id 解析顺序：payload（veo_project_id / project_id / current_project_id）
    → veo_url 中的 /tools/flow/project/{id}
    → 若传入 db 与 task_type_window_id（task_type_windows.id），则从 veo_flow_projects 随机一条。
    """
    _ = access_expires
    payload = payload or {}
    project_id_from_db = False
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise NonPenalizedTaskError("payload.prompt 不能为空", status_code=400)

    n_frames = _veo_resolve_n_frames(payload)
    image_mode = n_frames == 1

    want_i2v = _veo_payload_looks_like_i2v(payload)
    i2v_urls: List[str] = []
    if want_i2v:
        i2v_urls = _veo_collect_i2v_image_urls(payload)
        if len(i2v_urls) == 0:
            raise NonPenalizedTaskError(
                "图生图需要提供至少一张参考图（first_image_url / image_url / images 等）"
                if image_mode
                else "图生视频需要提供 1-2 张图片（first_image_url / image_url / images 等）",
                status_code=400,
            )

    labs_hint = str(payload.get("veo_url") or payload.get("target_url") or "").strip() or "https://labs.google/fx"
    project_id = str(
        payload.get("veo_project_id") or payload.get("project_id") or payload.get("current_project_id") or ""
    ).strip()
    if not project_id:
        project_id = _veo_extract_project_id_from_url(labs_hint) or ""
    if not project_id and db is not None and task_type_window_id:
        try:
            mid = int(task_type_window_id)
            if mid > 0:
                picked_pid = await db.get_random_veo_flow_project_id(mid)
                if picked_pid:
                    project_id = str(picked_pid).strip()
                    project_id_from_db = True
        except Exception:
            project_id = project_id or ""
    if not project_id:
        raise NonPenalizedTaskError(
            "缺少 Flow projectId：请在本窗口绑定的「Veo 项目」中至少添加一个项目，"
            "或在 payload 中设置 veo_project_id（或 project_id），"
            "或让 veo_url 包含 /tools/flow/project/{id}",
            status_code=400,
        )

    monitor_log_path = str(payload.get("monitor_log_path") or "").strip() or None
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)
    max_wait_seconds = float(payload.get("veo_pending_max_wait_seconds") or max(60.0, min(float(timeout_seconds), 1800.0)))
    poll_interval_seconds = float(payload.get("veo_pending_poll_interval_seconds") or 5.0)
    max_submit_retries = max(1, min(5, int(payload.get("veo_submit_max_retries") or 3)))

    sess = get_or_create_veo_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    sess.browser_headless = headless
    sess.monitor_log_path = monitor_log_path
    sess.idle_close_seconds = idle_close_seconds

    log_file = sess._log_file
    started_at = time.time()
    bring_prefix = _veo_labs_fx_prefix_url(labs_hint)
    project_page = _veo_project_page_url(project_id=None, hint_url=labs_hint)

    if project_id_from_db and task_type_window_id:
        append_log(
            log_file,
            f"[veo] project_id from DB random pick mapping_id={int(task_type_window_id)} -> {project_id!r}",
        )
    _mode = "IMAGE" if image_mode else ("I2V" if want_i2v else "T2V")
    append_log(
        log_file,
        f"[veo] workflow {_mode} n_frames={n_frames} start project_id={project_id!r} "
        f"prompt={safe_trim(prompt, 200)!r} images={len(i2v_urls) if want_i2v else 0}",
    )
    await progress_cb(
        1,
        {
            "stage": "init",
            "workflow_kind": "image" if image_mode else "video",
            "n_frames": n_frames,
            "video_mode": "i2v" if want_i2v else "t2v",
            "prompt": safe_trim(prompt, 200),
            "project_id": project_id,
            "image_count": len(i2v_urls) if want_i2v else 0,
        },
    )

    await sess.ensure_open(headless=headless)
    append_log(log_file, "[veo] browser open / CDP connected")

    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=bring_prefix)
    sess._cancel_idle_close()
    nav_timeout_ms = int(max(15_000, min(120_000, float(timeout_seconds) * 1000)))
    #await sess.navigate_to(project_page, timeout_ms=nav_timeout_ms)
    await progress_cb(5, {"stage": "navigate", "url": project_page})
    append_log(log_file, f"[veo] navigated project page {safe_trim(project_page, 200)!r}")

    #at = str(access_token or "").strip() or None
    #if not at:
    tok_info: Optional[Dict[str, Any]] = None
    try:
        tok_info = await veo_fetch_access_token_in_window(sess=sess, target_url=project_page)
        at = str((tok_info or {}).get("access_token") or "").strip() or None
    except Exception as e:
        append_log(log_file, f"[veo] fetch access_token in window failed: {e}")
        at = None
    if not at:
        raise NonPenalizedTaskError(
            "缺少可用的 access_token：请在任务窗口映射中配置 Labs access_token，或确保指纹窗口已登录并可取得凭证",
            status_code=401,
        )
    if db is not None and task_type_window_id:
        try:
            mid = int(task_type_window_id)
            if mid > 0:
                expires = str((tok_info or {}).get("expires") or "").strip() or None
                await db.update_task_type_window(
                    mapping_id=mid,
                    sora_access_token=at,
                    sora_access_expires=expires,
                )
                append_log(log_file, f"[veo] persisted Labs access_token to task_type_window id={mid}")
        except Exception as e:
            append_log(log_file, f"[veo] persist access_token to DB failed (non-fatal): {e}")

    user_tier = _veo_normalize_user_paygate_tier(
        str(payload.get("user_paygate_tier") or payload.get("userPaygateTier") or "").strip() or None
    )
    auto_tier_v = payload.get("veo_auto_user_paygate_tier", True)
    do_credits_tier = True
    if auto_tier_v is False:
        do_credits_tier = False
    elif isinstance(auto_tier_v, str) and auto_tier_v.strip().lower() in ("0", "false", "no", "off"):
        do_credits_tier = False
    try:
        if do_credits_tier:
            cred = await veo_fetch_credits_in_window(sess=sess, target_url=project_page, access_token=at)
            t2 = (cred or {}).get("user_paygate_tier")
            if t2:
                user_tier = _veo_normalize_user_paygate_tier(str(t2))
                append_log(log_file, f"[veo] user_paygate_tier from credits: {user_tier}")
    except Exception as e:
        append_log(log_file, f"[veo] credits tier probe skipped: {e}")

    recaptcha_override = str(
        payload.get("recaptcha_token")
        or payload.get("veo_recaptcha_token")
        or payload.get("recaptchaContextToken")
        or ""
    ).strip() or None

    await asyncio.sleep(max(0.5, float(payload.get("veo_recaptcha_pre_wait_seconds") or 2.0)))

    download_timeout = float(
        payload.get("i2v_image_download_timeout_seconds")
        or payload.get("veo_image_download_timeout_seconds")
        or 60.0
    )

    if image_mode:
        return await _veo_execute_image_mode(
            payload=payload,
            progress_cb=progress_cb,
            prompt=prompt,
            project_id=project_id,
            access_token=str(at),
            log_file=log_file,
            started_at=started_at,
            max_submit_retries=max_submit_retries,
            download_timeout=download_timeout,
            sess=sess,
            bring_prefix=bring_prefix,
            user_paygate_tier=user_tier,
        )

    if want_i2v:
        aspect_ratio = _veo_resolve_i2v_aspect_ratio(payload)
        base_model_key = (
            VEO_I2V_MODEL_PORTRAIT_FL
            if aspect_ratio == VIDEO_ASPECT_RATIO_PORTRAIT
            else VEO_I2V_MODEL_LANDSCAPE_FL
        )
    else:
        base_model_key, aspect_ratio = _veo_resolve_t2v_model(payload)
    model_key = _veo_adjust_model_key_for_tier(base_model_key, user_tier)
    if model_key != base_model_key:
        append_log(log_file, f"[veo] model_key adjusted for tier: {base_model_key!r} -> {model_key!r}")

    i2v_raw_batches: List[bytes] = []
    if want_i2v:
        await progress_cb(6, {"stage": "download_images", "count": len(i2v_urls)})
        for i, u in enumerate(i2v_urls):
            label = "首帧图片" if i == 0 else "尾帧图片"
            raw_b = await _veo_download_image_bytes_for_i2v(
                u,
                label=label,
                timeout_seconds=download_timeout,
                user_agent=None,
            )
            i2v_raw_batches.append(raw_b)

    async def _do_veo_video_page_work() -> Dict[str, Any]:
        nonlocal recaptcha_override

        await sess.ensure_open(
            args=sess.browser_open_args,
            force_open=sess.browser_force_open,
            headless=sess.browser_headless,
            acquire_bring_lock=False,
        )
        await sess._bring_target_page_to_front(
            refresh_target=False,
            drafts_url=bring_prefix,
            acquire_bring_lock=False,
        )
        sess._cancel_idle_close()

        page = sess.pw_ctx.page
        if page is None:
            raise RuntimeError("page 未初始化")

        start_media_id: Optional[str] = None
        end_media_id: Optional[str] = None
        if want_i2v:
            media_ids: List[str] = []
            for i, raw_b in enumerate(i2v_raw_batches):
                label = "首帧图片" if i == 0 else "尾帧图片"
                await progress_cb(7 + i, {"stage": "upload_image", "index": i + 1, "total": len(i2v_urls)})
                mid = await _veo_flow_upload_image_in_window(
                    page=page,
                    access_token=str(at),
                    project_id=project_id,
                    image_bytes=raw_b,
                    log_file=log_file,
                )
                media_ids.append(mid)
                append_log(log_file, f"[veo][i2v] uploaded {label} mediaId={safe_trim(mid, 80)!r}")
            start_media_id = media_ids[0]
            end_media_id = media_ids[1] if len(media_ids) > 1 else None

        operations: List[Dict[str, Any]] = []
        t2v_thumb_media_name: Optional[str] = None
        last_submit_err: Optional[str] = None

        for attempt in range(max_submit_retries):
            recaptcha_token = recaptcha_override
            if not recaptcha_token:
                recaptcha_token = await _veo_try_recaptcha_token_in_page(
                    page, action="VIDEO_GENERATION", log_file=log_file
                )
            if not recaptcha_token:
                last_submit_err = "无法获取 reCAPTCHA token（可在 payload 传入 recaptcha_token / veo_recaptcha_token 覆盖）"
                append_log(log_file, f"[veo] submit attempt {attempt + 1}: no recaptcha token")
                if attempt + 1 < max_submit_retries:
                    await asyncio.sleep(2.0)
                    try:
                        await sess._bring_target_page_to_front(
                            refresh_target=False,
                            drafts_url=bring_prefix,
                            acquire_bring_lock=False,
                        )
                    except Exception:
                        pass
                    sess._cancel_idle_close()
                continue

            session_id = _veo_generate_session_id()
            scene_id = str(uuid.uuid4())

            if want_i2v:
                assert start_media_id is not None
                if end_media_id:
                    submit_url = FLOW_VIDEO_SUBMIT_I2V_START_END_URL
                    req_item: Dict[str, Any] = {
                        "aspectRatio": aspect_ratio,
                        "seed": random.randint(1, 99999),
                        "textInput": {"prompt": prompt},
                        "videoModelKey": model_key,
                        "startImage": {"mediaId": start_media_id},
                        "endImage": {"mediaId": end_media_id},
                        "metadata": {"sceneId": scene_id},
                    }
                else:
                    submit_url = FLOW_VIDEO_SUBMIT_I2V_START_IMAGE_URL
                    single_mk = _veo_strip_i2v_fl_for_single_frame(model_key)
                    append_log(
                        log_file,
                        f"[veo][i2v] single-frame model_key: {model_key!r} -> {single_mk!r}",
                    )
                    req_item = {
                        "aspectRatio": aspect_ratio,
                        "seed": random.randint(1, 99999),
                        "textInput": {"prompt": prompt},
                        "videoModelKey": single_mk,
                        "startImage": {"mediaId": start_media_id},
                        "metadata": {"sceneId": scene_id},
                    }
            else:
                submit_url = FLOW_VIDEO_SUBMIT_T2V_URL
                req_item = {
                    "aspectRatio": aspect_ratio,
                    "seed": random.randint(1, 99999),
                    "textInput": {"prompt": prompt},
                    "videoModelKey": model_key,
                    "metadata": {"sceneId": scene_id},
                }

            json_data: Dict[str, Any] = {
                "clientContext": {
                    "recaptchaContext": {
                        "token": recaptcha_token,
                        "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                    },
                    "sessionId": session_id,
                    "projectId": project_id,
                    "tool": "PINHOLE",
                    "userPaygateTier": user_tier,
                },
                "requests": [req_item],
            }

            await progress_cb(
                10,
                {
                    "stage": "submit_task",
                    "attempt": attempt + 1,
                    "video_mode": "i2v" if want_i2v else "t2v",
                },
            )
            try:
                tx = await page_fetch_json(
                    page,
                    url=submit_url,
                    method="POST",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {at}",
                    },
                    json_data=json_data,
                    log_file=log_file,
                )
            except Exception as e:
                last_submit_err = _short_err_msg(e, max_len=200)
                append_log(log_file, f"[veo] submit fetch error: {e}")
                continue

            st = tx.get("status")
            if st is not None and int(st) >= 400:
                body = safe_trim(str(tx.get("response_body") or ""), 500)
                last_submit_err = f"HTTP {st} {body}"
                append_log(log_file, f"[veo] submit rejected: {last_submit_err}")
                recaptcha_override = None
                continue

            resp = tx.get("_json")
            ops = resp.get("operations") if isinstance(resp, dict) else None
            if not isinstance(ops, list) or len(ops) == 0:
                last_submit_err = f"提交返回无 operations: {safe_trim(str(resp), 300)}"
                append_log(log_file, f"[veo] submit bad response: {last_submit_err}")
                continue

            operations = ops
            if not want_i2v:
                op0 = ops[0] if isinstance(ops[0], dict) else {}
                raw_name = (op0.get("operation") or {}).get("name")
                t2v_thumb_media_name = str(raw_name or "").strip() or None
            append_log(log_file, f"[veo] submit ok operations[0].status={ops[0].get('status')!r}")
            break
        else:
            _submit_fail = "图生视频提交失败" if want_i2v else "文生视频提交失败"
            raise NonPenalizedTaskError(last_submit_err or _submit_fail, status_code=502)

        await progress_cb(25, {"stage": "polling", "operations": len(operations)})
        sess._cancel_idle_close()
        video_url = await _veo_poll_operations_until_video_url(
            page=page,
            access_token=str(at),
            log_file=log_file,
            progress_cb=progress_cb,
            operations=operations,
            max_wait_seconds=max_wait_seconds,
            poll_interval_seconds=poll_interval_seconds,
            sess=sess,
        )

        elapsed_ms = int(max(0.0, (time.time() - started_at) * 1000.0))
        await progress_cb(100, {"stage": "done", "elapsed_ms": elapsed_ms, "video_url": video_url})
        append_log(
            log_file,
            f"[veo] workflow {_mode} done elapsed_ms={elapsed_ms} video_url={safe_trim(video_url or '', 120)!r}",
        )

        if want_i2v:
            thumb_url = i2v_urls[0]
        else:
            thumb_url = ""
            if t2v_thumb_media_name:
                qs = urlencode(
                    {
                        "name": t2v_thumb_media_name,
                        "mediaUrlType": "MEDIA_URL_TYPE_THUMBNAIL",
                    }
                )
                redirect_thumb = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?{qs}"
                thumb_url = redirect_thumb
                resolved = await page_resolve_redirect_url(page, url=redirect_thumb, log_file=log_file)
                if resolved:
                    thumb_url = resolved
                else:
                    append_log(
                        log_file,
                        "[veo] thumb redirect 解析失败，保留 labs 跳转链 URL（仅带 Cookie 的浏览器内可用）",
                    )

        out: Dict[str, Any] = {
            "type": "veo_workflow_video",
            "message": "VEO 图生视频完成" if want_i2v else "VEO 文生视频完成",
            "share_url": video_url,
            "watermark_free_url": video_url,
            "thumb_url": thumb_url,
            "video_type": "i2v" if want_i2v else "t2v",
            "model_key": model_key,
            "aspect_ratio": aspect_ratio,
            "project_id": project_id,
            "elapsed_ms": elapsed_ms,
        }
        if want_i2v:
            out["i2v_image_count"] = len(i2v_urls)
        return out

    async with sess._bring_drafts_lock:
        try:
            return await _do_veo_video_page_work()
        finally:
            try:
                await sess.pw_ctx.disconnect_playwright_only()
            except Exception:
                pass
