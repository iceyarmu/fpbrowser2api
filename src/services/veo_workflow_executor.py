"""VEO 视频生成工作流执行器。

职责边界：
- `playwright_broswer_context.py`：通用"指纹浏览器自动化层"（开窗/连CDP/挑页/page内fetch）
- `veo_workflow_executor.py`：VEO 站点侧逻辑（打开页面、提交生成任务、轮询进度、获取结果）

入口：
- `veo_workflow`（由 `task_service.py` 发起调用）
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from .playwright_broswer_context import (
    PlaywrightBrowserContext,
    acquire_browser_open_slot,
    append_log,
    get_or_create_ctx as get_or_create_playwright_ctx,
    page_fetch_json,
    page_fetch_tx,
    pick_working_page_from_context,
    safe_trim,
)
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
    ) -> None:
        """确保指纹浏览器窗口已打开、CDP 已连接。"""
        self.last_used_at = time.time()
        async with self._bring_drafts_lock:
            await self.pw_ctx.ensure_open(args=args or [], force_open=force_open, headless=headless, require_page=False)

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
    async def _bring_target_page_to_front(self, refresh_target=True, *, drafts_url: str) -> None:
        """将目标页面置前，并尽量确保整个指纹浏览器实例只保留一个目标页面。

        需求背景：指纹浏览器可能开了多个标签页/窗口；即使 ensure_open 选中了可用 page，也不一定是目标页面。
        这里确保 drafts_url 在每次 ensure_open 后都会被 bring_to_front，
        且会关闭同一个指纹浏览器（同一 CDP 连接）内除 drafts_url 外的其它页面（包括其它站点、about:blank、新窗口、重复 drafts 等），
        尽量只保留一个目标页面以节省内存。
        """
        try:
            target_host = urlparse(drafts_url).netloc.strip().lower()
        except Exception:
            target_host = ""

        async with self._bring_drafts_lock:
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


# ---------------------------------------------------------------------------
# Google Labs / Flow：余额与档位（与 flow2api flow_client.get_credits 对齐）
# ---------------------------------------------------------------------------
FLOW_LABS_CREDITS_URL = "https://aisandbox-pa.googleapis.com/v1/credits"


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

    await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless)
    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url)
    sess._cancel_idle_close()

    async with sess._bring_drafts_lock:
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
    await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless)
    await sess._bring_target_page_to_front(refresh_target=False, drafts_url=target_url)
    sess._cancel_idle_close()

    async with sess._bring_drafts_lock:
        if sess.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")

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
    headless: bool = False,
) -> Dict[str, Any]:
    """VEO 视频生成工作流：复用同一指纹浏览器窗口 + Playwright(CDP) 轻量连接。

    流程框架：
    1. 打开指纹浏览器 / 复用已有 session
    2. 导航到 VEO 目标页面
    3. 提交视频生成任务
    4. 轮询任务进度
    5. 获取最终视频 URL 并返回
    """

    payload = payload or {}
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise NonPenalizedTaskError("payload.prompt 不能为空", status_code=400)

    # 从 payload 提取可选参数
    target_url = str(payload.get("veo_url") or payload.get("target_url") or "").strip()
    monitor_log_path = str(payload.get("monitor_log_path") or "").strip() or None
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)
    max_wait_seconds = float(payload.get("veo_pending_max_wait_seconds") or max(30.0, min(timeout_seconds, 600.0)))
    poll_interval_seconds = float(payload.get("veo_pending_poll_interval_seconds") or 5.0)

    # ── Step 1：获取/创建 VEO 会话 ──
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

    append_log(log_file, f"[veo] workflow start prompt={safe_trim(prompt, 200)!r}")
    await progress_cb(1, {"stage": "init", "prompt": safe_trim(prompt, 200)})

    # ── Step 2：打开指纹浏览器窗口 & 建立 CDP 连接 ──
    await sess.ensure_open(headless=headless)
    await progress_cb(5, {"stage": "browser_open"})
    append_log(log_file, "[veo] browser open / CDP connected")

    # ── Step 3：导航到 VEO 目标页面 ──
    if target_url:
        nav_timeout_ms = int(max(10_000, min(60_000, timeout_seconds * 1000)))
        await sess.navigate_to(target_url, timeout_ms=nav_timeout_ms)
        await progress_cb(10, {"stage": "navigate", "url": target_url})
        append_log(log_file, f"[veo] navigated to {safe_trim(target_url, 200)!r}")

    # ── Step 4：提交视频生成任务（TODO: 对接 VEO 实际 API） ──
    await progress_cb(15, {"stage": "submit_task", "prompt": safe_trim(prompt, 200)})
    append_log(log_file, "[veo] TODO: submit generation task")

    # TODO: 在此处实现实际的 VEO 任务提交逻辑
    # 示例：
    # tx = await sess.page_fetch(
    #     "https://veo-api-endpoint/generate",
    #     method="POST",
    #     json_data={"prompt": prompt, ...},
    # )
    # task_id = tx.get("_json", {}).get("task_id")

    # ── Step 5：轮询任务进度（TODO: 对接 VEO 实际轮询接口） ──
    await progress_cb(20, {"stage": "polling"})
    append_log(log_file, "[veo] TODO: poll task progress")

    # TODO: 在此处实现轮询逻辑
    # deadline = time.time() + max_wait_seconds
    # while time.time() < deadline:
    #     status = await sess.page_fetch(f"https://veo-api-endpoint/status/{task_id}")
    #     data = status.get("_json", {})
    #     if data.get("status") == "completed":
    #         video_url = data.get("video_url")
    #         break
    #     await asyncio.sleep(poll_interval_seconds)

    # ── Step 6：返回结果 ──
    elapsed_ms = int(max(0.0, (time.time() - started_at) * 1000.0))
    await progress_cb(100, {"stage": "done", "elapsed_ms": elapsed_ms})
    append_log(log_file, f"[veo] workflow done elapsed_ms={elapsed_ms}")

    return {
        "type": "veo_workflow",
        "message": "VEO 视频生成完成",
        "video_url": None,  # TODO: 替换为实际视频 URL
        "elapsed_ms": elapsed_ms,
    }
