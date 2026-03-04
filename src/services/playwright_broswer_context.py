"""Playwright 指纹浏览器上下文（通用能力）。

注意：文件名按项目历史约定使用 `broswer`（拼写保留），用于承载“指纹浏览器自动化”的通用能力。

目标：
- 仅负责“指纹浏览器窗口自动化”的通用能力：打开/关闭窗口、连接 CDP、选择可用页面、在页面上下文里 fetch。
- 不包含任何特定站点（如 sora.chatgpt.com / jimeng.jianying.com）的业务逻辑。

说明：
- `FPBrowserClient` 是对指纹浏览器局域网 API 的适配层，本模块只调用它，**不要改动 fp_browser_client.py**。
- 站点侧执行器（如 `sora_task_executor.py` / 未来的 `jimeng_task_executor.py`）应组合使用本模块提供的上下文能力。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from .fp_browser_client import FPBrowserClient


def safe_trim(s: Optional[str], max_len: int = 300) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= max_len else s[:max_len] + "...(truncated)"


def append_log(log_file: Path, s: str) -> None:
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8", newline="\n") as f:
            f.write(s)
            if not s.endswith("\n"):
                f.write("\n")
    except Exception:
        pass


def normalize_cdp_endpoint(endpoint: str) -> str:
    """将指纹浏览器返回的 http/ws 调试地址规范化为 Playwright 可连接的 endpoint。"""
    s = (endpoint or "").strip()
    if not s:
        return ""
    if s.startswith(("http://", "https://", "ws://", "wss://")):
        return s
    # 常见返回：127.0.0.1:9222
    return "http://" + s


def _is_probably_navigable_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return True
    if u.startswith(("http://", "https://", "about:blank")):
        return True
    # 不建议用这些页面做自动化入口页（通常无法 goto 或 DOM 不符合预期）
    if u.startswith(("chrome://", "edge://", "chrome-extension://", "moz-extension://", "devtools://", "view-source:")):
        return False
    return False


async def pick_working_page_from_context(ctx) -> Any:
    """从 context.pages 中挑一个“可用页面”；否则创建新页。"""
    try:
        pages = list(getattr(ctx, "pages", []) or [])
    except Exception:
        pages = []

    best = None
    best_score = -10
    for p in pages:
        try:
            is_closed = bool(getattr(p, "is_closed", lambda: False)())
        except Exception:
            is_closed = False
        if is_closed:
            continue
        try:
            u = str(getattr(p, "url", "") or "")
        except Exception:
            u = ""
        if not _is_probably_navigable_url(u):
            continue
        score = 0
        if u.startswith(("http://", "https://")):
            score += 10
        if u.startswith("about:blank") or not u:
            score += 3
        if score > best_score:
            best_score = score
            best = p

    if best is not None:
        return best
    return await ctx.new_page()


async def page_fetch_tx(
    page,
    *,
    url: str,
    method: str,
    headers: Dict[str, str],
    json_data: Optional[Dict[str, Any]],
    log_file: Path,
) -> Dict[str, Any]:
    """在“浏览器页面上下文”里 fetch（走指纹浏览器自己的网络栈/代理/DNS），返回兼容 tx 结构。"""
    if page is None:
        raise RuntimeError("page 为 None（窗口可能已被自动回收/关闭），无法执行 page.fetch")
    tx: Dict[str, Any] = {
        "seen": True,
        "request_id": None,
        "url": url,
        "method": str(method or "GET").upper().strip(),
        "status": None,
        "response_body": None,
        "headers": None,
        "log_file": str(log_file),
    }

    blocked = {"user-agent", "host", "cookie", "content-length", "accept-encoding", "connection", "origin", "referer"}
    safe_headers: Dict[str, str] = {}
    for k, v in (headers or {}).items():
        if not k:
            continue
        lk = str(k).strip().lower()
        if lk in blocked:
            continue
        safe_headers[str(k)] = str(v)

    try:
        res = await page.evaluate(
            """async (args) => {
              const { url, method, headers, body } = args;
              const init = {
                method,
                headers: headers || {},
                credentials: 'include',
              };
              if (body !== null && body !== undefined) {
                init.body = JSON.stringify(body);
              }
              const resp = await fetch(url, init);
              const text = await resp.text();
              const hdrs = {};
              try {
                for (const [k, v] of resp.headers.entries()) hdrs[k] = v;
              } catch (e) {}
              return { status: resp.status, text, headers: hdrs };
            }""",
            {"url": url, "method": tx["method"], "headers": safe_headers, "body": json_data},
        )
    except Exception as e:
        append_log(log_file, f"[browser][page_fetch] fetch failed url={url!r} err={e}")
        raise

    try:
        tx["status"] = int((res or {}).get("status")) if (res or {}).get("status") is not None else None
    except Exception:
        tx["status"] = None
    try:
        tx["response_body"] = str((res or {}).get("text") or "")
    except Exception:
        tx["response_body"] = ""
    try:
        tx["headers"] = dict((res or {}).get("headers") or {})
    except Exception:
        tx["headers"] = None

    append_log(log_file, f"[browser][page_fetch] {tx['method']} url={url!r} status={tx['status']}")
    append_log(log_file, f"[browser][page_fetch] body={safe_trim(str(tx.get('response_body') or ''), 800)!r}")
    return tx


async def page_fetch_json(
    page,
    *,
    url: str,
    method: str,
    headers: Dict[str, str],
    json_data: Optional[Dict[str, Any]],
    log_file: Path,
) -> Dict[str, Any]:
    """页面内 fetch 并解析 JSON（失败则抛出带 response 文本摘要的异常）。"""
    tx = await page_fetch_tx(page, url=url, method=method, headers=headers, json_data=json_data, log_file=log_file)
    body = str(tx.get("response_body") or "")
    try:
        obj = json.loads(body) if body else None
    except Exception:
        obj = None
    if obj is None and (tx.get("status") not in (204,)):
        raise RuntimeError(f"fetch_json 解析失败：status={tx.get('status')} body={safe_trim(body, 600)}")
    tx["_json"] = obj
    return tx


@dataclass
class PlaywrightBrowserContext:
    """通用的 Playwright + 指纹浏览器窗口上下文。"""

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

    driver_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
        require_page: bool = True,
    ) -> None:
        self.last_used_at = time.time()

        # 优先走“复用已连接的 browser/context”。
        # 但如果用户手动关掉了指纹浏览器，缓存中的句柄会变成“非空但失效”，
        # 这里先做活性检查，避免后续在 new_page/goto 时报 TargetClosedError。
        if self.browser is not None and self.context is not None:
            browser_ok = True
            try:
                is_connected_fn = getattr(self.browser, "is_connected", None)
                if callable(is_connected_fn):
                    browser_ok = bool(is_connected_fn())
            except Exception:
                browser_ok = False

            context_ok = True
            if browser_ok:
                try:
                    _ = list(getattr(self.context, "pages", []) or [])
                except Exception:
                    context_ok = False
            else:
                context_ok = False

            if browser_ok and context_ok:
                return

            # 失效句柄：清空后走下面的重连流程
            self.browser = None
            self.context = None
            self.page = None

        try:
            from playwright.async_api import async_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(
                f"Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；错误：{e}"
            )

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

        debugger_address = normalize_cdp_endpoint(raw_endpoint)
        if not debugger_address:
            raise RuntimeError(f"无法获取 http/ws(CDP endpoint)：raw={raw_endpoint!r}")
        self.cdp_endpoint = debugger_address

        if self.playwright is None:
            self.playwright = await async_playwright().start()

        try:
            self.browser = await self.playwright.chromium.connect_over_cdp(debugger_address)
        except Exception as e:
            try:
                await self.playwright.stop()
            except Exception:
                pass
            self.playwright = None
            self.browser = None
            raise RuntimeError(f"连接指纹浏览器 CDP 失败：endpoint={debugger_address} err={e}") from e

        try:
            ctxs = list(getattr(self.browser, "contexts", []) or [])
        except Exception:
            ctxs = []
        if ctxs:
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

    async def close(self) -> None:
        self.last_used_at = time.time()
        try:
            await self.fp_client.browser_close(
                vendor=self.vendor,
                base_url=self.base_url,
                access_key=self.access_key,
                window_key=self.window_key,
            )
        except Exception:
            pass

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

    async def close_and_drop(self) -> None:
        await self.close()
        drop_ctx(self.cache_key)


_CTX_LOCK = threading.Lock()
_CTX_POOL: Dict[str, PlaywrightBrowserContext] = {}


def _ctx_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return "|".join([(vendor or "").strip().lower(), (base_url or "").strip().lower(), (space_id or "").strip(), (window_key or "").strip()])


def drop_ctx(cache_key: str) -> None:
    k = (cache_key or "").strip()
    if not k:
        return
    with _CTX_LOCK:
        _CTX_POOL.pop(k, None)


def get_or_create_ctx(
    *,
    vendor: str,
    base_url: str,
    access_key: Optional[str],
    space_id: str,
    window_key: str,
) -> PlaywrightBrowserContext:
    """获取/创建通用 PlaywrightBrowserContext（按 window 维度缓存）。"""
    k = _ctx_key(vendor, base_url, space_id, window_key)
    with _CTX_LOCK:
        ctx = _CTX_POOL.get(k)
        if ctx is None:
            ctx = PlaywrightBrowserContext(
                cache_key=k,
                vendor=(vendor or "roxy").strip().lower(),
                base_url=(base_url or "").strip().rstrip("/"),
                access_key=access_key,
                space_id=(space_id or "").strip(),
                window_key=(window_key or "").strip(),
                fp_client=FPBrowserClient(),
            )
            _CTX_POOL[k] = ctx
        else:
            ctx.access_key = access_key
        return ctx

