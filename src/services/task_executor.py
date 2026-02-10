"""任务执行器（当前先实现“可运行的模拟执行”）。

你后续要接入真实自动化（例如 Playwright + 指纹浏览器窗口启动）时，
只需要在这里把 `simulate_*` 替换成真实执行逻辑，并持续调用 `progress_cb` 更新进度即可。
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from .fp_browser_client import FPBrowserClient


ProgressCB = Callable[[int, Optional[Dict[str, Any]]], Awaitable[None]]


class NonPenalizedTaskError(RuntimeError):
    """失败但不计入窗口连续错误（consecutive_errors）的异常。

    用途：Sora 创建阶段常见的 400/invalid_request 等错误，以及“未监控到 POST 请求”等，
    这类错误不应导致窗口被连续错误熔断。
    """

    no_penalty: bool = True

    def __init__(self, message: str, *, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


async def simulate_image_task(prompt: str, image_path: Optional[str], progress_cb: ProgressCB) -> Dict[str, Any]:
    # 约 8 秒完成
    for p in (5, 15, 30, 45, 60, 75, 90):
        await asyncio.sleep(0.8)
        await progress_cb(p, None)
    await asyncio.sleep(0.8)
    await progress_cb(100, None)
    return {
        "type": "image",
        "message": "模拟执行完成（请在 task_executor.py 接入真实自动化）",
        "prompt": prompt,
        "image_path": image_path,
        "outputs": [],
    }


async def simulate_video_task(prompt: str, image_path: Optional[str], progress_cb: ProgressCB) -> Dict[str, Any]:
    # 约 25 秒完成
    for p in (3, 8, 15, 25, 35, 45, 55, 65, 75, 85, 92, 96):
        await asyncio.sleep(2.0)
        await progress_cb(p, None)
    await asyncio.sleep(1.5)
    await progress_cb(100, None)
    return {
        "type": "video",
        "message": "模拟执行完成（请在 task_executor.py 接入真实自动化）",
        "prompt": prompt,
        "image_path": image_path,
        "outputs": [],
    }

def _safe_trim(s: Optional[str], max_len: int = 300) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= max_len else s[:max_len] + "...(truncated)"


def _append_log(log_file: Path, s: str) -> None:
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8", newline="\n") as f:
            f.write(s)
            if not s.endswith("\n"):
                f.write("\n")
    except Exception:
        pass


def _normalize_cdp_endpoint(endpoint: str) -> str:
    """将指纹浏览器返回的 http/ws 调试地址规范化为 Playwright 可连接的 endpoint。"""
    s = (endpoint or "").strip()
    if not s:
        return ""
    if s.startswith(("http://", "https://", "ws://", "wss://")):
        return s
    # 常见返回：127.0.0.1:9222
    return "http://" + s


async def _debug_dump_span_and_button_texts_pw(page, *, max_items: int = 40) -> None:
    """找不到按钮时，输出页面上部分 span/button 文本，帮助判断文案/语言/登录态。"""
    try:
        spans = await page.eval_on_selector_all(
            "span",
            """(els, maxItems) => els
              .map(e => (e.textContent || '').trim())
              .filter(t => t)
              .slice(0, maxItems)""",
            max_items,
        )
        buttons = await page.eval_on_selector_all(
            "button",
            """(els, maxItems) => els
              .map(e => {
                const t = (e.textContent || '').trim();
                const aria = (e.getAttribute('aria-label') || '').trim();
                const title = (e.getAttribute('title') || '').trim();
                return [t, aria, title].filter(Boolean).join(' / ');
              })
              .filter(t => t)
              .slice(0, maxItems)""",
            max_items,
        )
    except Exception:
        return

    try:
        print("=== 调试：页面 span 文本采样 ===")
        for t in spans or []:
            print("-", _safe_trim(str(t), 120))
        print("=== 调试：页面 button 文本采样 ===")
        for t in buttons or []:
            print("-", _safe_trim(str(t), 160))
    except Exception:
        return


def _normalize_progress(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
    except Exception:
        return None
    if fv > 1.0:
        return fv / 100.0
    return fv


def _extract_task_obj(payload: Any, task_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(payload, list):
        for it in payload:
            if isinstance(it, dict) and str(it.get("id", "")) == str(task_id):
                return it
        return None
    if isinstance(payload, dict):
        for k in ["data", "rows", "items", "tasks"]:
            v = payload.get(k)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict) and str(it.get("id", "")) == str(task_id):
                        return it
        return None
    return None


async def _sniff_http_transaction_pw(
    page,
    *,
    url_regex: str,
    method: Optional[str] = None,
    timeout_seconds: float = 15.0,
    log_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Playwright 轻量抓包：等待命中 url_regex 的请求响应，并返回 {seen,url,method,status,headers,response_body,log_file}。"""
    url_pat = re.compile(url_regex, flags=re.IGNORECASE)
    method_norm = method.upper().strip() if method else None
    log_file = Path(log_path) if log_path else (Path(__file__).resolve().parent / "logs.txt")

    result: Dict[str, Any] = {
        "seen": False,
        "request_id": None,  # Playwright 不暴露 requestId，保留字段以兼容旧结构
        "url": None,
        "method": None,
        "status": None,
        "response_body": None,
        "headers": None,
        "log_file": str(log_file),
    }

    _append_log(log_file, "\n" + "=" * 100)
    _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] sniff_start url_regex={url_regex!r} method={method_norm!r}")

    def _pred(resp) -> bool:
        try:
            if not url_pat.search(str(resp.url or "")):
                return False
            m = str(resp.request.method or "").upper().strip()
            if method_norm and m != method_norm:
                return False
            return True
        except Exception:
            return False

    try:
        resp = await page.wait_for_response(_pred, timeout=max(1.0, float(timeout_seconds)) * 1000.0)
    except Exception:
        _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] sniff_end (timeout/no match)")
        return result

    try:
        result["seen"] = True
        result["url"] = str(getattr(resp, "url", "") or "")
        result["method"] = str(getattr(resp.request, "method", "") or "").upper().strip()
        try:
            result["status"] = int(getattr(resp, "status", None))
        except Exception:
            result["status"] = None
        try:
            result["headers"] = dict(resp.headers or {})
        except Exception:
            result["headers"] = None

        _append_log(log_file, "-" * 100)
        _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] response url={result['url']} status={result['status']} method={result['method']}")
        try:
            pd = getattr(resp.request, "post_data", None)
            if pd:
                _append_log(log_file, "postData:")
                _append_log(log_file, str(pd))
        except Exception:
            pass

        body_text = ""
        try:
            body_text = await resp.text()
        except Exception:
            try:
                b = await resp.body()
                body_text = b.decode("utf-8", errors="replace")
            except Exception:
                body_text = ""
        result["response_body"] = body_text
        _append_log(log_file, "responseBody:")
        _append_log(log_file, str(body_text))
    finally:
        _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] sniff_end")

    return result


async def _response_to_tx_pw(resp, *, log_path: Optional[str]) -> Dict[str, Any]:
    """将 Playwright Response 转成旧 sniff_* 兼容结构，并写入日志文件。"""
    log_file = Path(log_path) if log_path else (Path(__file__).resolve().parent / "logs.txt")
    tx: Dict[str, Any] = {
        "seen": True,
        "request_id": None,
        "url": None,
        "method": None,
        "status": None,
        "response_body": None,
        "headers": None,
        "log_file": str(log_file),
    }

    try:
        tx["url"] = str(getattr(resp, "url", "") or "")
    except Exception:
        tx["url"] = None

    try:
        tx["method"] = str(getattr(resp.request, "method", "") or "").upper().strip()
    except Exception:
        tx["method"] = None

    try:
        tx["status"] = int(getattr(resp, "status", None))
    except Exception:
        tx["status"] = None

    try:
        tx["headers"] = dict(resp.headers or {})
    except Exception:
        tx["headers"] = None

    body_text = ""
    try:
        body_text = await resp.text()
    except Exception:
        try:
            b = await resp.body()
            body_text = b.decode("utf-8", errors="replace")
        except Exception:
            body_text = ""

    tx["response_body"] = body_text

    _append_log(log_file, "\n" + "=" * 100)
    _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] response")
    _append_log(log_file, f"url: {tx['url']}")
    _append_log(log_file, f"method: {tx['method']}")
    _append_log(log_file, f"status: {tx['status']}")
    try:
        pd = getattr(resp.request, "post_data", None)
        if pd:
            _append_log(log_file, "postData:")
            _append_log(log_file, str(pd))
    except Exception:
        pass
    _append_log(log_file, "responseBody:")
    _append_log(log_file, str(body_text))
    _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] end")

    return tx


async def _pw_pick_first_visible(loc, *, max_items: int = 12):
    """从 locator 列表中挑选第一个可见元素。返回 locator.nth(i) 或 None。"""
    try:
        cnt = await loc.count()
    except Exception:
        cnt = 0
    n = max(0, min(int(cnt or 0), int(max_items)))
    for i in range(n):
        it = loc.nth(i)
        try:
            if await it.is_visible():
                return it
        except Exception:
            continue
    return None


async def _pw_get_editable_value(el) -> str:
    """尽量读取输入控件当前值（textarea/input/contenteditable/role=textbox）。"""
    # textarea/input
    try:
        v = await el.input_value()
        if v is not None:
            return str(v)
    except Exception:
        pass
    # 通用：value / innerText / textContent
    try:
        v = await el.evaluate(
            """(e) => {
              try {
                if (typeof e.value === 'string') return e.value;
              } catch (err) {}
              try {
                if (e.isContentEditable) return (e.innerText || '');
              } catch (err) {}
              return (e.textContent || '');
            }"""
        )
        return str(v or "")
    except Exception:
        return ""


def _pw_list_frames(page) -> list[Any]:
    """安全获取 page.frames 列表（包含主 frame）。"""
    try:
        frames = list(getattr(page, "frames", []) or [])
    except Exception:
        frames = []
    # Playwright 的 page.frames 通常包含 main_frame；这里兜底确保至少有一个可用对象
    if not frames:
        try:
            mf = getattr(page, "main_frame", None)
            if mf is not None:
                frames = [mf]
        except Exception:
            frames = []
    return frames


async def _pw_debug_dump_page_overview(page, *, log_file: Path, max_text: int = 600) -> None:
    """当关键元素找不到时，写入页面/frames 的概览到日志，辅助判断登录态/重定向/拦截页。"""
    try:
        url = str(getattr(page, "url", "") or "")
    except Exception:
        url = ""
    try:
        title = await page.title()
    except Exception:
        title = ""
    _append_log(log_file, f"[sora][debug] page url={_safe_trim(url, 240)!r} title={_safe_trim(title, 240)!r}")

    frames = _pw_list_frames(page)
    _append_log(log_file, f"[sora][debug] frames count={len(frames)}")
    for idx, fr in enumerate(frames[:8]):
        try:
            fr_url = str(getattr(fr, "url", "") or "")
        except Exception:
            fr_url = ""
        _append_log(log_file, f"[sora][debug] frame[{idx}] url={_safe_trim(fr_url, 260)!r}")
        # body 文本采样（可能跨域/不可访问，失败则跳过）
        try:
            t = await fr.locator("body").inner_text(timeout=1500)
            t = (t or "").strip()
            if t:
                _append_log(log_file, f"[sora][debug] frame[{idx}] body_text_sample={_safe_trim(t, max_text)!r}")
        except Exception:
            pass


async def _pw_find_prompt_candidate_in_frame(fr) -> tuple[Optional[str], Any]:
    """在指定 frame 内寻找可见的 prompt 输入控件。返回 (kind, locator) 或 (None, None)。"""
    # 候选输入控件：Sora UI 可能是 textarea，也可能是 contenteditable/role=textbox/input
    candidates: list[tuple[str, Any]] = [
        ("textarea", fr.locator("textarea")),
        ('role=textbox', fr.get_by_role("textbox")),
        ('div[role="textbox"]', fr.locator('div[role="textbox"]')),
        ('[contenteditable="true"]', fr.locator('[contenteditable="true"]')),
        # 兜底：普通 input（有些 UI 会用 input+自动扩展）
        ('input[type="text/search"]', fr.locator('input[type="text"], input[type="search"], input:not([type])')),
        # 兜底：通过 placeholder/aria-label/data-testid 关键词匹配
        (
            "prompt_hint_attrs",
            fr.locator(
                '[placeholder*="Describe" i], [placeholder*="Prompt" i], [placeholder*="描述" i], [placeholder*="提示" i], '
                '[aria-label*="Describe" i], [aria-label*="Prompt" i], [aria-label*="描述" i], '
                '[data-testid*="prompt" i], [name*="prompt" i]'
            ),
        ),
    ]

    for k, loc in candidates:
        el = await _pw_pick_first_visible(loc)
        if el is not None:
            return k, el
    return None, None


def _pw_is_probably_navigable_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return True
    if u.startswith(("http://", "https://", "about:blank")):
        return True
    # 不建议用这些页面做自动化入口页（通常无法 goto 或 DOM 不符合预期）
    if u.startswith(("chrome://", "edge://", "chrome-extension://", "moz-extension://", "devtools://", "view-source:")):
        return False
    return False


async def _pw_pick_working_page_from_context(ctx) -> Any:
    """从 context.pages 中挑一个“可用页面”；否则创建新页。"""
    try:
        pages = list(getattr(ctx, "pages", []) or [])
    except Exception:
        pages = []

    best = None
    best_score = -10
    for p in pages:
        try:
            u = str(getattr(p, "url", "") or "")
        except Exception:
            u = ""
        if not _pw_is_probably_navigable_url(u):
            continue
        score = 0
        if u.startswith(("http://", "https://")):
            score += 10
        if u.startswith("about:blank") or not u:
            score += 3
        # 尽量避开“空白但已打开很久”的页（无法可靠判断，这里保守给小分）
        if score > best_score:
            best_score = score
            best = p

    if best is not None:
        return best
    return await ctx.new_page()


async def _sora_fill_prompt_pw(page, *, prompt: str, log_file: Path) -> Dict[str, Any]:
    """多策略写入 prompt，并做读回校验；返回调试信息 dict。"""
    info: Dict[str, Any] = {
        "ok": False,
        "kind": None,
        "value_len": 0,
        "value_sample": "",
        "frame_url": None,
        "frame_idx": None,
    }

    # 先确保 body 出现（有些页面 domcontentloaded 但 body/主 UI 还没挂载）
    try:
        await page.wait_for_selector("body", timeout=20_000)
    except Exception:
        pass

    # 在所有 frames 中找输入框（包含 main frame + iframes）
    # 注意：Sora 前端可能需要一点时间渲染，因此轮询等待一段时间
    el = None
    kind = None
    fr_url = None
    fr_idx: Optional[int] = None
    deadline = time.time() + 25.0
    while time.time() < deadline and el is None:
        frames = _pw_list_frames(page)
        for i, fr in enumerate(frames):
            k, candidate = await _pw_find_prompt_candidate_in_frame(fr)
            if candidate is not None:
                el = candidate
                kind = k
                try:
                    fr_url = str(getattr(fr, "url", "") or "")
                except Exception:
                    fr_url = None
                fr_idx = i
                break
        if el is None:
            # 给 SPA 渲染一点时间；同时尽量等一次 networkidle（失败则忽略）
            try:
                await page.wait_for_load_state("networkidle", timeout=1500)
            except Exception:
                pass
            await page.wait_for_timeout(350)

    if el is None:
        await _debug_dump_span_and_button_texts_pw(page, max_items=40)
        await _pw_debug_dump_page_overview(page, log_file=log_file)
        # 额外：截图与 HTML 片段（帮助判断是否登录页/拦截页/空白页/被重定向）
        try:
            png = log_file.with_suffix(".prompt_not_found.png")
            await page.screenshot(path=str(png), full_page=True)
            _append_log(log_file, f"[sora][debug] screenshot_saved={str(png)!r}")
        except Exception as e:
            _append_log(log_file, f"[sora][debug] screenshot_failed={e}")
        try:
            html = await page.content()
            _append_log(log_file, f"[sora][debug] html_sample={_safe_trim(html, 1200)!r}")
        except Exception as e:
            _append_log(log_file, f"[sora][debug] page.content failed: {e}")

        # 这类错误大概率是登录态/权限/站点变化/拦截页导致，不应计入窗口连续错误
        raise NonPenalizedTaskError("未找到可用的 prompt 输入框（textarea/textbox/contenteditable/input/placeholder 均未命中）")

    # 尝试 fill（优先），失败再退回键盘输入
    async def _verify() -> bool:
        cur = (await _pw_get_editable_value(el)).strip()
        info["value_len"] = len(cur or "")
        info["value_sample"] = _safe_trim(cur, 120)
        return (cur == (prompt or "").strip()) and bool(cur)

    try:
        await el.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        await el.click(timeout=10_000)
    except Exception:
        pass

    filled = False
    try:
        # 尽量先清空再写入
        try:
            await el.fill("")
        except Exception:
            pass
        await el.fill(prompt)
        filled = True
    except Exception:
        filled = False

    if not filled or not await _verify():
        # 键盘兜底：Ctrl+A Backspace + insert_text
        try:
            await el.click(timeout=10_000)
        except Exception:
            pass
        try:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
        except Exception:
            pass
        try:
            # insert_text 比 type 更像“粘贴”，更稳定且更快
            await page.keyboard.insert_text(prompt)
        except Exception:
            # 最后再退到 type
            try:
                await page.keyboard.type(prompt, delay=5)
            except Exception:
                pass

    # 触发一次 input/change 事件（部分 SPA 需要）
    try:
        await el.dispatch_event("input")
    except Exception:
        pass
    try:
        await el.dispatch_event("change")
    except Exception:
        pass

    # 给前端一点时间更新按钮状态
    await page.wait_for_timeout(300)

    ok = await _verify()
    info["ok"] = bool(ok)
    info["kind"] = kind
    info["frame_url"] = fr_url
    info["frame_idx"] = fr_idx
    _append_log(
        log_file,
        f"[sora] prompt_fill kind={kind!r} frame_idx={fr_idx!r} frame_url={_safe_trim(str(fr_url or ''), 220)!r} ok={info['ok']} "
        f"value_len={info['value_len']} sample={info['value_sample']!r}",
    )
    return info


async def _pw_is_actionable_button(btn) -> bool:
    try:
        if not await btn.is_visible():
            return False
    except Exception:
        return False
    # is_enabled 对非原生 button 有时会抛异常；所以多策略判断
    try:
        if await btn.is_enabled():
            return True
    except Exception:
        pass
    try:
        aria_disabled = await btn.get_attribute("aria-disabled")
        if str(aria_disabled or "").strip().lower() in ("true", "1", "yes"):
            return False
    except Exception:
        pass
    try:
        disabled = await btn.get_attribute("disabled")
        if disabled is not None:
            return False
    except Exception:
        pass
    # 兜底：可见即认为可点（交由 click 处理）
    return True


async def _pw_wait_button_actionable(btn, *, timeout_seconds: float, log_file: Path) -> None:
    deadline = time.time() + max(0.5, float(timeout_seconds))
    last_reason = ""
    while time.time() < deadline:
        try:
            await btn.wait_for(state="visible", timeout=2_000)
        except Exception:
            last_reason = "not_visible"
            await asyncio.sleep(0.2)
            continue
        try:
            if await _pw_is_actionable_button(btn):
                return
            last_reason = "not_actionable"
        except Exception:
            last_reason = "check_failed"
        await asyncio.sleep(0.2)
    _append_log(log_file, f"[sora] wait_button_actionable timeout reason={last_reason}")


async def _pw_click_button_robust(page, btn, *, log_file: Path) -> None:
    """多策略点击（普通→force→dispatch_event→js click）。"""
    try:
        await btn.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        await btn.click(timeout=10_000)
        _append_log(log_file, "[sora] click: locator.click ok")
        return
    except Exception as e1:
        _append_log(log_file, f"[sora] click: locator.click failed: {e1}")

    try:
        await btn.click(timeout=10_000, force=True)
        _append_log(log_file, "[sora] click: locator.click(force) ok")
        return
    except Exception as e2:
        _append_log(log_file, f"[sora] click: locator.click(force) failed: {e2}")

    try:
        await btn.dispatch_event("click")
        _append_log(log_file, "[sora] click: dispatch_event ok")
        return
    except Exception as e3:
        _append_log(log_file, f"[sora] click: dispatch_event failed: {e3}")

    # 坐标点击兜底：某些复杂 UI 覆盖层会导致 locator.click 行为异常
    try:
        box = await btn.bounding_box()
    except Exception:
        box = None
    if box and box.get("width") and box.get("height"):
        try:
            x = float(box["x"]) + float(box["width"]) / 2.0
            y = float(box["y"]) + float(box["height"]) / 2.0
            await page.mouse.click(x, y, delay=50)
            _append_log(log_file, f"[sora] click: mouse.click center=({x:.1f},{y:.1f}) ok")
            return
        except Exception as e5:
            _append_log(log_file, f"[sora] click: mouse.click failed: {e5}")

    # 最后：JS click（需要 elementHandle）
    try:
        h = await btn.element_handle()
    except Exception:
        h = None
    if h is not None:
        try:
            await page.evaluate("(el) => el.click()", h)
            _append_log(log_file, "[sora] click: js el.click() ok")
            return
        except Exception as e4:
            _append_log(log_file, f"[sora] click: js el.click() failed: {e4}")

    raise RuntimeError("create 按钮点击失败（多策略点击均失败）")


async def _pw_debug_dump_clickables_pw(page, *, log_file: Path, max_items: int = 60) -> None:
    """把页面各 frame 中可疑 button/role=button 文本与属性写入日志（用于定位真实按钮文案/属性）。"""
    frames = _pw_list_frames(page)
    _append_log(log_file, f"[sora][debug] dump_clickables frames={len(frames)} max_items={max_items}")
    for idx, fr in enumerate(frames[:10]):
        try:
            fr_url = str(getattr(fr, "url", "") or "")
        except Exception:
            fr_url = ""
        _append_log(log_file, f"[sora][debug] frame[{idx}] url={_safe_trim(fr_url, 260)!r}")
        try:
            items = await fr.eval_on_selector_all(
                "button, [role='button']",
                """(els, maxItems) => {
                  const out = [];
                  for (const e of els) {
                    if (out.length >= maxItems) break;
                    try {
                      const t = (e.textContent || '').trim();
                      const aria = (e.getAttribute('aria-label') || '').trim();
                      const title = (e.getAttribute('title') || '').trim();
                      const tid = (e.getAttribute('data-testid') || '').trim();
                      const dis = e.hasAttribute('disabled');
                      const ariaDis = (e.getAttribute('aria-disabled') || '').trim();
                      const tag = (e.tagName || '').toLowerCase();
                      const typ = (e.getAttribute('type') || '').trim();
                      const cls = (e.getAttribute('class') || '').trim();
                      const s = [
                        `tag=${tag}`,
                        typ ? `type=${typ}` : '',
                        t ? `text=${t}` : '',
                        aria ? `aria=${aria}` : '',
                        title ? `title=${title}` : '',
                        tid ? `testid=${tid}` : '',
                        dis ? 'disabled=true' : '',
                        ariaDis ? `aria-disabled=${ariaDis}` : '',
                        cls ? `class=${cls}` : '',
                      ].filter(Boolean).join(' | ');
                      if (s) out.push(s);
                    } catch (err) {}
                  }
                  return out;
                }""",
                max_items,
            )
        except Exception:
            items = []
        for it in items or []:
            _append_log(log_file, f"[sora][debug] - {_safe_trim(str(it), 500)}")


async def _pw_focus_prompt_input_pw(page, *, log_file: Path) -> None:
    """尽量把焦点放回 prompt 输入框，便于键盘提交。"""
    frames = _pw_list_frames(page)
    for fr in frames:
        k, el = await _pw_find_prompt_candidate_in_frame(fr)
        if el is None:
            continue
        try:
            await el.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            await el.click(timeout=5_000)
            _append_log(log_file, f"[sora] focus_prompt ok kind={k!r}")
            return
        except Exception:
            continue
    _append_log(log_file, "[sora] focus_prompt failed (no candidate clickable)")


async def _pw_find_create_button_pw(
    page,
    *,
    primary_regex: str,
    log_file: Path,
    timeout_seconds: float = 12.0,
    prefer_frame_idx: Optional[int] = None,
) -> Any:
    """跨 frame 查找 create/submit 按钮。优先 primary_regex，其次常见兜底关键词。返回 locator 或 None。"""
    deadline = time.time() + max(0.5, float(timeout_seconds))
    # primary: 用户传入（例如 Create video）
    try:
        primary_pat = re.compile(primary_regex, flags=re.IGNORECASE)
    except Exception:
        primary_pat = re.compile(re.escape(str(primary_regex or "")), flags=re.IGNORECASE)

    # fallback：常见文案（尽量不太激进）
    fallback_pats = [
        re.compile(r"^\s*Create\s+video\s*$", flags=re.IGNORECASE),
        re.compile(r"\bCreate\b", flags=re.IGNORECASE),
        re.compile(r"\bGenerate\b", flags=re.IGNORECASE),
        re.compile(r"\bSend\b", flags=re.IGNORECASE),
        re.compile(r"创建\s*视频|生成\s*视频|创建|生成|发送|提交", flags=re.IGNORECASE),
    ]

    # 常见 icon 按钮会放在 aria-label/title/testid 里
    attr_css = [
        # create / generate / send
        "button[aria-label*='create' i], button[title*='create' i], button[data-testid*='create' i]",
        "button[aria-label*='generate' i], button[title*='generate' i], button[data-testid*='generate' i]",
        "button[aria-label*='send' i], button[title*='send' i], button[data-testid*='send' i]",
        "[role='button'][aria-label*='create' i], [role='button'][title*='create' i], [role='button'][data-testid*='create' i]",
        "[role='button'][aria-label*='send' i], [role='button'][title*='send' i], [role='button'][data-testid*='send' i]",
        # 中文关键字
        "button[aria-label*='生成' i], button[aria-label*='创建' i], button[aria-label*='发送' i]",
        "button[title*='生成' i], button[title*='创建' i], button[title*='发送' i]",
        "[role='button'][aria-label*='生成' i], [role='button'][aria-label*='创建' i], [role='button'][aria-label*='发送' i]",
    ]

    while time.time() < deadline:
        frames = _pw_list_frames(page)
        # 优先在 prompt 所在 frame 查找（更贴近真实提交按钮所在区域）
        if prefer_frame_idx is not None:
            try:
                i = int(prefer_frame_idx)
            except Exception:
                i = -1
            if 0 <= i < len(frames):
                frames = [frames[i]] + [f for j, f in enumerate(frames) if j != i]
        for fr in frames:
            # 1) primary regex by role/name
            try:
                loc = fr.get_by_role("button", name=primary_pat)
                el = await _pw_pick_first_visible(loc)
                if el is not None:
                    return el
            except Exception:
                pass
            # 2) primary regex by text
            try:
                loc = fr.locator("button").filter(has_text=primary_pat)
                el = await _pw_pick_first_visible(loc)
                if el is not None:
                    return el
            except Exception:
                pass

            # 3) fallback patterns
            for pat in fallback_pats:
                try:
                    loc = fr.get_by_role("button", name=pat)
                    el = await _pw_pick_first_visible(loc)
                    if el is not None:
                        return el
                except Exception:
                    pass
                try:
                    loc = fr.locator("button").filter(has_text=pat)
                    el = await _pw_pick_first_visible(loc)
                    if el is not None:
                        return el
                except Exception:
                    pass

            # 4) attribute-based css (icon button)
            for css in attr_css:
                try:
                    loc = fr.locator(css)
                    el = await _pw_pick_first_visible(loc)
                    if el is not None:
                        return el
                except Exception:
                    pass

        await page.wait_for_timeout(350)

    _append_log(log_file, f"[sora] create_button not found within {timeout_seconds}s regex={primary_regex!r}")
    return None


async def _pw_log_recent_posts(page, *, seconds: float, log_file: Path) -> None:
    """记录短时间内页面发出的所有 POST 请求 URL（用于判断是否真的触发了提交以及真实接口路径）。"""
    secs = max(0.2, float(seconds))
    seen: list[str] = []

    def _on_request(req) -> None:
        try:
            m = str(getattr(req, "method", "") or "").upper().strip()
            if m != "POST":
                return
            u = str(getattr(req, "url", "") or "")
            if u:
                seen.append(u)
        except Exception:
            return

    try:
        page.on("request", _on_request)
    except Exception:
        return
    try:
        await page.wait_for_timeout(int(secs * 1000))
    finally:
        try:
            page.off("request", _on_request)
        except Exception:
            pass

    if not seen:
        _append_log(log_file, f"[sora][debug] recent_posts({secs:.1f}s): none")
        return
    # 去重保序
    uniq: list[str] = []
    for u in seen:
        if u not in uniq:
            uniq.append(u)
    _append_log(log_file, f"[sora][debug] recent_posts({secs:.1f}s) count={len(uniq)}")
    for u in uniq[:40]:
        _append_log(log_file, f"[sora][debug] POST {u}")


async def _sora_create_task_pw(
    *,
    page,
    prompt: str,
    target_url: str,
    create_button_text_regex: str,
    monitor_seconds: float,
    monitor_url_regex: str,
    monitor_log_path: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    """在页面上创建任务并抓取 POST /backend/nf/create 响应，返回 task_id。"""
    log_file = Path(monitor_log_path) if monitor_log_path else (Path(__file__).resolve().parent / "logs.txt")
    await page.goto(target_url, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass

    _append_log(log_file, "\n" + "=" * 100)
    _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [sora] create_task start url={target_url!r}")
    _append_log(log_file, f"[sora] monitor_seconds={monitor_seconds} monitor_url_regex={monitor_url_regex!r} create_btn_regex={create_button_text_regex!r}")

    prompt_info = await _sora_fill_prompt_pw(page, prompt=prompt, log_file=log_file)
    if not bool(prompt_info.get("ok")):
        # 没写进去就别点了，直接给更明确的错误（同时不计惩罚）
        raise NonPenalizedTaskError(f"prompt 未成功写入输入框：{prompt_info}")
    await asyncio.sleep(3)
    btn0 = await _pw_find_create_button_pw(
        page,
        primary_regex=create_button_text_regex,
        log_file=log_file,
        timeout_seconds=15.0,
        prefer_frame_idx=prompt_info.get("frame_idx"),
    )
    if btn0 is None:
        # 先把页面可疑按钮都写入日志，方便你调整 regex/定位
        await _pw_debug_dump_clickables_pw(page, log_file=log_file, max_items=80)
        # 不立即失败：后面还会尝试键盘提交兜底
    else:
        try:
            txt = await btn0.inner_text()
        except Exception:
            txt = ""
        _append_log(log_file, f"[sora] create_button picked text={_safe_trim(txt, 120)!r}")
        await _pw_wait_button_actionable(btn0, timeout_seconds=15.0, log_file=log_file)

    # URL 匹配做更宽容的兜底：用户传入 regex 为主；若包含 backend/nf/create 字样则追加路径级匹配
    url_pats = []
    try:
        url_pats.append(re.compile(monitor_url_regex, flags=re.IGNORECASE))
    except Exception:
        url_pats.append(re.compile(re.escape(str(monitor_url_regex or "")), flags=re.IGNORECASE))
    try:
        raw = str(monitor_url_regex or "").replace("\\", "")
        if "backend/nf/create" in raw:
            url_pats.append(re.compile(r"/backend/nf/create(\b|/|\?|$)", flags=re.IGNORECASE))
    except Exception:
        pass

    def _pred(resp) -> bool:
        try:
            u = str(resp.url or "")
            ok_url = False
            for p in url_pats:
                if p.search(u):
                    ok_url = True
                    break
            if not ok_url:
                return False
            return str(resp.request.method or "").upper().strip() == "POST"
        except Exception:
            return False

    def _pred_req(req) -> bool:
        try:
            u = str(getattr(req, "url", "") or "")
            ok_url = False
            for p in url_pats:
                if p.search(u):
                    ok_url = True
                    break
            if not ok_url:
                return False
            return str(getattr(req, "method", "") or "").upper().strip() == "POST"
        except Exception:
            return False

    resp = None
    # 触发策略：按钮点击 -> Ctrl+Enter -> Enter（逐个尝试直到命中 POST）
    deadline = time.time() + max(1.0, float(monitor_seconds))
    attempts: list[tuple[str, Callable[[], Awaitable[None]]]] = []
    if btn0 is not None:
        attempts.append(("click_button", lambda: _pw_click_button_robust(page, btn0, log_file=log_file)))

    async def _press_ctrl_enter():
        await _pw_focus_prompt_input_pw(page, log_file=log_file)
        try:
            await page.keyboard.press("Control+Enter")
        except Exception:
            # 某些环境按键名差异，兜底 Enter
            await page.keyboard.press("Enter")
        _append_log(log_file, "[sora] submit: pressed Control+Enter")

    async def _press_enter():
        await _pw_focus_prompt_input_pw(page, log_file=log_file)
        await page.keyboard.press("Enter")
        _append_log(log_file, "[sora] submit: pressed Enter")

    attempts.append(("ctrl_enter", _press_ctrl_enter))
    attempts.append(("enter", _press_enter))

    last_err: Optional[Exception] = None
    for name, trigger in attempts:
        remain = deadline - time.time()
        if remain <= 1.0:
            break
        try:
            _append_log(log_file, f"[sora] try_trigger name={name} remain={remain:.2f}s")
            # 先抓 request 再拿 response（比 expect_response 更不容易漏）
            req_task = asyncio.create_task(page.wait_for_request(_pred_req, timeout=int(remain * 1000.0)))
            try:
                await trigger()
            except Exception as te:
                last_err = te
                _append_log(log_file, f"[sora] trigger {name} failed: {te}")
                if not req_task.done():
                    req_task.cancel()
                continue
            try:
                req = await req_task
                try:
                    req_url = str(getattr(req, "url", "") or "")
                except Exception:
                    req_url = ""
                _append_log(log_file, f"[sora] got_request after trigger={name} url={_safe_trim(req_url, 240)!r}")
                try:
                    resp = await req.response()
                except Exception:
                    resp = None
                if resp is not None and _pred(resp):
                    _append_log(log_file, f"[sora] got_response(via request.response) trigger={name}")
                else:
                    _append_log(log_file, f"[sora] response missing/unmatched trigger={name} -> will fallback")
                    resp = None
                break
            except Exception as we:
                last_err = we
                _append_log(log_file, f"[sora] wait_for_request after trigger={name} failed: {we}")
                # 记录触发后短时间内有没有任何 POST（帮助判断“到底点到没”）
                try:
                    await _pw_log_recent_posts(page, seconds=2.0, log_file=log_file)
                except Exception:
                    pass
                continue
        except Exception as e:
            last_err = e
            continue

    if resp is None:
        # 兜底：再用 sniff 等待一次（避免“response 已发生但 expect_response 没匹配/错过”的情况）
        if last_err is not None:
            _append_log(log_file, f"[sora] wait/trigger failed (final): {last_err}")
        try:
            tx = await _sniff_http_transaction_pw(
                page,
                url_regex=str(monitor_url_regex or ""),
                method="POST",
                timeout_seconds=max(1.0, float(max(1.0, deadline - time.time()))),
                log_path=str(log_file),
            )
        except Exception:
            tx = {}
        if (tx or {}).get("seen"):
            body_text = (tx or {}).get("response_body") or ""
            task_id = None
            try:
                payload_obj = json.loads(body_text) if body_text else {}
                task_id = (payload_obj or {}).get("id") or (payload_obj or {}).get("task_id")
            except Exception:
                task_id = None
            if task_id:
                _append_log(log_file, f"[sora] fallback sniff ok task_id={task_id}")
                return str(task_id), dict(tx)

        # 这类错误多为登录态/权限/站点变化/按钮没真正触发；不计入窗口连续错误
        try:
            cur_url = str(getattr(page, "url", "") or "")
        except Exception:
            cur_url = ""
        raise NonPenalizedTaskError(
            "未监控到匹配的 POST 请求/响应（可能未登录/无权限/接口地址变化/按钮未真正触发）"
            + f" monitor_url_regex={monitor_url_regex!r} page_url={_safe_trim(cur_url, 200)!r} prompt_fill={prompt_info}"
        )

    if resp is None:
        raise NonPenalizedTaskError("未监控到匹配的 POST 响应（create 已触发但未抓到 response）")

    create_tx = await _response_to_tx_pw(resp, log_path=monitor_log_path)

    status = create_tx.get("status")
    body_text = create_tx.get("response_body") or ""
    task_id = None
    try:
        payload_obj = json.loads(body_text) if body_text else {}
        task_id = (payload_obj or {}).get("id") or (payload_obj or {}).get("task_id")
    except Exception:
        task_id = None

    try:
        status_i = int(status) if status is not None else None
    except Exception:
        status_i = None

    if status_i == 400:
        # 400 类错误（invalid_request 等）通常与 prompt/请求内容相关，不计入窗口连续错误
        raise NonPenalizedTaskError(
            f"create 未成功或未解析到任务ID：status={status_i} body={_safe_trim(body_text, 400)}",
            status_code=status_i,
        )

    if status_i != 200 or not task_id:
        raise RuntimeError(f"create 未成功或未解析到任务ID：status={status_i} body={_safe_trim(body_text, 400)}")

    return str(task_id), create_tx


@dataclass
class _SoraWatcher:
    task_id: str
    deadline: float
    progress_cb: ProgressCB
    future: "asyncio.Future[Dict[str, Any]]"
    last_sent_progress: int = -1
    last_status: Any = None
    last_progress_pct: Optional[float] = None


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

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
    ) -> None:
        """确保窗口已打开且 Playwright 已通过 CDP 连接到指纹浏览器。"""
        self.last_used_at = time.time()
        if self.browser is not None and self.page is not None:
            return

        try:
            from playwright.async_api import async_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；错误：{e}")

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
            raise RuntimeError(f"browser_open 失败：{rsp}")
        data = (rsp or {}).get("data") or {}
        raw_endpoint = str(data.get("http") or data.get("ws") or "").strip()
        debugger_address = _normalize_cdp_endpoint(raw_endpoint)
        if not debugger_address:
            raise RuntimeError(f"browser_open 返回缺少 http/ws(CDP endpoint)：{rsp}")

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
        self._cancel_idle_close()

        async def _job():
            try:
                secs = max(0.0, float(self.idle_close_seconds))
                if secs <= 0:
                    return
                await asyncio.sleep(secs)
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
                    return await _sora_create_task_pw(
                        page=self.page,
                        prompt=prompt,
                        target_url=target_url,
                        create_button_text_regex=create_button_text_regex,
                        monitor_seconds=monitor_seconds,
                        monitor_url_regex=monitor_url_regex,
                        monitor_log_path=monitor_log_path,
                    )
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
                async with self.driver_lock:
                    try:
                        tx = await _sniff_http_transaction_pw(
                            self.page,
                            url_regex=str(self.pending_url_regex or ""),
                            method="GET",
                            timeout_seconds=float(self.sniff_timeout_seconds),
                            log_path=self.monitor_log_path,
                        )
                    except Exception:
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

                for tid, w in list(self.watchers.items()):
                    task_obj = index.get(str(tid)) if index else _extract_task_obj(payload_obj, str(tid))
                    if not task_obj:
                        continue

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

                if not self.watchers:
                    return

                await asyncio.sleep(float(self.poll_interval_seconds))
        finally:
            if not self.watchers:
                self._schedule_idle_close()

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


async def sora_gen_video(
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    """Sora 生视频：复用同一指纹浏览器窗口 + Playwright(CDP) 轻量连接，拆分“创建任务”和“进度轮询”。

    参数来源：
    - 运行时浏览器参数来自 TaskService（picked window / browser / space）
    - 业务参数从 payload 读取（prompt / url / regex / 超时等）
    """
    payload = payload or {}
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("payload.prompt 不能为空")

    # Playwright 行为配置（可从 payload 覆盖；默认值与 roxy_sora_automation.py 保持一致）
    target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
    create_button_text_regex = str(payload.get("sora_create_video_regex") or r"^\s*Create\s+video\s*$").strip()
    monitor_seconds = float(payload.get("sora_monitor_seconds") or 8.0)
    monitor_url_regex = str(payload.get("sora_monitor_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/create").strip()
    monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)
    pending_url_regex = str(payload.get("sora_pending_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/pending/v2").strip()
    max_wait_seconds = float(payload.get("sora_pending_max_wait_seconds") or max(30.0, min(float(timeout_seconds), 60.0 * 10)))

    ctx = _get_or_create_ctx(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    # 轮询与 ctx 回收策略
    poll_interval_seconds = float(payload.get("sora_pending_poll_interval_seconds") or 1.0)
    sniff_timeout_seconds = float(payload.get("sora_pending_sniff_timeout_seconds") or 4.0)
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)

    try:
        from playwright.async_api import async_playwright  # type: ignore  # noqa: F401
    except Exception as e:
        raise RuntimeError(f"Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；错误：{e}")

    await progress_cb(0, {"stage": "create_task"})
    task_id, create_tx = await ctx.create_task(
        prompt=prompt,
        target_url=target_url,
        create_button_text_regex=create_button_text_regex,
        monitor_seconds=monitor_seconds,
        monitor_url_regex=monitor_url_regex,
        monitor_log_path=monitor_log_path,
        browser_open_args=[],
        browser_force_open=False,
        browser_headless=False,
    )

    await progress_cb(1, {"stage": "created", "task_id": task_id})
    await progress_cb(1, {"stage": "monitor_progress", "task_id": task_id})
    progress_result = await ctx.watch_task_progress(
        task_id=task_id,
        progress_cb=progress_cb,
        pending_url_regex=pending_url_regex,
        monitor_log_path=monitor_log_path,
        max_wait_seconds=max_wait_seconds,
        poll_interval_seconds=poll_interval_seconds,
        sniff_timeout_seconds=sniff_timeout_seconds,
        idle_close_seconds=idle_close_seconds,
    )

    await progress_cb(100, {"stage": "done", "task_id": task_id})

    result: Dict[str, Any] = {
        "type": "video",
        "message": "Sora 任务已创建并监控完成",
        "task_id": task_id,
        "prompt": prompt,
        "create_tx": {
            "url": create_tx.get("url"),
            "status": create_tx.get("status"),
            "log_file": create_tx.get("log_file"),
        },
        "progress": progress_result,
        "outputs": [],
    }

    return result

