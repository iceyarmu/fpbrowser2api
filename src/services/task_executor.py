"""任务执行器（当前先实现“可运行的模拟执行”）。

你后续要接入真实自动化（例如 Selenium/Playwright + 指纹浏览器窗口启动）时，
只需要在这里把 `simulate_*` 替换成真实执行逻辑，并持续调用 `progress_cb` 更新进度即可。
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from .fp_browser_client import FPBrowserClient
from .sora_net_tools import sniff_get, sniff_post


ProgressCB = Callable[[int, Optional[Dict[str, Any]]], Awaitable[None]]


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


def _element_text_content(driver, el) -> str:
    """尽量拿到元素完整文本（包含不可见子节点的 textContent）。"""
    try:
        t = driver.execute_script("return arguments[0].textContent || '';", el)
        return (t or "").strip()
    except Exception:
        return (getattr(el, "text", "") or "").strip()


def _find_button_by_regex(driver, pattern: re.Pattern) -> Optional[object]:
    """在页面所有 button 中查找文本命中 pattern 的按钮。匹配源：textContent + aria-label + title + span。"""
    try:
        from selenium.webdriver.common.by import By  # type: ignore
    except Exception:
        return None

    buttons = driver.find_elements(By.TAG_NAME, "button")
    for b in buttons:
        try:
            text_content = _element_text_content(driver, b)
            aria = (b.get_attribute("aria-label") or "").strip()
            title = (b.get_attribute("title") or "").strip()
            hay = " ".join([text_content, aria, title]).strip()
            if hay and pattern.search(hay):
                return b

            spans = b.find_elements(By.TAG_NAME, "span")
            for sp in spans:
                sp_txt = _element_text_content(driver, sp)
                if sp_txt and pattern.search(sp_txt):
                    return b
        except Exception:
            continue
    return None


def _debug_dump_span_and_button_texts(driver, *, max_items: int = 40) -> None:
    """找不到按钮时，输出页面上部分 span/button 文本，帮助判断文案/语言/登录态。"""
    try:
        from selenium.webdriver.common.by import By  # type: ignore
    except Exception:
        return

    print("=== 调试：页面 span 文本采样 ===")
    spans = driver.find_elements(By.TAG_NAME, "span")
    shown = 0
    for sp in spans:
        if shown >= max_items:
            break
        try:
            txt = _element_text_content(driver, sp)
            if not txt:
                continue
            print("-", _safe_trim(txt, 120))
            shown += 1
        except Exception:
            continue

    print("=== 调试：页面 button 文本采样 ===")
    buttons = driver.find_elements(By.TAG_NAME, "button")
    shown = 0
    for b in buttons:
        if shown >= max_items:
            break
        try:
            txt = _element_text_content(driver, b)
            aria = (b.get_attribute("aria-label") or "").strip()
            if not txt and not aria:
                continue
            line = " / ".join([x for x in [_safe_trim(txt, 120), _safe_trim(aria, 120)] if x])
            print("-", line)
            shown += 1
        except Exception:
            continue


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


def _init_selenium_driver(*, debugger_address: str, driver_path: str):
    """在单线程 executor 内创建 driver（必须在同一线程里后续使用）。"""
    from selenium import webdriver  # type: ignore
    from selenium.webdriver.chrome.service import Service  # type: ignore

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("debuggerAddress", debugger_address)
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def _sora_create_task_sync(
    *,
    driver,
    prompt: str,
    target_url: str,
    create_button_text_regex: str,
    monitor_seconds: float,
    monitor_url_regex: str,
    monitor_log_path: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    """在页面上创建任务并抓取 POST /backend/nf/create 响应，返回 task_id。"""
    from selenium.webdriver.common.by import By  # type: ignore
    from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore

    wait = WebDriverWait(driver, 30)
    driver.get(target_url)
    try:
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    except Exception:
        pass

    textarea = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "textarea")))
    try:
        if not textarea.is_displayed():
            for el in driver.find_elements(By.CSS_SELECTOR, "textarea"):
                if el.is_displayed():
                    textarea = el
                    break
    except Exception as e:
        pass

    textarea.click()
    try:
        textarea.clear()
    except Exception:
        pass
    textarea.send_keys(prompt)
    time.sleep(2.0)

    pattern = re.compile(create_button_text_regex, flags=re.IGNORECASE)
    btn = None
    try:
        btn = wait.until(lambda d: _find_button_by_regex(d, pattern))
    except Exception:
        btn = _find_button_by_regex(driver, pattern)

    if not btn:
        _debug_dump_span_and_button_texts(driver, max_items=40)
        raise RuntimeError(f"未找到创建按钮：regex={create_button_text_regex!r}")

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    except Exception:
        pass
    try:
        btn.click()
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception as e:
            raise RuntimeError(f"按钮点击失败：{e}")

    create_tx = sniff_post(
        driver,
        url_regex=monitor_url_regex,
        timeout_seconds=monitor_seconds,
        log_path=monitor_log_path,
    )
    if not create_tx.get("seen"):
        raise RuntimeError("未监控到匹配的 POST 请求（可能未登录/无权限/接口地址变化/按钮未真正触发）")

    status = create_tx.get("status")
    body_text = create_tx.get("response_body") or ""
    task_id = None
    try:
        payload_obj = json.loads(body_text) if body_text else {}
        task_id = (payload_obj or {}).get("id") or (payload_obj or {}).get("task_id")
    except Exception:
        task_id = None

    if status != 200 or not task_id:
        raise RuntimeError(f"create 未成功或未解析到任务ID：status={status} body={_safe_trim(body_text, 400)}")

    return str(task_id), create_tx


def _sora_monitor_progress_sync(
    *,
    driver,
    task_id: str,
    pending_url_regex: str,
    max_wait_seconds: float,
    monitor_log_path: Optional[str],
    progress_notify: Callable[[int, Optional[Dict[str, Any]]], None],
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    """被动抓取 GET /backend/nf/pending/v2 响应，直到 progress_pct>=1.0 或超时。"""
    deadline = time.time() + max(1.0, float(max_wait_seconds))
    last_print_at = 0.0
    last_status = None
    last_progress = None
    last_sent_progress_int = -1

    while time.time() < deadline:
        tx = sniff_get(
            driver,
            url_regex=pending_url_regex,
            timeout_seconds=10,
            log_path=monitor_log_path,
        )
        body = (tx or {}).get("response_body") or ""
        payload_obj = None
        try:
            payload_obj = json.loads(body) if body else None
        except Exception:
            payload_obj = None

        task_obj = _extract_task_obj(payload_obj, task_id)
        if not task_obj:
            now = time.time()
            if now - last_print_at >= 5.0:
                print(f"[进度] 已捕获 pending/v2 响应，但未包含 task_id={task_id}（可能是列表未包含该任务）")
                last_print_at = now
            time.sleep(max(0.2, float(poll_interval_seconds)))
            continue

        status = task_obj.get("status")
        progress_pct = _normalize_progress(task_obj.get("progress_pct"))
        now = time.time()
        changed = (status != last_status) or (progress_pct != last_progress)

        if progress_pct is not None:
            p_int = int(max(0.0, min(1.0, float(progress_pct))) * 100.0)
            if p_int != last_sent_progress_int:
                progress_notify(p_int, {"status": status, "task_id": task_id})
                last_sent_progress_int = p_int

        if changed or (now - last_print_at >= 1.0):
            print(f"[进度] id={task_id} status={status} progress_pct={progress_pct}")
            last_print_at = now
            last_status = status
            last_progress = progress_pct

        if progress_pct is not None and float(progress_pct) >= 1.0:
            time.sleep(3.0)
            return {"task_id": task_id, "status": status, "progress_pct": progress_pct, "done": True, "last_tx": tx}

        time.sleep(max(0.2, float(poll_interval_seconds)))

    return {"task_id": task_id, "status": last_status, "progress_pct": last_progress, "done": False}


@dataclass
class _SoraBrowserContext:
    vendor: str
    base_url: str
    access_key: Optional[str]
    space_id: str
    window_key: str
    fp_client: FPBrowserClient

    executor: ThreadPoolExecutor = field(default_factory=lambda: ThreadPoolExecutor(max_workers=1))
    driver: Any = None  # 仅能在 executor 线程内调用
    debugger_address: Optional[str] = None
    driver_path: Optional[str] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_used_at: float = field(default_factory=lambda: time.time())

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
    ) -> None:
        """确保窗口已打开且 driver 已连接。driver 的所有调用必须在 executor 线程里发生。"""
        self.last_used_at = time.time()
        if self.driver is not None:
            return

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
        debugger_address = (data.get("http") or "").strip()
        driver_path = (data.get("driver") or "").strip()
        if not debugger_address or not driver_path:
            raise RuntimeError(f"browser_open 返回缺少 http(driver debuggerAddress) 或 driver(chromedriver path)：{rsp}")

        self.debugger_address = debugger_address
        self.driver_path = driver_path

        loop = asyncio.get_running_loop()
        self.driver = await loop.run_in_executor(
            self.executor,
            lambda: _init_selenium_driver(debugger_address=debugger_address, driver_path=driver_path),
        )

    async def close(self) -> None:
        """关闭窗口与 driver（谨慎：会影响同窗口后续复用）。"""
        loop = asyncio.get_running_loop()
        drv = self.driver
        self.driver = None
        if drv is not None:
            try:
                await loop.run_in_executor(self.executor, lambda: drv.quit())
            except Exception:
                pass
        try:
            await self.fp_client.browser_close(
                vendor=self.vendor,
                base_url=self.base_url,
                access_key=self.access_key,
                window_key=self.window_key,
            )
        except Exception:
            pass


_CTX_LOCK = threading.Lock()
_SORA_CTXS: Dict[str, _SoraBrowserContext] = {}


def _ctx_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return "|".join([(vendor or "").strip().lower(), (base_url or "").strip().lower(), (space_id or "").strip(), (window_key or "").strip()])


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
                vendor=(vendor or "roxy").strip().lower(),
                base_url=(base_url or "").strip().rstrip("/"),
                access_key=access_key,
                space_id=(space_id or "").strip(),
                window_key=(window_key or "").strip(),
                fp_client=FPBrowserClient(),
            )
            _SORA_CTXS[k] = ctx
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
    """Sora 生视频：复用同一指纹浏览器窗口 + Selenium driver，拆分“创建任务”和“进度轮询”。

    参数来源：
    - 运行时浏览器参数来自 TaskService（picked window / browser / space）
    - 业务参数从 payload 读取（prompt / url / regex / 超时等）
    """
    payload = payload or {}
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("payload.prompt 不能为空")

    # Selenium 行为配置（可从 payload 覆盖；默认值与 roxy_sora_automation.py 保持一致）
    target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/explore").strip()
    create_button_text_regex = str(payload.get("sora_create_video_regex") or r"^\s*Create\s+video\s*$").strip()
    monitor_seconds = float(payload.get("sora_monitor_seconds") or 8.0)
    monitor_url_regex = str(payload.get("sora_monitor_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/create").strip()
    monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)
    pending_url_regex = str(payload.get("sora_pending_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/pending/v2").strip()
    max_wait_seconds = float(payload.get("sora_pending_max_wait_seconds") or max(30.0, min(float(timeout_seconds), 60.0 * 10)))

    # 指纹浏览器窗口打开参数（可选）
    browser_open_args = payload.get("browser_open_args")
    if not isinstance(browser_open_args, list):
        browser_open_args = []
    browser_open_args = [str(x) for x in browser_open_args if x is not None]
    browser_force_open = bool(payload.get("browser_force_open") or False)
    browser_headless = bool(payload.get("browser_headless") or False)

    # 是否在本任务结束后关闭窗口/driver（默认不关，满足“一个 driver 执行多个任务”）
    close_on_finish = bool(payload.get("close_browser_on_finish") or False)

    ctx = _get_or_create_ctx(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )

    async with ctx.lock:
        try:
            from selenium import webdriver  # type: ignore  # noqa: F401
        except Exception as e:
            raise RuntimeError(f"Selenium 未安装或导入失败，请先安装依赖：pip install selenium；错误：{e}")

        await progress_cb(0, {"stage": "browser_open"})
        await ctx.ensure_open(args=browser_open_args, force_open=browser_force_open, headless=browser_headless)

        loop = asyncio.get_running_loop()

        def _notify(p: int, extra: Optional[Dict[str, Any]] = None) -> None:
            try:
                fut = asyncio.run_coroutine_threadsafe(progress_cb(int(p), extra), loop)
                fut.result(timeout=5)
            except Exception:
                pass

        await progress_cb(0, {"stage": "create_task"})
        task_id, create_tx = await loop.run_in_executor(
            ctx.executor,
            lambda: _sora_create_task_sync(
                driver=ctx.driver,
                prompt=prompt,
                target_url=target_url,
                create_button_text_regex=create_button_text_regex,
                monitor_seconds=monitor_seconds,
                monitor_url_regex=monitor_url_regex,
                monitor_log_path=monitor_log_path,
            ),
        )
        _notify(0, {"stage": "created", "task_id": task_id})

        await progress_cb(0, {"stage": "monitor_progress", "task_id": task_id})
        progress_result = await loop.run_in_executor(
            ctx.executor,
            lambda: _sora_monitor_progress_sync(
                driver=ctx.driver,
                task_id=task_id,
                pending_url_regex=pending_url_regex,
                max_wait_seconds=max_wait_seconds,
                monitor_log_path=monitor_log_path,
                progress_notify=_notify,
                poll_interval_seconds=3.0,
            ),
        )

        if not bool(progress_result.get("done")):
            raise RuntimeError(f"进度监控超时（max_wait_seconds={max_wait_seconds}）：task_id={task_id}")

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

        if close_on_finish:
            await ctx.close()

        return result

