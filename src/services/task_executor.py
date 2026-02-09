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
        # 这类错误多为登录态/权限/站点变化导致，通常不应计入窗口连续错误
        raise NonPenalizedTaskError("未监控到匹配的 POST 请求（可能未登录/无权限/接口地址变化/按钮未真正触发）")

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

    executor: ThreadPoolExecutor = field(default_factory=lambda: ThreadPoolExecutor(max_workers=1))
    driver: Any = None  # 仅能在 executor 线程内调用
    debugger_address: Optional[str] = None
    driver_path: Optional[str] = None
    last_used_at: float = field(default_factory=lambda: time.time())
    # 创建任务必须串行（create_lock），driver 操作互斥（driver_lock）
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
        print("ctx close_and_drop")
        await self.close()
        print(f"browser_close-----3-----")
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
                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(
                        self.executor,
                        lambda: _sora_create_task_sync(
                            driver=self.driver,
                            prompt=prompt,
                            target_url=target_url,
                            create_button_text_regex=create_button_text_regex,
                            monitor_seconds=monitor_seconds,
                            monitor_url_regex=monitor_url_regex,
                            monitor_log_path=monitor_log_path,
                        ),
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

                tx = None
                async with self.driver_lock:
                    loop = asyncio.get_running_loop()
                    tx = await loop.run_in_executor(
                        self.executor,
                        lambda: sniff_get(
                            self.driver,
                            url_regex=str(self.pending_url_regex or ""),
                            timeout_seconds=float(self.sniff_timeout_seconds),
                            log_path=self.monitor_log_path,
                        ),
                    )

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
        loop = asyncio.get_running_loop()

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
        print(f"browser_close-----4-----")
        drv = self.driver
        self.driver = None
        if drv is not None:
            try:
                await loop.run_in_executor(self.executor, lambda: drv.quit())
                print(f"browser_close-----4-----")
            except Exception:
                pass


_CTX_LOCK = threading.Lock()
_SORA_CTXS: Dict[str, _SoraBrowserContext] = {}


def _ctx_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return "|".join([(vendor or "").strip().lower(), (base_url or "").strip().lower(), (space_id or "").strip(), (window_key or "").strip()])


def _drop_ctx(cache_key: str) -> None:
    k = (cache_key or "").strip()
    print(f"_drop_ctx----------{k}")
    if not k:
        return
    with _CTX_LOCK:
        print(f"_drop_ctx-----1-----{k}")
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
    print(f"_get_or_create_ctx----------{k}")
    with _CTX_LOCK:
        ctx = _SORA_CTXS.get(k)
        if ctx is None:
            print(f"_get_or_create_ctx-----1----{k}")
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
        from selenium import webdriver  # type: ignore  # noqa: F401
    except Exception as e:
        raise RuntimeError(f"Selenium 未安装或导入失败，请先安装依赖：pip install selenium；错误：{e}")

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

