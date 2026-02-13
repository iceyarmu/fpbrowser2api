"""Sora 执行器：专注于对 `https://sora.chatgpt.com` 的接口访问与任务编排。

拆分后的职责边界：
- `playwright_broswer_context.py`：通用“指纹浏览器自动化层”（开窗/连CDP/挑页/page内fetch）
- `sora_task_executor.py`：Sora 站点侧逻辑（鉴权抓取、nf/create、pending轮询、drafts/publish、nf_check、invite）

入口：
- `sora_gen_video`（由 `task_service.py` 发起调用）
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from .playwright_broswer_context import (
    PlaywrightBrowserContext,
    append_log,
    get_or_create_ctx as get_or_create_playwright_ctx,
    page_fetch_json,
    page_fetch_tx,
    safe_trim,
)
from .task_executor_types import ProgressCB


class NonPenalizedTaskError(RuntimeError):
    """失败但不计入窗口连续错误（consecutive_errors）的异常。

用途：Sora 创建阶段常见的 400/invalid_request 等错误，以及“未监控到 POST 请求”等，
这类错误不应导致窗口被连续错误熔断。
"""

    no_penalty: bool = True

    def __init__(self, message: str, *, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code

def _mask_secret(s: Optional[str], *, head: int = 8, tail: int = 6) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= head + tail + 3:
        return s[: max(1, min(len(s), head))] + "...(masked)"
    return s[:head] + "...(masked)..." + s[-tail:]


def _pick_orientation_from_ratio(ratio: Optional[str]) -> Optional[str]:
    """将尺寸比例（如 19:6 / 6:19）转换为 sora 的 orientation（landscape/portrait）。"""
    if not ratio:
        return None
    s = str(ratio).strip().lower().replace("：", ":")
    if "19:6" in s:
        return "landscape"
    if "6:19" in s:
        return "portrait"
    return None


def _pick_n_frames(v: Any) -> int:
    """将“时长参数”归一化为 n_frames（当前按你的需求仅支持 300/450）。"""
    try:
        iv = int(float(v))
    except Exception:
        iv = 300
    if iv in (300, 450):
        return iv
    # 常见：秒数 10/15
    if iv == 10:
        return 300
    if iv == 15:
        return 450
    return 450


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


def _sora_backend_url_from_target(target_url: str, path: str) -> str:
    """根据 target_url 组合 sora backend URL（默认 host 同源）。"""
    try:
        p = urlparse(str(target_url or "").strip())
        scheme = p.scheme or "https"
        netloc = p.netloc or "sora.chatgpt.com"
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return "https://sora.chatgpt.com" + str(path)


async def _pw_get_user_agent(page) -> Optional[str]:
    try:
        ua = await page.evaluate("() => navigator.userAgent")
        ua = str(ua or "").strip()
        return ua or None
    except Exception:
        return None


async def _sora_extract_bearer_from_any_post_pw(page, *, timeout_seconds: float, log_file: Path) -> Dict[str, Any]:
    """监听任意 POST/GET 请求，从 headers 提取 Authorization: Bearer <token>。"""
    result: Dict[str, Any] = {"seen": False, "authorization": None, "token": None, "user_agent": None, "url": None}
    loop = asyncio.get_running_loop()
    fut: "asyncio.Future[Dict[str, Any]]" = loop.create_future()

    def _on_request(req) -> None:
        if fut.done():
            return
        try:
            m = str(getattr(req, "method", "") or "").upper().strip()
            if m not in ("POST", "GET"):
                return
            headers = getattr(req, "headers", None) or {}
            auth = headers.get("authorization") or headers.get("Authorization")
            if not auth:
                return
            auth = str(auth).strip()
            if not auth.lower().startswith("bearer "):
                return
            tok = auth.split(" ", 1)[1].strip()
            if not tok:
                return
            ua = headers.get("user-agent") or headers.get("User-Agent")
            try:
                u = str(getattr(req, "url", "") or "")
            except Exception:
                u = ""
            fut.set_result(
                {
                    "seen": True,
                    "authorization": auth,
                    "token": tok,
                    "user_agent": str(ua or "") or None,
                    "url": u or None,
                }
            )
        except Exception:
            return

    try:
        page.on("request", _on_request)
    except Exception as e:
        append_log(log_file, f"[sora][token] attach request listener failed: {e}")
        return result

    try:
        data = await asyncio.wait_for(fut, timeout=max(1.0, float(timeout_seconds)))
        result.update(data or {})
        append_log(
            log_file,
            f"[sora][token] bearer_captured url={safe_trim(str(result.get('url') or ''), 240)!r} "
            f"ua={safe_trim(str(result.get('user_agent') or ''), 120)!r} "
            f"auth={_mask_secret(str(result.get('authorization') or ''), head=15, tail=15)!r}",
        )
        return result
    except Exception as e:
        append_log(log_file, f"[sora][token] bearer capture timeout/failed: {e}")
        return result
    finally:
        try:
            page.off("request", _on_request)
        except Exception:
            pass


async def _sora_generate_sentinel_token_in_fp_context_pw(page, *, device_id: Optional[str], log_file: Path) -> Optional[str]:
    """在“指纹浏览器的同一 context”中，用 __sentinel__ hack 生成 SentinelToken。"""
    try:
        ctx = page.context
    except Exception:
        ctx = None
    if ctx is None:
        append_log(log_file, "[sora][sentinel] page.context unavailable")
        return None

    did = (device_id or "").strip() if device_id else ""
    if not did:
        try:
            cookies = await ctx.cookies("https://sora.chatgpt.com")
        except Exception:
            cookies = []
        for c in cookies or []:
            try:
                if str(c.get("name") or "") == "oai-did" and c.get("value"):
                    did = str(c["value"])
                    break
            except Exception:
                continue
    if not did:
        did = str(uuid4())

    inject_html = "<!DOCTYPE html><html><head><script src=\"https://chatgpt.com/backend-api/sentinel/sdk.js\"></script></head><body></body></html>"

    try:
        p2 = await ctx.new_page()
    except Exception as e:
        append_log(log_file, f"[sora][sentinel] new_page failed: {e}")
        return None

    async def handle_route(route):
        try:
            url = str(route.request.url or "")
        except Exception:
            url = ""
        try:
            if "__sentinel__" in url:
                await route.fulfill(status=200, content_type="text/html", body=inject_html)
                return
            if ("/sentinel/" in url) or ("chatgpt.com" in url) or ("sora.chatgpt.com" in url):
                await route.continue_()
                return
            await route.abort()
        except Exception:
            try:
                await route.continue_()
            except Exception:
                pass

    try:
        try:
            await p2.route("**/*", handle_route)
        except Exception:
            pass
        append_log(log_file, f"[sora][sentinel] loading sdk under did={did!r}")
        await p2.goto("https://sora.chatgpt.com/__sentinel__", wait_until="load", timeout=30_000)
        await p2.wait_for_function("typeof SentinelSDK !== 'undefined' && typeof SentinelSDK.token === 'function'", timeout=15_000)
        token = await p2.evaluate(
            """async (did) => {
              try {
                return await SentinelSDK.token('sora_2_create_task', did);
              } catch (e) {
                return 'ERROR: ' + (e && e.message ? e.message : String(e));
              }
            }""",
            did,
        )
        token_s = str(token or "")
        if token_s and not token_s.startswith("ERROR"):
            append_log(log_file, f"[sora][sentinel] token_ok value={_mask_secret(token_s, head=15, tail=15)!r}")
            return token_s
        append_log(log_file, f"[sora][sentinel] token_error value={safe_trim(token_s, 300)!r}")
        return None
    except Exception as e:
        append_log(log_file, f"[sora][sentinel] generate failed: {e}")
        return None
    finally:
        try:
            await p2.close()
        except Exception:
            pass


def _guess_image_filename_and_mime(headers: Dict[str, str], *, default_name: str = "image.png") -> Tuple[str, str]:
    ct = ""
    try:
        ct = str((headers or {}).get("content-type") or (headers or {}).get("Content-Type") or "").lower()
    except Exception:
        ct = ""
    if "image/jpeg" in ct or "image/jpg" in ct:
        return "image.jpg", "image/jpeg"
    if "image/webp" in ct:
        return "image.webp", "image/webp"
    if "image/png" in ct:
        return "image.png", "image/png"
    return default_name, "image/png"


def _download_bytes_local(url: str, *, timeout_seconds: float = 30.0, user_agent: Optional[str] = None) -> Tuple[bytes, Dict[str, str]]:
    """本地下载资源（按当前实现：不走指纹浏览器下载首帧图）。"""
    u = str(url or "").strip()
    if not u:
        raise ValueError("url 不能为空")
    headers = {"Accept": "*/*"}
    if user_agent:
        headers["User-Agent"] = str(user_agent)
    req = Request(u, headers=headers, method="GET")
    with urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
        data = resp.read()
        try:
            hdrs = dict(getattr(resp, "headers", {}) or {})
        except Exception:
            hdrs = {}
        return bytes(data or b""), hdrs


async def _sora_api_upload_image_bytes_pw(
    page,
    *,
    target_url: str,
    bearer_token: str,
    image_bytes: bytes,
    filename: str,
    mime_type: str,
    log_file: Path,
) -> str:
    """上传首帧图片获取 media_id。"""
    candidates = [
        _sora_backend_url_from_target(target_url, "/backend/uploads"),
        _sora_backend_url_from_target(target_url, "/uploads"),
    ]
    try:
        b64 = base64.b64encode(image_bytes or b"").decode("ascii")
    except Exception:
        b64 = ""
    if not b64:
        raise RuntimeError("首帧图片为空或 base64 编码失败")

    last_err: Optional[str] = None
    for upload_url in candidates:
        try:
            res = await page.evaluate(
                """async (args) => {
                  const { uploadUrl, bearer, filename, mime, b64 } = args;
                  const bin = atob(b64);
                  const bytes = new Uint8Array(bin.length);
                  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
                  const blob = new Blob([bytes], { type: mime || 'image/png' });
                  const file = new File([blob], filename || 'image.png', { type: mime || 'image/png' });
                  const fd = new FormData();
                  fd.append('file', file, filename || 'image.png');
                  fd.append('file_name', filename || 'image.png');
                  const resp = await fetch(uploadUrl, {
                    method: 'POST',
                    headers: { 'Authorization': 'Bearer ' + bearer },
                    body: fd,
                    credentials: 'include',
                  });
                  const text = await resp.text();
                  return { status: resp.status, text };
                }""",
                {"uploadUrl": upload_url, "bearer": bearer_token, "filename": filename, "mime": mime_type, "b64": b64},
            )
            status = int((res or {}).get("status") or 0)
            text = str((res or {}).get("text") or "")
            append_log(log_file, f"[sora][upload] url={upload_url!r} status={status} body={safe_trim(text, 500)!r}")
            if status not in (200, 201):
                last_err = f"status={status} body={safe_trim(text, 300)}"
                continue
            try:
                obj = json.loads(text) if text else {}
            except Exception:
                obj = {}
            media_id = str((obj or {}).get("id") or "").strip()
            if media_id:
                return media_id
            last_err = f"missing id body={safe_trim(text, 300)}"
        except Exception as e:
            last_err = str(e)
            continue

    raise RuntimeError(f"上传首帧失败：{last_err}")


async def _sora_api_get_video_drafts_pw(page, *, target_url: str, bearer_token: str, limit: int, log_file: Path) -> Dict[str, Any]:
    """GET /backend/project_y/profile/drafts?limit=..."""
    url = _sora_backend_url_from_target(target_url, f"/backend/project_y/profile/drafts?limit={int(limit)}")
    headers = {"Authorization": f"Bearer {bearer_token}", "OAI-Language": "en-US"}
    tx = await page_fetch_json(page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
    obj = tx.get("_json")
    return obj if isinstance(obj, dict) else {}


async def _sora_api_post_project_y_post_pw(
    page,
    *,
    target_url: str,
    bearer_token: str,
    sentinel_token: str,
    generation_id: str,
    log_file: Path,
) -> Dict[str, Any]:
    """POST /backend/project_y/post（发布草稿，获取 post_id）。"""
    url = _sora_backend_url_from_target(target_url, "/backend/project_y/post")
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "OpenAI-Sentinel-Token": str(sentinel_token),
        "Content-Type": "application/json",
        "OAI-Language": "en-US",
    }
    payload = {"attachments_to_create": [{"generation_id": str(generation_id), "kind": "sora"}], "post_text": ""}
    tx = await page_fetch_json(page, url=url, method="POST", headers=headers, json_data=payload, log_file=log_file)
    obj = tx.get("_json")
    return obj if isinstance(obj, dict) else {}


async def _sora_create_task_pw(
    *,
    page,
    prompt: str,
    target_url: str,
    monitor_log_path: Optional[str],
    first_image_url: Optional[str],
    orientation: str,
    n_frames: int,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """通过指纹浏览器环境直接调用 /backend/nf/create（不再模拟 UI 输入/点击）。"""
    log_file = Path(monitor_log_path) if monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
    await page.goto(target_url, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass

    bearer_info = await _sora_extract_bearer_from_any_post_pw(page, timeout_seconds=20.0, log_file=log_file)
    bearer_token = str(bearer_info.get("token") or "").strip() or None
    user_agent = str(bearer_info.get("user_agent") or "").strip() or None
    if not user_agent:
        user_agent = await _pw_get_user_agent(page)
    sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(page, device_id=None, log_file=log_file)

    if not bearer_token:
        raise RuntimeError(f"未能读取到Bearer token（请确保已登录且页面会发出带 Authorization 的请求）");
    if not sentinel_token:
        raise RuntimeError(f"未能生成 SentinelToken，触发了429");

    oai_device_id = None
    try:
        sentinel_data = json.loads(str(sentinel_token))
        if isinstance(sentinel_data, dict):
            oai_device_id = str(sentinel_data.get("id") or "").strip() or None
    except Exception:
        oai_device_id = None
    if not oai_device_id:
        oai_device_id = str(uuid4())

    inpaint_items: list[Dict[str, Any]] = []
    if first_image_url:
        try:
            img_bytes, img_headers = _download_bytes_local(first_image_url, timeout_seconds=30.0, user_agent=user_agent)
        except Exception as e:
            raise NonPenalizedTaskError(
                f"首帧图片下载失败（请检查图片地址是否正确/可访问）：url={safe_trim(str(first_image_url), 400)!r} err={e}"
            ) from e

        if not img_bytes:
            raise NonPenalizedTaskError(
                f"首帧图片下载失败：下载内容为空（可能图片地址错误/无权限/已过期）：url={safe_trim(str(first_image_url), 400)!r}"
            )

        # 明显不是图片的响应（常见：返回 HTML 错误页/JSON 错误体）
        ct = ""
        try:
            ct = str((img_headers or {}).get("content-type") or (img_headers or {}).get("Content-Type") or "").lower()
        except Exception:
            ct = ""
        if ("text/html" in ct) or ("application/json" in ct):
            raise NonPenalizedTaskError(
                f"首帧图片下载失败：响应 Content-Type={safe_trim(ct, 120)!r}，疑似非图片（请检查图片地址）：url={safe_trim(str(first_image_url), 400)!r}"
            )
        filename, mime_type = _guess_image_filename_and_mime(img_headers)
        media_id = await _sora_api_upload_image_bytes_pw(
            page,
            target_url=target_url,
            bearer_token=str(bearer_token),
            image_bytes=img_bytes,
            filename=filename,
            mime_type=mime_type,
            log_file=log_file,
        )
        inpaint_items = [{"kind": "upload", "upload_id": str(media_id)}]

    create_payload: Dict[str, Any] = {
        "kind": "video",
        "prompt": prompt,
        "orientation": str(orientation or "portrait"),
        "size": "small",
        "n_frames": int(_pick_n_frames(n_frames)),
        "model": "sy_8",
        "inpaint_items": inpaint_items,
        "style_id": None,
    }

    create_url = _sora_backend_url_from_target(target_url, "/backend/nf/create")
    headers: Dict[str, str] = {
        "Authorization": f"Bearer {bearer_token}",
        "OpenAI-Sentinel-Token": str(sentinel_token),
        "Content-Type": "application/json",
        "OAI-Language": "en-US",
        "OAI-Device-Id": str(oai_device_id),
    }

    append_log(log_file, "\n" + "=" * 100)
    append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [sora][api] nf/create start url={create_url!r}")
    append_log(log_file, f"[sora][api] ua={safe_trim(headers.get('User-Agent') or '', 140)!r} device_id={oai_device_id!r}")
    append_log(log_file, f"[sora][api] payload={safe_trim(json.dumps(create_payload, ensure_ascii=False), 1200)!r}")

    create_tx = await page_fetch_tx(page, url=create_url, method="POST", headers=headers, json_data=create_payload, log_file=log_file)

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

    if status_i != 200 or not task_id:
        raise RuntimeError(f"create 未成功或未解析到任务ID：status={status_i} body={safe_trim(body_text, 400)}")

    auth_state: Dict[str, Any] = {
        "bearer_token": bearer_token,
        "sentinel_token": sentinel_token,
        "user_agent": user_agent,
        "oai_device_id": oai_device_id,
        "create_url": create_url,
    }
    return str(task_id), create_tx, auth_state


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
class SoraSession:
    """按 window 维度缓存的 Sora 会话。

说明：
- 会话内复用同一个指纹浏览器窗口与 Playwright CDP 连接。
- create 必须串行（create_lock）；页面操作互斥（pw_ctx.driver_lock）。
"""

    cache_key: str
    pw_ctx: PlaywrightBrowserContext

    last_used_at: float = field(default_factory=lambda: time.time())
    create_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    watchers: Dict[str, _SoraWatcher] = field(default_factory=dict)
    monitor_task: Optional[asyncio.Task] = None
    idle_close_task: Optional[asyncio.Task] = None
    idle_close_disabled: bool = False

    # 监控配置（watch 时更新）
    monitor_log_path: Optional[str] = None
    poll_interval_seconds: float = 1.0
    sniff_timeout_seconds: float = 4.0
    idle_close_seconds: float = 30.0

    # 最近一次 browser_open 参数（用于重连）
    browser_open_args: list[str] = field(default_factory=list)
    browser_force_open: bool = False
    browser_headless: bool = False

    # 鉴权信息（从指纹浏览器网络抓取）
    bearer_token: Optional[str] = None
    user_agent: Optional[str] = None
    oai_device_id: Optional[str] = None
    sentinel_token: Optional[str] = None
    invite_code: Optional[str] = None

    async def _bring_sora_drafts_to_front(self) -> None:
        """将 Sora drafts 页面置前（如果不存在则新开一个）。

        需求背景：指纹浏览器可能开了多个标签页；即使 ensure_open 选中了可用 page，也不一定是 drafts。
        这里确保 `https://sora.chatgpt.com/drafts` 这个窗口在每次 ensure_open 后都会被 bring_to_front。
        """
        drafts_url = "https://sora.chatgpt.com/drafts"

        ctx = getattr(self.pw_ctx, "context", None)
        if ctx is None:
            return

        # 先在已有 pages 中找 drafts
        try:
            pages = list(getattr(ctx, "pages", []) or [])
        except Exception:
            pages = []

        drafts_page = None
        for p in pages:
            try:
                is_closed = bool(getattr(p, "is_closed", lambda: False)())
            except Exception:
                is_closed = False
            if is_closed:
                continue
            try:
                u = str(getattr(p, "url", "") or "").strip()
            except Exception:
                u = ""
            if not u:
                continue
            if u.startswith(drafts_url):
                drafts_page = p
                break

        # 没找到则新开一个 drafts 页
        if drafts_page is None:
            try:
                drafts_page = await ctx.new_page()
            except Exception:
                return
            try:
                await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
            except Exception:
                # 即使 goto 失败，也尽量 bring_to_front（有些环境网络慢/被拦截）
                pass

        try:
            await drafts_page.bring_to_front()
        except Exception:
            pass

        try:
            await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
        except Exception:
            # 即使 goto 失败，也尽量 bring_to_front（有些环境网络慢/被拦截）
            pass

        try:
            await drafts_page.evaluate("() => { try { window.focus(); } catch(e) {} }")
        except Exception:
            pass

    async def ensure_open(
        self,
        *,
        args: Optional[list[str]] = None,
        force_open: bool = False,
        headless: bool = False,
    ) -> None:
        self.last_used_at = time.time()
        await self.pw_ctx.ensure_open(args=args, force_open=force_open, headless=headless)

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
        _drop_sora_session(self.cache_key)

    async def close(self) -> None:
        self._cancel_idle_close()
        t = self.monitor_task
        self.monitor_task = None
        if t and not t.done():
            t.cancel()
        await self.pw_ctx.close_and_drop()

    async def api_nf_check(self, *, target_url: str) -> Dict[str, Any]:
        """读取 Sora 余额：GET /backend/nf/check。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front();
        # 余额查询属于“仍在使用窗口”的行为：不要触发倒计时关窗
        self._cancel_idle_close()
        async with self.pw_ctx.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = await self._ensure_bearer_token(target_url=target_url, log_file=log_file)
            url = _sora_backend_url_from_target(target_url, "/backend/nf/check")
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US"}
            tx = await page_fetch_json(self.pw_ctx.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
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
        await self._bring_sora_drafts_to_front();
        # 查询邀请码属于“仍在使用窗口”的行为：不要触发倒计时关窗
        self._cancel_idle_close()
        async with self.pw_ctx.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = await self._ensure_bearer_token(target_url=target_url, log_file=log_file)
            url = _sora_backend_url_from_target(target_url, "/backend/project_y/invite/mine")
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US"}
            tx = await page_fetch_tx(self.pw_ctx.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
            status = int(tx.get("status") or 0) if tx.get("status") is not None else 0
            body = str(tx.get("response_body") or "")
            obj: Any = None
            try:
                obj = json.loads(body) if body else None
            except Exception:
                obj = None

            if status == 401:
                try:
                    boot_url = _sora_backend_url_from_target(target_url, "/backend/m/bootstrap")
                    await page_fetch_tx(self.pw_ctx.page, url=boot_url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    tx2 = await page_fetch_json(self.pw_ctx.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
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

    async def _ensure_bearer_token(self, *, target_url: str, log_file: Path) -> str:
        if self.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")
        if self.bearer_token:
            return str(self.bearer_token)
        try:
            await self.pw_ctx.page.goto(target_url, wait_until="domcontentloaded")
        except Exception:
            pass
        try:
            await self.pw_ctx.page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass
        info = await _sora_extract_bearer_from_any_post_pw(self.pw_ctx.page, timeout_seconds=20.0, log_file=log_file)
        tok = str(info.get("token") or "").strip()
        if not tok:
            raise RuntimeError("未抓到 Bearer token（请确认窗口已登录 Sora）")
        self.bearer_token = tok
        try:
            self.user_agent = str(info.get("user_agent") or "").strip() or self.user_agent
        except Exception:
            pass
        return tok

    async def create_task(
        self,
        *,
        prompt: str,
        target_url: str,
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
        self.monitor_log_path = monitor_log_path

        self.browser_open_args = browser_open_args or []
        self.browser_force_open = bool(browser_force_open)
        self.browser_headless = bool(browser_headless)

        async with self.create_lock:
            try:
                await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
                await self._bring_sora_drafts_to_front();
                async with self.pw_ctx.driver_lock:
                    task_id, create_tx, auth_state = await _sora_create_task_pw(
                        page=self.pw_ctx.page,
                        prompt=prompt,
                        target_url=target_url,
                        monitor_log_path=monitor_log_path,
                        first_image_url=first_image_url,
                        orientation=orientation,
                        n_frames=n_frames,
                    )
                    try:
                        self.bearer_token = auth_state.get("bearer_token")
                        self.sentinel_token = auth_state.get("sentinel_token")
                        self.user_agent = auth_state.get("user_agent")
                        self.oai_device_id = auth_state.get("oai_device_id")
                    except Exception:
                        pass
                    return task_id, create_tx
            finally:
                # create_task 后通常仍会继续监控/发布，需要窗口保持前端状态
                self._cancel_idle_close()

    async def watch_task_progress(
        self,
        *,
        task_id: str,
        progress_cb: ProgressCB,
        max_wait_seconds: float,
        poll_interval_seconds: float,
        sniff_timeout_seconds: float,
        idle_close_seconds: float,
    ) -> Dict[str, Any]:
        self.last_used_at = time.time()
        self._cancel_idle_close()

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
                # 监控结束不代表“窗口不用了”，不要自动进入倒计时关窗
                self._cancel_idle_close()

    async def _monitor_loop(self) -> None:
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

                try:
                    await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
                except Exception as e:
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError(f"浏览器/driver 不可用：{e}"))
                        self.watchers.pop(tid, None)
                    return

                log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
                if not self.bearer_token:
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError("缺少 bearer_token，无法轮询 pending（create 未成功抓取鉴权信息）"))
                        self.watchers.pop(tid, None)
                    return

                try:
                    pending_url = _sora_backend_url_from_target(
                        getattr(self.pw_ctx.page, "url", "") or "https://sora.chatgpt.com", "/backend/nf/pending/v2"
                    )
                except Exception:
                    pending_url = "https://sora.chatgpt.com/backend/nf/pending/v2"

                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {self.bearer_token}",
                    "OAI-Language": "en-US",
                    "OAI-Device-Id": str(self.oai_device_id or ""),
                }

                tx: Optional[Dict[str, Any]] = None
                async with self.pw_ctx.driver_lock:
                    try:
                        tx = await page_fetch_tx(self.pw_ctx.page, url=pending_url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    except Exception as e:
                        append_log(log_file, f"[sora][api] pending poll failed: {e}")
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

                missing_tids: list[str] = []
                for tid, w in list(self.watchers.items()):
                    task_obj = index.get(str(tid)) if index else _extract_task_obj(payload_obj, str(tid))
                    if not task_obj:
                        w.miss_pending_count += 1
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
                    try:
                        drafts = await _sora_api_get_video_drafts_pw(
                            self.pw_ctx.page,
                            target_url=str(getattr(self.pw_ctx.page, "url", "") or "https://sora.chatgpt.com/drafts"),
                            bearer_token=str(self.bearer_token or ""),
                            limit=15,
                            log_file=log_file,
                        )
                        items = drafts.get("items", []) if isinstance(drafts, dict) else []
                    except Exception as e:
                        items = []
                        try:
                            append_log(log_file, f"[sora][drafts] fetch failed: {e}")
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
                            if tid in self.watchers:
                                w = self.watchers.get(tid)
                                if w and not w.future.done():
                                    w.future.set_result({"task_id": tid, "status": "completed", "progress_pct": 1.0, "done": True, "draft": it})
                                self.watchers.pop(tid, None)

                if not self.watchers:
                    return
                await asyncio.sleep(float(self.poll_interval_seconds))
        finally:
            if not self.watchers:
                # monitor loop 退出不代表“窗口不用了”，不要自动进入倒计时关窗
                self._cancel_idle_close()

    async def finalize_video_and_publish(
        self,
        *,
        task_id: str,
        prompt: str,
        target_url: str,
        drafts_limit: int = 100,
    ) -> Dict[str, Any]:
        """任务完成后：从 drafts 找到对应视频 → 发布草稿（去水印）→ 返回 {post_id, urls, draft}。"""
        _ = prompt  # 预留：未来扩展（例如校验 prompt 匹配）
        self.last_used_at = time.time()
        # 发布/轮询 drafts 期间仍在使用窗口：不要触发倒计时关窗
        self._cancel_idle_close()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await asyncio.sleep(5);
        await self._bring_sora_drafts_to_front();
        async with self.pw_ctx.driver_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            if not self.bearer_token:
                raise RuntimeError("缺少 bearer_token，无法查询 drafts/发布")
            self.sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(self.pw_ctx.page, device_id=self.oai_device_id, log_file=log_file)
            if not self.sentinel_token:
                raise RuntimeError("缺少 sentinel_token，无法发布草稿")

            def _get_item_task_id(it: Dict[str, Any]) -> str:
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

            drafts_wait_seconds = 150.0
            drafts_poll_interval = 15.0
            deadline = time.time() + drafts_wait_seconds
            draft_item: Optional[Dict[str, Any]] = None
            last_items_sample: list[str] = []
            attempt = 0
            while time.time() < deadline and draft_item is None:
                attempt += 1
                drafts = await _sora_api_get_video_drafts_pw(
                    self.pw_ctx.page,
                    target_url=target_url,
                    bearer_token=str(self.bearer_token),
                    limit=int(drafts_limit),
                    log_file=log_file,
                )
                items = drafts.get("items", []) if isinstance(drafts, dict) else []
                if not isinstance(items, list):
                    items = []

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

                append_log(
                    log_file,
                    f"[sora][drafts] poll attempt={attempt} found={bool(draft_item)} items={len(items)} "
                    f"sample_task_ids={safe_trim(','.join(last_items_sample), 260)!r}",
                )

                if draft_item is not None:
                    break
                await asyncio.sleep(float(drafts_poll_interval))
                await self._bring_sora_drafts_to_front();

            if not draft_item:
                raise RuntimeError(
                    f"草稿箱未找到任务对应视频（已轮询 {drafts_wait_seconds:.0f}s）：task_id={task_id} "
                    f"sample_task_ids={safe_trim(','.join(last_items_sample), 260)}"
                )

            generation_id = str(draft_item.get("id") or "").strip()
            if not generation_id:
                raise RuntimeError("草稿箱记录缺少 generation_id（draft_item.id）")

            post_resp = await _sora_api_post_project_y_post_pw(
                self.pw_ctx.page,
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
                raise RuntimeError(f"发布草稿失败：未返回 post_id resp={safe_trim(json.dumps(post_resp, ensure_ascii=False), 600)}")

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


_SORA_LOCK = asyncio.Lock()
_SORA_SESSIONS: Dict[str, SoraSession] = {}


def _sora_key(vendor: str, base_url: str, space_id: str, window_key: str) -> str:
    return "|".join([(vendor or "").strip().lower(), (base_url or "").strip().lower(), (space_id or "").strip(), (window_key or "").strip()])


def _drop_sora_session(cache_key: str) -> None:
    k = (cache_key or "").strip()
    if not k:
        return
    _SORA_SESSIONS.pop(k, None)


def get_or_create_sora_session(
    *,
    vendor: str,
    base_url: str,
    access_key: Optional[str],
    space_id: str,
    window_key: str,
) -> SoraSession:
    """对外提供：用于 admin/registry 等复用同一 window 的 Sora 会话。"""
    k = _sora_key(vendor, base_url, space_id, window_key)
    # 说明：这里用简单字典缓存；并发创建窗口时由内部 ensure_open/create_lock 兜底
    sess = _SORA_SESSIONS.get(k)
    if sess is None:
        pw_ctx = get_or_create_playwright_ctx(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
        sess = SoraSession(cache_key=k, pw_ctx=pw_ctx)
        _SORA_SESSIONS[k] = sess
    else:
        sess.pw_ctx.access_key = access_key
    return sess


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
    """Sora 生视频：复用同一指纹浏览器窗口 + Playwright(CDP) 轻量连接，拆分“创建任务”和“进度轮询”。"""
    payload = payload or {}
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("payload.prompt 不能为空")

    first_image_url = str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip() or None
    ratio = str(payload.get("size_ratio") or payload.get("aspect_ratio") or payload.get("ratio") or payload.get("尺寸") or "").strip() or None
    orientation = _pick_orientation_from_ratio(ratio) or str(payload.get("orientation") or "").strip() or None
    duration_v = payload.get("n_frames") or payload.get("duration_frames") or payload.get("duration") or payload.get("时长")
    n_frames = _pick_n_frames(duration_v)

    target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
    monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)

    max_wait_seconds = float(payload.get("sora_pending_max_wait_seconds") or max(30.0, min(float(timeout_seconds), 60.0 * 10)))
    poll_interval_seconds = float(payload.get("sora_pending_poll_interval_seconds") or 1.0)
    sniff_timeout_seconds = float(payload.get("sora_pending_sniff_timeout_seconds") or 4.0)
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)

    sess = get_or_create_sora_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )

    await progress_cb(0, {"stage": "create_task"})
    task_id, _create_tx = await sess.create_task(
        prompt=prompt,
        target_url=target_url,
        monitor_log_path=monitor_log_path,
        first_image_url=first_image_url,
        orientation=str(orientation or "portrait"),
        n_frames=int(n_frames),
        browser_open_args=[],
        browser_force_open=False,
        browser_headless=False,
    )
    await sess._bring_sora_drafts_to_front();
    await progress_cb(1, {"stage": "created", "task_id": task_id})
    await progress_cb(1, {"stage": "monitor_progress", "task_id": task_id})
    progress_result = await sess.watch_task_progress(
        task_id=task_id,
        progress_cb=progress_cb,
        max_wait_seconds=max_wait_seconds,
        poll_interval_seconds=poll_interval_seconds,
        sniff_timeout_seconds=sniff_timeout_seconds,
        idle_close_seconds=idle_close_seconds,
    )

    await progress_cb(95, {"stage": "drafts_and_publish", "task_id": task_id})
    publish_result = await sess.finalize_video_and_publish(
        task_id=task_id,
        prompt=prompt,
        target_url=target_url,
        drafts_limit=int(payload.get("sora_drafts_limit") or 100),
    )

    nf_check = None
    nf_check_err: Optional[Exception] = None
    try:
        nf_check = await sess.api_nf_check(target_url=target_url)
    except Exception as e:
        nf_check_err = e
        nf_check = None

    # 仅当“任务执行完毕后确认余额 <= 1”，或“查询余额报错并将导致禁用”时，才启动倒计时关窗
    try:
        if nf_check_err is not None:
            sess._schedule_idle_close()
        else:
            remaining = int((nf_check or {}).get("remaining_count") or 0)
            if remaining <= 1:
                sess._schedule_idle_close()
            else:
                sess._cancel_idle_close()
    except Exception:
        # 不要让关窗策略影响主流程返回
        pass

    await progress_cb(100, {"stage": "done", "task_id": task_id, "post_id": publish_result.get("post_id")})
    _ = progress_result

    return {
        "type": "video",
        "message": "Sora创建完成",
        "task_id": task_id,
        "post_id": publish_result.get("post_id"),
        "share_url": publish_result.get("share_url"),
        "watermark_free_url": publish_result.get("watermark_free_url"),
        "nf_check": nf_check,
    }

