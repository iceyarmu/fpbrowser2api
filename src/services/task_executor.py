"""旧版执行器实现（保留大量 Sora helper 逻辑）。

当前推荐的拆分结构：
- 图片：`image_task_executor.py`
- 视频：`video_task_executor.py`
- Sora：`sora_task_executor.py` + `sora_browser_context.py`

本文件仍保留（兼容 + 复用 helper），避免一次性迁移带来风险。
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from uuid import uuid4
from urllib.parse import urlparse
from urllib.request import Request, urlopen

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
    # 兼容入口：转发到拆分后的模块
    from .image_task_executor import simulate_image_task as _impl

    return await _impl(prompt, image_path, progress_cb)


async def simulate_video_task(prompt: str, image_path: Optional[str], progress_cb: ProgressCB) -> Dict[str, Any]:
    # 兼容入口：转发到拆分后的模块
    from .video_task_executor import simulate_video_task as _impl

    return await _impl(prompt, image_path, progress_cb)

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


async def _sora_extract_bearer_from_any_post_pw(page, *, timeout_seconds: float, log_file: Path) -> Dict[str, Any]:
    """监听任意 POST 请求，从 headers 提取 Authorization: Bearer <token>。

    返回：
    {
      "seen": bool,
      "authorization": "Bearer xxx" | None,
      "token": "xxx" | None,
      "user_agent": str|None,
      "url": str|None
    }
    """
    result: Dict[str, Any] = {"seen": False, "authorization": None, "token": None, "user_agent": None, "url": None}
    loop = asyncio.get_running_loop()
    fut: "asyncio.Future[Dict[str, Any]]" = loop.create_future()

    def _on_request(req) -> None:
        if fut.done():
            return
        try:
            m = str(getattr(req, "method", "") or "").upper().strip()
            if m != "POST" and m != "GET":
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
        _append_log(log_file, f"[sora][token] attach request listener failed: {e}")
        return result

    try:
        data = await asyncio.wait_for(fut, timeout=max(1.0, float(timeout_seconds)))
        result.update(data or {})
        _append_log(
            log_file,
            f"[sora][token] bearer_captured url={_safe_trim(str(result.get('url') or ''), 240)!r} "
            f"ua={_safe_trim(str(result.get('user_agent') or ''), 120)!r} "
            f"auth={_mask_secret(str(result.get('authorization') or ''), head=15, tail=15)!r}",
        )
        return result
    except Exception as e:
        _append_log(log_file, f"[sora][token] bearer capture timeout/failed: {e}")
        # 顺手记录一下近期 POST，方便判断“页面到底有没有 POST”
        try:
            await _pw_log_recent_posts(page, seconds=2.0, log_file=log_file)
        except Exception:
            pass
        return result
    finally:
        try:
            page.off("request", _on_request)
        except Exception:
            pass


async def _sora_generate_sentinel_token_in_fp_context_pw(page, *, device_id: Optional[str], log_file: Path) -> Optional[str]:
    """在“指纹浏览器的同一 context”中，用 __sentinel__ hack 生成 SentinelToken（参考 sora_client.py）。"""
    try:
        ctx = page.context
    except Exception:
        ctx = None
    if ctx is None:
        _append_log(log_file, "[sora][sentinel] page.context unavailable")
        return None

    # 优先从 cookie 取 oai-did（若无则随机一个）
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
        _append_log(log_file, f"[sora][sentinel] new_page failed: {e}")
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
            # 放行 sentinel sdk / 调用链；其余资源 abort，降低加载量
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
        _append_log(log_file, f"[sora][sentinel] loading sdk under did={did!r}")
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
            _append_log(log_file, f"[sora][sentinel] token_ok value={_mask_secret(token_s, head=15, tail=15)!r}")
            return token_s
        _append_log(log_file, f"[sora][sentinel] token_error value={_safe_trim(token_s, 300)!r}")
        return None
    except Exception as e:
        _append_log(log_file, f"[sora][sentinel] generate failed: {e}")
        return None
    finally:
        try:
            await p2.close()
        except Exception:
            pass


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


async def _pw_api_post_json(ctx, *, url: str, headers: Dict[str, str], json_data: Dict[str, Any], log_file: Path) -> Dict[str, Any]:
    """使用 BrowserContext.request 发 POST JSON，返回兼容 tx 结构。"""
    tx: Dict[str, Any] = {
        "seen": True,
        "request_id": None,
        "url": url,
        "method": "POST",
        "status": None,
        "response_body": None,
        "headers": None,
        "log_file": str(log_file),
    }
    req_ctx = None
    try:
        req_ctx = getattr(ctx, "request", None)
    except Exception:
        req_ctx = None
    if req_ctx is None:
        raise RuntimeError("context.request 不可用，无法发起 API 请求")

    resp = None
    try:
        try:
            resp = await req_ctx.post(url, headers=headers, json=json_data, timeout=30_000)
        except TypeError:
            # 兼容旧版本参数
            resp = await req_ctx.post(url, headers=headers, data=json.dumps(json_data), timeout=30_000)
    except Exception as e:
        _append_log(log_file, f"[sora][api] POST failed url={url!r} err={e}")
        raise

    try:
        try:
            tx["status"] = int(getattr(resp, "status", None))
        except Exception:
            tx["status"] = None
        try:
            tx["headers"] = dict(await resp.headers())
        except Exception:
            try:
                tx["headers"] = dict(getattr(resp, "headers", None) or {})
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
        _append_log(log_file, f"[sora][api] POST url={url!r} status={tx['status']}")
        _append_log(log_file, f"[sora][api] response_body={_safe_trim(body_text, 800)!r}")
        return tx
    finally:
        try:
            await resp.dispose()
        except Exception:
            pass


async def _pw_api_get(ctx, *, url: str, headers: Dict[str, str], log_file: Path) -> Dict[str, Any]:
    tx: Dict[str, Any] = {
        "seen": True,
        "request_id": None,
        "url": url,
        "method": "GET",
        "status": None,
        "response_body": None,
        "headers": None,
        "log_file": str(log_file),
    }
    req_ctx = None
    try:
        req_ctx = getattr(ctx, "request", None)
    except Exception:
        req_ctx = None
    if req_ctx is None:
        raise RuntimeError("context.request 不可用，无法发起 API 请求")

    resp = None
    try:
        resp = await req_ctx.get(url, headers=headers, timeout=30_000)
    except Exception as e:
        _append_log(log_file, f"[sora][api] GET failed url={url!r} err={e}")
        raise

    try:
        try:
            tx["status"] = int(getattr(resp, "status", None))
        except Exception:
            tx["status"] = None
        try:
            tx["headers"] = dict(await resp.headers())
        except Exception:
            try:
                tx["headers"] = dict(getattr(resp, "headers", None) or {})
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
        return tx
    finally:
        try:
            await resp.dispose()
        except Exception:
            pass


async def _pw_page_fetch_tx(
    page,
    *,
    url: str,
    method: str,
    headers: Dict[str, str],
    json_data: Optional[Dict[str, Any]],
    log_file: Path,
) -> Dict[str, Any]:
    """在“浏览器页面上下文”里 fetch（走指纹浏览器自己的网络栈/代理/DNS），返回兼容 tx 结构。

    重要：浏览器侧无法设置 User-Agent/Host/Cookie 等受限 header，这些会由浏览器自动携带。
    """
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

    # 去掉浏览器禁止设置的 headers（避免 fetch 直接抛 TypeError）
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
        _append_log(log_file, f"[sora][page_fetch] fetch failed url={url!r} err={e}")
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

    _append_log(log_file, f"[sora][page_fetch] {tx['method']} url={url!r} status={tx['status']}")
    _append_log(log_file, f"[sora][page_fetch] body={_safe_trim(str(tx.get('response_body') or ''), 800)!r}")
    return tx


async def _pw_page_fetch_json(
    page,
    *,
    url: str,
    method: str,
    headers: Dict[str, str],
    json_data: Optional[Dict[str, Any]],
    log_file: Path,
) -> Dict[str, Any]:
    """页面内 fetch 并解析 JSON（失败则抛出带 response 文本摘要的异常）。"""
    tx = await _pw_page_fetch_tx(page, url=url, method=method, headers=headers, json_data=json_data, log_file=log_file)
    body = str(tx.get("response_body") or "")
    try:
        obj = json.loads(body) if body else None
    except Exception:
        obj = None
    if obj is None and (tx.get("status") not in (204,)):
        raise RuntimeError(f"fetch_json 解析失败：status={tx.get('status')} body={_safe_trim(body, 600)}")
    tx["_json"] = obj
    return tx


async def _sora_api_get_video_drafts_pw(page, *, target_url: str, bearer_token: str, limit: int, log_file: Path) -> Dict[str, Any]:
    """GET /backend/project_y/profile/drafts?limit=..."""
    url = _sora_backend_url_from_target(target_url, f"/backend/project_y/profile/drafts?limit={int(limit)}")
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "OAI-Language": "en-US",
    }
    tx = await _pw_page_fetch_json(page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
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
    payload = {
        "attachments_to_create": [{"generation_id": str(generation_id), "kind": "sora"}],
        "post_text": "",
    }
    tx = await _pw_page_fetch_json(page, url=url, method="POST", headers=headers, json_data=payload, log_file=log_file)
    obj = tx.get("_json")
    return obj if isinstance(obj, dict) else {}


async def _pw_download_bytes_via_context_pw(ctx, *, url: str, timeout_ms: int = 30_000) -> Tuple[bytes, Dict[str, str]]:
    """用同一指纹浏览器 context 下载资源（走浏览器网络栈），返回 (bytes, headers)。"""
    p = await ctx.new_page()
    try:
        resp = await p.goto(url, wait_until="networkidle", timeout=int(timeout_ms))
        if resp is None:
            raise RuntimeError(f"下载失败：无响应 url={url!r}")
        try:
            status = int(getattr(resp, "status", 0) or 0)
        except Exception:
            status = 0
        if status and status >= 400:
            raise RuntimeError(f"下载失败：status={status} url={url!r}")
        data = await resp.body()
        try:
            hdrs = dict(resp.headers or {})
        except Exception:
            hdrs = {}
        return bytes(data or b""), hdrs
    finally:
        try:
            await p.close()
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
    """上传首帧图片获取 media_id（参考 sora_client.upload_image）。"""
    candidates = [
        _sora_backend_url_from_target(target_url, "/backend/uploads"),
        _sora_backend_url_from_target(target_url, "/uploads"),
    ]
    try:
        import base64

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
            _append_log(log_file, f"[sora][upload] url={upload_url!r} status={status} body={_safe_trim(text, 500)!r}")
            if status not in (200, 201):
                last_err = f"status={status} body={_safe_trim(text, 300)}"
                continue
            try:
                obj = json.loads(text) if text else {}
            except Exception:
                obj = {}
            media_id = str((obj or {}).get("id") or "").strip()
            if media_id:
                return media_id
            last_err = f"missing id body={_safe_trim(text, 300)}"
        except Exception as e:
            last_err = str(e)
            continue

    raise RuntimeError(f"上传首帧失败：{last_err}")


def _download_bytes_local(url: str, *, timeout_seconds: float = 30.0, user_agent: Optional[str] = None) -> Tuple[bytes, Dict[str, str]]:
    """本地下载资源（按你的要求：不走指纹浏览器下载首帧图）。"""
    u = str(url or "").strip()
    if not u:
        raise ValueError("url 不能为空")
    headers = {
        "Accept": "*/*",
    }
    if user_agent:
        headers["User-Agent"] = str(user_agent)
    req = Request(u, headers=headers, method="GET")
    with urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
        data = resp.read()
        # Python 返回的是 HTTPMessage，可转成 dict
        try:
            hdrs = dict(getattr(resp, "headers", {}) or {})
        except Exception:
            hdrs = {}
        return bytes(data or b""), hdrs


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
    # 默认写到 fpbrowser2api 根目录，避免在 src/ 下产生提交噪音
    log_file = Path(log_path) if log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

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
    # 默认写到 fpbrowser2api 根目录，避免在 src/ 下产生提交噪音
    log_file = Path(log_path) if log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
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

    # 新增：打开目标页后，从指纹浏览器 Network 抓 Bearer + 生成 Sentinel（后续用于直接 nf/create POST）
    # 安全起见：这里只打印脱敏值；完整值保存在变量中供下一步请求使用
    bearer_info = await _sora_extract_bearer_from_any_post_pw(page, timeout_seconds=20.0, log_file=log_file)
    bearer_token = str(bearer_info.get("token") or "").strip() or None
    user_agent = str(bearer_info.get("user_agent") or "").strip() or None
    if not user_agent:
        user_agent = await _pw_get_user_agent(page)
    sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(page, device_id=None, log_file=log_file)


    if not bearer_token:
        raise NonPenalizedTaskError("未能从指纹浏览器 Network 抓到 Bearer token（请确保已登录且页面会发出带 Authorization 的请求）")
    if not sentinel_token:
        raise NonPenalizedTaskError("未能生成 SentinelToken（__sentinel__ SDK 注入失败/被拦截）")

    # 从 SentinelToken JSON 里提取 device_id（与 sora_client.py 行为一致）
    oai_device_id = None
    try:
        sentinel_data = json.loads(str(sentinel_token))
        if isinstance(sentinel_data, dict):
            oai_device_id = str(sentinel_data.get("id") or "").strip() or None
    except Exception:
        oai_device_id = None
    if not oai_device_id:
        oai_device_id = str(uuid4())

    # 首帧参考图：下载 -> 上传拿 media_id -> 写入 inpaint_items（参考 sora_client.generate_video）
    inpaint_items: list[Dict[str, Any]] = []
    if first_image_url:
        img_bytes, img_headers = _download_bytes_local(first_image_url, timeout_seconds=30.0, user_agent=user_agent)
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

    # 构造 nf/create payload（参考 sora2api/sora_client.py generate_video）
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

    _append_log(log_file, "\n" + "=" * 100)
    _append_log(log_file, f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [sora][api] nf/create start url={create_url!r}")
    _append_log(log_file, f"[sora][api] ua={_safe_trim(headers.get('User-Agent') or '', 140)!r} device_id={oai_device_id!r}")
    _append_log(log_file, f"[sora][api] payload={_safe_trim(json.dumps(create_payload, ensure_ascii=False), 1200)!r}")

    # 使用“页面内 fetch”发起 nf/create（走指纹浏览器网络栈，避免 APIRequestContext IPv6/DNS/代理差异）
    create_tx = await _pw_page_fetch_tx(page, url=create_url, method="POST", headers=headers, json_data=create_payload, log_file=log_file)

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
            raise RuntimeError(f"Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；错误：{e}")

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
        self._schedule_idle_close();
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
                                    w.future.set_result({"task_id": tid, "status": "completed", "progress_pct": 1.0, "done": True, "draft": it})
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

    # 新增：首帧参考图 / 尺寸比例 / 时长（映射到 nf/create 的 inpaint_items/orientation/n_frames）
    first_image_url = str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip() or None
    ratio = str(payload.get("size_ratio") or payload.get("aspect_ratio") or payload.get("ratio") or payload.get("尺寸") or "").strip() or None
    orientation = _pick_orientation_from_ratio(ratio) or str(payload.get("orientation") or "").strip() or None
    duration_v = payload.get("n_frames") or payload.get("duration_frames") or payload.get("duration") or payload.get("时长")
    n_frames = _pick_n_frames(duration_v)

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
        first_image_url=first_image_url,
        orientation=str(orientation or "portrait"),
        n_frames=int(n_frames),
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

    await progress_cb(95, {"stage": "drafts_and_publish", "task_id": task_id})
    publish_result = await ctx.finalize_video_and_publish(
        task_id=task_id,
        prompt=prompt,
        target_url=target_url,
        drafts_limit=int(payload.get("sora_drafts_limit") or 100),
    )
    # 成功后顺带读取余额（nf/check）
    nf_check = None
    try:
        nf_check = await ctx.api_nf_check(target_url=target_url)
    except Exception:
        nf_check = None
    await progress_cb(100, {"stage": "done", "task_id": task_id, "post_id": publish_result.get("post_id")})

    result: Dict[str, Any] = {
        "type": "video",
        "message": "Sora创建完成",
        "task_id": task_id,
        "post_id": publish_result.get("post_id"),
        "share_url": publish_result.get("share_url"),
        "watermark_free_url": publish_result.get("watermark_free_url"),
        "nf_check": nf_check,
    }

    return result