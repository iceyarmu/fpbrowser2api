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