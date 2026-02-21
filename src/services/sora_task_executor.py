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
import contextvars
import json
import os
import random
import tempfile
import time
from contextlib import asynccontextmanager
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
from ..core.database import Database


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

async def sora_fetch_access_token_in_window(
    *,
    sess: "SoraSession",
    target_url: str = "https://sora.chatgpt.com/drafts",
) -> Dict[str, Any]:
    """在“指纹浏览器窗口页面上下文”里调用 `/api/auth/session` 获取 access_token + expires。

    说明：
    - 使用 `page_fetch_json`（credentials: include），会自动携带该窗口 Cookie（含代理/指纹网络栈）。
    - 不允许手工传 Cookie header（page_fetch_tx 会屏蔽 cookie/ua/origin/referer 等头）。
    """
    if sess.pw_ctx.page is None:
        raise RuntimeError("page 未初始化")

    await sess.ensure_open(args=sess.browser_open_args, force_open=sess.browser_force_open, headless=sess.browser_headless)
    await sess._bring_sora_drafts_to_front(refresh_target=False)

    log_file = Path(sess.monitor_log_path) if sess.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
    url = "https://sora.chatgpt.com/api/auth/session"
    tx = await page_fetch_json(sess.pw_ctx.page, url=url, method="GET", headers={"Accept": "application/json"}, json_data=None, log_file=log_file)
    data = tx.get("_json")
    if not isinstance(data, dict):
        raise RuntimeError(f"读取 session 失败：status={tx.get('status')} body={safe_trim(str(tx.get('response_body') or ''), 500)!r}")

    access_token = str(data.get("accessToken") or "").strip() or None
    expires = str(data.get("expires") or "").strip() or None
    user = data.get("user") if isinstance(data.get("user"), dict) else {}
    email = str((user or {}).get("email") or "").strip() or None
    if not access_token:
        raise RuntimeError("读取 session 失败：响应缺少 accessToken（请确认窗口已登录 Sora）")
    return {"access_token": access_token, "expires": expires, "email": email}


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


def _download_to_tempfile_local(
    url: str,
    *,
    suffix: str,
    timeout_seconds: float = 60.0,
    user_agent: Optional[str] = None,
    max_bytes: Optional[int] = None,
) -> Tuple[Path, Dict[str, str]]:
    """本地下载资源到临时文件（用于在浏览器上下文以 File 上传）。"""
    u = str(url or "").strip()
    if not u:
        raise ValueError("url 不能为空")

    headers = {"Accept": "*/*"}
    if user_agent:
        headers["User-Agent"] = str(user_agent)
    req = Request(u, headers=headers, method="GET")

    fd = tempfile.NamedTemporaryFile(delete=False, suffix=str(suffix or ""))
    tmp_path = Path(fd.name)
    try:
        fd.close()
        with urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
            try:
                hdrs = dict(getattr(resp, "headers", {}) or {})
            except Exception:
                hdrs = {}
            with open(tmp_path, "wb") as f:
                total = 0
                try:
                    if max_bytes is not None:
                        cl = hdrs.get("Content-Length") or hdrs.get("content-length")
                        if cl is not None:
                            try:
                                if int(cl) > int(max_bytes):
                                    raise RuntimeError(f"下载资源大小超过限制：Content-Length={cl} max_bytes={max_bytes}")
                            except Exception:
                                pass
                except Exception:
                    # 忽略 content-length 解析问题
                    pass
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    if max_bytes is not None:
                        total += len(chunk)
                        if total > int(max_bytes):
                            raise RuntimeError(f"下载资源大小超过限制：max_bytes={max_bytes}")
                    f.write(chunk)
        return tmp_path, hdrs
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)  # type: ignore[call-arg]
        except Exception:
            try:
                os.unlink(str(tmp_path))
            except Exception:
                pass
        raise


def _mp4_duration_seconds(path: Path) -> float:
    """解析 MP4 容器时长（秒）。

    仅用于“角色视频”校验（≤5秒），不引入第三方依赖。
    """
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"视频文件不存在：{p}")
    size = p.stat().st_size
    if size <= 0:
        raise RuntimeError("视频文件为空")

    CONTAINERS = {
        b"moov",
        b"trak",
        b"mdia",
        b"minf",
        b"stbl",
        b"edts",
        b"udta",
        b"meta",
        b"ilst",
        b"moof",
    }

    def _read(n: int) -> bytes:
        b = f.read(n)
        if len(b) != n:
            raise EOFError("unexpected eof")
        return b

    def _u32(b: bytes) -> int:
        return int.from_bytes(b, "big", signed=False)

    def _u64(b: bytes) -> int:
        return int.from_bytes(b, "big", signed=False)

    with open(p, "rb") as f:
        # 用栈模拟递归进入 container box
        stack = [int(size)]
        while stack and f.tell() < stack[-1]:
            start = f.tell()
            try:
                hdr = f.read(8)
            except Exception:
                break
            if len(hdr) < 8:
                break

            box_size = _u32(hdr[0:4])
            box_type = hdr[4:8]
            header_size = 8
            if box_size == 1:
                try:
                    box_size = _u64(_read(8))
                except Exception:
                    break
                header_size = 16
            elif box_size == 0:
                # extends to end of container
                box_size = int(stack[-1] - start)

            if box_size < header_size:
                # 无效 box，避免死循环
                break

            box_end = int(start + box_size)

            if box_type == b"mvhd":
                try:
                    version_flags = _read(4)
                    version = version_flags[0]
                    if version == 1:
                        _read(8)  # creation_time
                        _read(8)  # modification_time
                        timescale = _u32(_read(4))
                        duration = _u64(_read(8))
                    else:
                        _read(4)  # creation_time
                        _read(4)  # modification_time
                        timescale = _u32(_read(4))
                        duration = _u32(_read(4))
                    if timescale <= 0:
                        raise RuntimeError("timescale 无效")
                    return float(duration) / float(timescale)
                except Exception as e:
                    raise RuntimeError(f"解析 mvhd 失败：{e}")

            if box_type in CONTAINERS:
                # 进入 container：meta 需要跳过 4 bytes version/flags
                if box_type == b"meta":
                    try:
                        _read(4)
                    except Exception:
                        break
                stack.append(box_end)
                continue

            # 跳过 box 内容
            try:
                f.seek(box_end)
            except Exception:
                break

            # 弹出已结束的 container
            while stack and f.tell() >= stack[-1]:
                stack.pop()

    raise RuntimeError("无法解析 MP4 时长（未找到 mvhd）")


async def _sora_api_upload_file_via_input_fetch_pw(
    page,
    *,
    upload_url: str,
    bearer_token: str,
    file_path: Path,
    file_field_name: str = "file",
    extra_fields: Optional[Dict[str, str]] = None,
    log_file: Path,
    timeout_ms: int = 120_000,
) -> Tuple[int, str]:
    """在页面内用 `<input type=file>` + `fetch(FormData)` 上传本地文件（可带 Authorization 头）。

    说明：
    - 避免把大文件 bytes/base64 传入 page.evaluate（视频可能很大）。
    - 通过指纹浏览器窗口网络栈发起请求（credentials: include）。
    """
    p = Path(file_path)
    if not p.exists():
        raise RuntimeError(f"本地文件不存在：{p}")

    # 关键：必须让页面处于与 upload_url 同源的站点上下文，否则 fetch 可能被 CORS 以 “Failed to fetch” 拦截。
    try:
        up = urlparse(str(upload_url))
        up_host = str(up.netloc or "").strip().lower()
        up_scheme = str(up.scheme or "https").strip().lower() or "https"
    except Exception:
        up_host = ""
        up_scheme = "https"
    try:
        cur = urlparse(str(getattr(page, "url", "") or ""))
        cur_host = str(cur.netloc or "").strip().lower()
    except Exception:
        cur_host = ""

    if up_host and cur_host != up_host:
        try:
            await page.goto(f"{up_scheme}://{up_host}/drafts", wait_until="domcontentloaded")
        except Exception as e:
            append_log(log_file, f"[sora][upload] goto same-origin failed: {e}")

    # 注入一个隐藏 input，供 set_input_files 使用
    input_id = "fp_upload_input"
    try:
        await page.evaluate(
            """(args) => {
              const { inputId } = args;
              let el = document.getElementById(inputId);
              if (!el) {
                el = document.createElement('input');
                el.type = 'file';
                el.id = inputId;
                el.style.position = 'fixed';
                el.style.left = '-9999px';
                el.style.top = '-9999px';
                document.body.appendChild(el);
              }
            }""",
            {"inputId": input_id},
        )
    except Exception as e:
        raise RuntimeError(f"注入 file input 失败：{e}")

    try:
        loc = page.locator(f"#{input_id}")
        await loc.wait_for(state="attached", timeout=5_000)
        await loc.set_input_files(str(p), timeout=timeout_ms)
    except Exception as e:
        raise RuntimeError(f"set_input_files 失败：{e}")

    ef = extra_fields or {}
    res = await page.evaluate(
        """async (args) => {
          const { uploadUrl, bearer, fieldName, extraFields, inputId } = args;
          try {
            const input = document.getElementById(inputId);
            const file = input && input.files && input.files[0];
            if (!file) return { status: 0, text: 'missing file' };
            const fd = new FormData();
            fd.append(fieldName || 'file', file, file.name || 'file.bin');
            const extras = extraFields || {};
            for (const k of Object.keys(extras)) {
              try { fd.append(k, String(extras[k])); } catch (e) {}
            }
            try {
              const resp = await fetch(uploadUrl, {
                method: 'POST',
                headers: { 'Authorization': 'Bearer ' + bearer },
                body: fd,
                credentials: 'include',
              });
              const text = await resp.text();
              return { status: resp.status, text };
            } catch (e) {
              return { status: 0, text: 'fetch_error: ' + (e && e.message ? e.message : String(e)) };
            }
          } catch (e) {
            return { status: 0, text: 'js_error: ' + (e && e.message ? e.message : String(e)) };
          }
        }""",
        {
            "uploadUrl": str(upload_url),
            "bearer": str(bearer_token),
            "fieldName": str(file_field_name),
            "extraFields": dict(ef),
            "inputId": input_id,
        },
    )
    status = int((res or {}).get("status") or 0)
    text = str((res or {}).get("text") or "")
    append_log(log_file, f"[sora][upload] url={str(upload_url)!r} status={status} body={safe_trim(text, 600)!r}")
    return status, text


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


async def _sora_api_upload_character_video_pw(
    page,
    *,
    target_url: str,
    bearer_token: str,
    video_path: Path,
    timestamps: str,
    log_file: Path,
) -> str:
    """POST /characters/upload（multipart）：上传视频创建 cameo，返回 cameo_id。"""
    # sora2api 配置 base_url 默认为 /backend，因此这里优先尝试 /backend/characters/upload
    candidates = [
        _sora_backend_url_from_target(target_url, "/backend/characters/upload"),
        _sora_backend_url_from_target(target_url, "/characters/upload"),
    ]

    last_err: Optional[str] = None
    for upload_url in candidates:
        try:
            status, text = await _sora_api_upload_file_via_input_fetch_pw(
                page,
                upload_url=upload_url,
                bearer_token=bearer_token,
                file_path=Path(video_path),
                file_field_name="file",
                extra_fields={"timestamps": str(timestamps or "0,3")},
                log_file=log_file,
                timeout_ms=180_000,
            )
            if status not in (200, 201):
                last_err = f"url={upload_url!r} status={status} body={safe_trim(text, 300)}"
                continue
            try:
                obj = json.loads(text) if text else {}
            except Exception:
                obj = {}
            cameo_id = str((obj or {}).get("id") or "").strip()
            if cameo_id:
                return cameo_id
            last_err = f"url={upload_url!r} missing id body={safe_trim(text, 300)}"
        except Exception as e:
            last_err = f"url={upload_url!r} err={e}"
            continue

    raise RuntimeError(f"上传角色视频失败：{last_err}")


async def _sora_api_upload_character_image_pw(
    page,
    *,
    target_url: str,
    bearer_token: str,
    image_path: Path,
    log_file: Path,
) -> str:
    """POST /project_y/file/upload：上传头像，返回 asset_pointer。"""
    candidates = [
        _sora_backend_url_from_target(target_url, "/backend/project_y/file/upload"),
        _sora_backend_url_from_target(target_url, "/project_y/file/upload"),
    ]

    last_err: Optional[str] = None
    for upload_url in candidates:
        try:
            status, text = await _sora_api_upload_file_via_input_fetch_pw(
                page,
                upload_url=upload_url,
                bearer_token=bearer_token,
                file_path=Path(image_path),
                file_field_name="file",
                extra_fields={"use_case": "profile"},
                log_file=log_file,
                timeout_ms=120_000,
            )
            if status not in (200, 201):
                last_err = f"url={upload_url!r} status={status} body={safe_trim(text, 300)}"
                continue
            try:
                obj = json.loads(text) if text else {}
            except Exception:
                obj = {}
            asset_pointer = str((obj or {}).get("asset_pointer") or "").strip()
            if asset_pointer:
                return asset_pointer
            last_err = f"url={upload_url!r} missing asset_pointer body={safe_trim(text, 300)}"
        except Exception as e:
            last_err = f"url={upload_url!r} err={e}"
            continue

    raise RuntimeError(f"上传角色头像失败：{last_err}")


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


async def _sora_ui_fill_prompt_textarea(page, *, prompt: Any, log_file: Path) -> bool:
    """模拟用户在 textarea 输入 prompt（仅输入，不点击任何按钮；发送由后续 post 接口完成）。"""
    try:
        prompt_s = str(prompt or "")
    except Exception:
        prompt_s = ""
    prompt_s = prompt_s.strip()
    if not prompt_s:
        return False

    # 选择器按“更精确 -> 更通用”降级
    selectors = [
        'textarea[data-testid="prompt-textarea"]',
        'textarea[placeholder*="Describe"]',
        "textarea",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            await loc.first.wait_for(state="visible", timeout=2500)
            try:
                await loc.first.click(timeout=1500)
            except Exception:
                # click 不是必须；focus 失败也继续尝试 fill
                pass

            # 优先用 fill（不会触发点击发送）
            await loc.first.fill(prompt_s, timeout=5000)

            ok = False
            try:
                v = await loc.first.input_value(timeout=1500)
                if str(v or "").strip():
                    ok = True
            except Exception:
                ok = False

            if ok:
                append_log(log_file, f"[sora][ui] prompt filled into textarea selector={sel!r} len={len(prompt_s)}")
                return True
        except Exception:
            continue

    append_log(log_file, "[sora][ui] prompt fill skipped: textarea not found/visible")
    return False


async def _sora_create_task_pw(
    *,
    page,
    prompt: str,
    target_url: str,
    monitor_log_path: Optional[str],
    first_image_url: Optional[str],
    orientation: str,
    n_frames: int,
    bearer_token: Optional[str] = None,
    sentinel_token: Optional[str] = None,
    user_agent: Optional[str] = None,
    oai_device_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """通过指纹浏览器环境直接调用 /backend/nf/create（不再模拟 UI 输入/点击）。"""
    log_file = Path(monitor_log_path) if monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

    # 先生成 SentinelToken（并用于填充 OAI-Device-Id），再确保 BearerToken
    if not sentinel_token:
        sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(page, device_id=oai_device_id, log_file=log_file)

    if not oai_device_id:
        try:
            sentinel_data = json.loads(str(sentinel_token or ""))
            if isinstance(sentinel_data, dict):
                oai_device_id = str(sentinel_data.get("id") or "").strip() or None
        except Exception:
            oai_device_id = None
    if not oai_device_id:
        oai_device_id = str(uuid4())

    bearer_info: Dict[str, Any] = {}
    if not bearer_token:
        bearer_info = await _sora_extract_bearer_from_any_post_pw(page, timeout_seconds=20.0, log_file=log_file)
        bearer_token = str(bearer_info.get("token") or "").strip() or None

    if not user_agent:
        try:
            user_agent = str((bearer_info or {}).get("user_agent") or "").strip() or None
        except Exception:
            user_agent = None
    if not user_agent:
        user_agent = await _pw_get_user_agent(page)

    if not bearer_token:
        raise RuntimeError("未能读取到Bearer token（请确保已登录且页面会发出带 Authorization 的请求）")
    if not sentinel_token:
        raise RuntimeError("未能生成 SentinelToken，触发了429")

    await _sora_ui_fill_prompt_textarea(page, prompt=prompt, log_file=log_file)

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
    _bring_drafts_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    watchers: Dict[str, _SoraWatcher] = field(default_factory=dict)
    monitor_task: Optional[asyncio.Task] = None
    idle_close_task: Optional[asyncio.Task] = None
    idle_close_disabled: bool = False

    # 监控配置（watch 时更新）
    monitor_log_path: Optional[str] = None
    poll_interval_seconds: float = 5.0
    sniff_timeout_seconds: float = 4.0
    idle_close_seconds: float = 30.0

    # 最近一次 browser_open 参数（用于重连）
    browser_open_args: list[str] = field(default_factory=list)
    browser_force_open: bool = False
    browser_headless: bool = False

    # 鉴权信息（从指纹浏览器网络抓取）
    bearer_token: Optional[str] = None
    # 由管理台转换并写入 DB 的 access_token（优先使用）
    access_token: Optional[str] = None
    access_expires: Optional[str] = None
    user_agent: Optional[str] = None
    oai_device_id: Optional[str] = None
    sentinel_token: Optional[str] = None
    invite_code: Optional[str] = None

    def set_access_token(self, access_token: Optional[str], expires: Optional[str] = None) -> None:
        tok = str(access_token or "").strip() or None
        exp = str(expires or "").strip() or None
        self.access_token = tok
        self.access_expires = exp
        # 兼容旧代码路径：依旧使用 bearer_token 字段生成 Authorization 头
        if tok:
            self.bearer_token = tok

    def _get_bearer_token_required(self) -> str:
        tok = str(self.access_token or "").strip() or (str(self.bearer_token or "").strip() or "")
        if not tok:
            raise RuntimeError("缺少 access_token（请在任务类型管理页为该窗口转换并保存 access_token）")
        return tok

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
        # 轮询等待时优先用 fast 判断；只有最终确认时才 deep 看 HTML（更耗时）
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

    async def _wait_cloudflare_auto_pass(self, page, *, max_wait_seconds: float = 10.0) -> bool:
        """等待 Cloudflare 可能自动放行。

        返回：
        - True: 超时后仍像 Cloudflare（可考虑重启）
        - False: 已不再像 Cloudflare（无需重启）
        """
        try:
            deadline = time.time() + max(0.0, float(max_wait_seconds))
        except Exception:
            deadline = time.time() + 10.0

        poll = 1.0
        while time.time() < deadline:
            try:
                is_closed = bool(getattr(page, "is_closed", lambda: False)())
            except Exception:
                is_closed = False
            if is_closed:
                return True

            try:
                still_cf = await self._is_cloudflare_page(page, deep=False)
            except Exception:
                still_cf = True
            if not still_cf:
                return False

            remain = deadline - time.time()
            if remain <= 0:
                break
            try:
                await asyncio.sleep(min(poll, max(0.1, remain)))
            except Exception:
                break
        return True

    async def _restart_window_and_restore_single_drafts(self, *, drafts_url: str, sora_host: str) -> Any:
        """关闭并重开窗口，并确保只保留一个 drafts tab 且置前。"""
        log_file = (
            Path(self.monitor_log_path)
            if self.monitor_log_path
            else (Path(__file__).resolve().parents[2] / "logs.txt")
        )
        try:
            append_log(log_file, "[sora][drafts] detected cloudflare interstitial, restarting fp window once")
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

        # 重开窗口前：清空本地缓存 + 一键随机指纹（降低 Cloudflare “复用旧环境”导致的持续拦截概率）
        try:
            append_log(log_file, "[sora][drafts] pre-open: clear_local_cache + random_env")
        except Exception:
            pass

        try:
            await self.pw_ctx.fp_client.browser_clear_local_cache(
                vendor=self.pw_ctx.vendor,
                base_url=self.pw_ctx.base_url,
                access_key=self.pw_ctx.access_key,
                window_keys=[self.pw_ctx.window_key],
            )
        except Exception as e:
            try:
                append_log(log_file, f"[sora][drafts] clear_local_cache failed: {e}")
            except Exception:
                pass

        #注释掉下方代码
        '''
        try:
            await self.pw_ctx.fp_client.browser_random_env(
                vendor=self.pw_ctx.vendor,
                base_url=self.pw_ctx.base_url,
                access_key=self.pw_ctx.access_key,
                space_id=self.pw_ctx.space_id,
                window_key=self.pw_ctx.window_key,
            )
        except Exception as e:
            try:
                append_log(log_file, f"[sora][drafts] random_env failed: {e}")
            except Exception:
                pass
        
        # random_env 后：从本地代理 IP 池选择“使用次数最少”的代理并切换（且必须与当前出口 IP 不同）
        try:
            append_log(log_file, "[sora][drafts] post-random_env: selecting least-used proxy from local pool")
        except Exception:
            pass
        '''
        # 说明：窗口切 IP 的逻辑已封装为独立方法，且不在“重启窗口自愈 Cloudflare”流程中触发。
        # 触发时机改为：TaskService 在“连续错误达到阈值/进入冷却”时再切换 IP。

        await self.pw_ctx.ensure_open(
            args=self.browser_open_args,
            force_open=self.browser_force_open,
            headless=self.browser_headless,
            require_page=False,
        )

        ctx2 = getattr(self.pw_ctx, "context", None)
        if ctx2 is None:
            return None

        try:
            pages2 = list(getattr(ctx2, "pages", []) or [])
        except Exception:
            pages2 = []

        open_pages2 = []
        for p2 in pages2:
            try:
                is_closed2 = bool(getattr(p2, "is_closed", lambda: False)())
            except Exception:
                is_closed2 = False
            if not is_closed2:
                open_pages2.append(p2)

        sora_pages2: list[tuple[Any, str]] = []
        drafts_pages2: list[Any] = []
        for p2 in open_pages2:
            try:
                u2 = str(getattr(p2, "url", "") or "").strip()
            except Exception:
                u2 = ""
            if not u2:
                continue
            try:
                host2 = (urlparse(u2).netloc or "").strip().lower()
            except Exception:
                host2 = ""
            if host2 != sora_host:
                continue
            sora_pages2.append((p2, u2))
            if u2.startswith(drafts_url):
                drafts_pages2.append(p2)

        drafts_page2 = drafts_pages2[0] if drafts_pages2 else None
        if drafts_page2 is None:
            try:
                drafts_page2 = await ctx2.new_page()
            except Exception:
                return None
        try:
            await drafts_page2.goto(drafts_url, wait_until="domcontentloaded")
        except Exception:
            pass

        for p2, _u2 in sora_pages2:
            if p2 is drafts_page2:
                continue
            try:
                await p2.close()
            except Exception:
                pass

        try:
            self.pw_ctx.page = drafts_page2
        except Exception:
            pass
        try:
            await drafts_page2.bring_to_front()
        except Exception:
            pass
        try:
            await drafts_page2.evaluate("() => { try { window.focus(); } catch(e) {} }")
        except Exception:
            pass
        return drafts_page2

    async def switch_window_ip_by_proxy_pool(self, *, log_file: Optional[Path] = None) -> Optional[int]:
        """从本地代理池挑选“使用次数最少”的代理并切换（尽量与当前代理/出口 IP 不同）。

        返回：
        - picked_proxy_id：成功切换时返回代理ID
        - None：未切换（无可用代理/接口异常等）
        """
        lf = log_file
        if lf is None:
            lf = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

        async with self._bring_drafts_lock:
            try:
                db = Database()
                space_pk = await db.resolve_space_pk_for_window(space_id=self.pw_ctx.space_id, window_key=self.pw_ctx.window_key)
                if not space_pk:
                    raise RuntimeError("resolve space_pk failed")

                # 读取当前窗口明细（拿当前出口 IP + 保留 windowPlatformList，避免 /browser/mdf 清空账号/网址）
                try:
                    detail = await self.pw_ctx.fp_client.get_browser_detail(
                        vendor=self.pw_ctx.vendor,
                        base_url=self.pw_ctx.base_url,
                        access_key=self.pw_ctx.access_key,
                        space_id=self.pw_ctx.space_id,
                        window_key=self.pw_ctx.window_key,
                    )
                except Exception:
                    detail = {}

                cur_proxy_info = (detail or {}).get("proxyInfo") if isinstance((detail or {}).get("proxyInfo"), dict) else {}
                cur_proxy_id = None
                cur_exit_ip = None
                try:
                    mid = cur_proxy_info.get("moduleId")
                    cur_proxy_id = int(mid) if mid not in (None, "", "-") else None
                except Exception:
                    cur_proxy_id = None
                try:
                    cur_exit_ip = str(cur_proxy_info.get("lastIp") or "").strip() or None
                except Exception:
                    cur_exit_ip = None

                proxies = await db.list_proxies(int(space_pk))
                if not proxies:
                    raise RuntimeError("no local proxies in pool (please sync proxies)")
                counts = await db.count_proxy_bindings(int(space_pk))

                # 按“绑定窗口数”(使用次数) 最少优先选择；且与当前 proxy_id / 当前出口 IP 不同
                candidates: list[tuple[int, int, Optional[str]]] = []
                for p in proxies:
                    pid = int(getattr(p, "proxy_id", 0) or 0)
                    if pid <= 0:
                        continue
                    if cur_proxy_id is not None and pid == cur_proxy_id:
                        continue
                    lip = str(getattr(p, "last_ip", "") or "").strip() or None
                    if cur_exit_ip and lip and lip == cur_exit_ip:
                        continue
                    use_cnt = int(counts.get(pid, 0) or 0)
                    candidates.append((use_cnt, pid, lip))

                if not candidates:
                    raise RuntimeError("no available proxy candidate different from current ip/proxy")

                candidates.sort(key=lambda x: (x[0], x[1]))
                picked_use_cnt, picked_proxy_id, picked_last_ip = candidates[0]

                try:
                    append_log(
                        lf,
                        f"[sora][proxy] picked proxy_id={picked_proxy_id} use_cnt={picked_use_cnt} last_ip={picked_last_ip or '-'} cur_proxy_id={cur_proxy_id} cur_ip={cur_exit_ip or '-'}",
                    )
                except Exception:
                    pass

                proxy_info = {"moduleId": int(picked_proxy_id), "proxyMethod": "choose"}
                mdf_payload: Dict[str, Any] = {"proxyInfo": proxy_info}
                wpl = (detail or {}).get("windowPlatformList")
                if isinstance(wpl, list) and wpl:
                    mdf_payload["windowPlatformList"] = wpl

                await self.pw_ctx.fp_client.browser_mdf(
                    vendor=self.pw_ctx.vendor,
                    base_url=self.pw_ctx.base_url,
                    access_key=self.pw_ctx.access_key,
                    space_id=self.pw_ctx.space_id,
                    window_key=self.pw_ctx.window_key,
                    data=mdf_payload,
                )

                # 回写本地窗口 proxy_id，便于“绑定数/当前代理”统计
                try:
                    await db.update_window_proxy_id(space_pk=int(space_pk), window_key=self.pw_ctx.window_key, proxy_id=int(picked_proxy_id))
                except Exception:
                    pass

                return int(picked_proxy_id)
            except Exception as e:
                try:
                    append_log(lf, f"[sora][proxy] pick/switch proxy skipped: {e}")
                except Exception:
                    pass
                return None

    async def _maybe_click_login_button_if_prompted(self, page) -> bool:
        """若页面出现未登录提示，则尝试点击 'Log in' 按钮/链接。"""
        if page is None:
            return False
        phrase = "Log in to create and edit images and videos"
        has_prompt = False

        # 优先用 locator（更快），不行再用 HTML 兜底
        try:
            if hasattr(page, "get_by_text"):
                loc = page.get_by_text(phrase)
                try:
                    has_prompt = bool(await loc.first.is_visible())
                except Exception:
                    try:
                        has_prompt = (await loc.count()) > 0
                    except Exception:
                        has_prompt = False
            else:
                loc = page.locator(f"text={phrase}")
                has_prompt = (await loc.count()) > 0
        except Exception:
            has_prompt = False

        if not has_prompt:
            try:
                html = await page.content()
                has_prompt = phrase.lower() in (html or "").lower()
            except Exception:
                has_prompt = False

        if not has_prompt:
            return False

        # 点击 Log in（按钮优先，其次链接；最后 CSS 兜底）
        try:
            if hasattr(page, "get_by_role"):
                try:
                    btn = page.get_by_role("button", name="Log in")
                    await btn.first.click(timeout=3000)
                    return True
                except Exception:
                    pass
                try:
                    link = page.get_by_role("link", name="Log in")
                    await link.first.click(timeout=3000)
                    return True
                except Exception:
                    pass
        except Exception:
            pass

        try:
            loc2 = page.locator('button:has-text("Log in"), a:has-text("Log in")')
            await loc2.first.click(timeout=3000)
            return True
        except Exception:
            return False

    async def _bring_sora_drafts_to_front(self, refresh_target=True) -> None:
        """将 Sora drafts 页面置前，并确保同一站点只保留一个 drafts tab。

        需求背景：指纹浏览器可能开了多个标签页；即使 ensure_open 选中了可用 page，也不一定是 drafts。
        这里确保 `https://sora.chatgpt.com/drafts` 在每次 ensure_open 后都会被 bring_to_front，
        且会关闭其它 `https://sora.chatgpt.com/*` 的页面（包括重复的 drafts），只保留一个 drafts。
        """
        drafts_url = "https://sora.chatgpt.com/drafts"
        sora_host = "sora.chatgpt.com"

        async with self._bring_drafts_lock:
            ctx = getattr(self.pw_ctx, "context", None)
            if ctx is None:
                return

            try:
                pages = list(getattr(ctx, "pages", []) or [])
            except Exception:
                pages = []

            open_pages = []
            for p in pages:
                try:
                    is_closed = bool(getattr(p, "is_closed", lambda: False)())
                except Exception:
                    is_closed = False
                if not is_closed:
                    open_pages.append(p)

            # 找出同站点(sora.chatgpt.com)的页面，并挑一个 drafts 作为保留页
            sora_pages: list[tuple[Any, str]] = []
            drafts_pages: list[Any] = []
            for p in open_pages:
                try:
                    u = str(getattr(p, "url", "") or "").strip()
                except Exception:
                    u = ""
                if not u:
                    continue
                try:
                    host = (urlparse(u).netloc or "").strip().lower()
                except Exception:
                    host = ""
                if host != sora_host:
                    continue
                sora_pages.append((p, u))
                if u.startswith(drafts_url):
                    drafts_pages.append(p)

            drafts_page = drafts_pages[0] if drafts_pages else None
            if drafts_page is None:
                # 没有 drafts：新开一个
                try:
                    drafts_page = await ctx.new_page()
                except Exception:
                    return
                try:
                    await drafts_page.goto(drafts_url, wait_until="domcontentloaded")
                except Exception:
                    # 即使 goto 失败，也继续尝试 bring_to_front（网络慢/被拦截时仍尽量保证窗口前置）
                    pass

            # 关闭其它 sora.chatgpt.com 的页面（包括重复 drafts），只保留 drafts_page
            for p, u in sora_pages:
                if p is drafts_page:
                    continue
                try:
                    await p.close()
                except Exception:
                    pass

            # 强制将 drafts_page 置前并作为后续操作的统一 page
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
                except Exception:
                    pass

                try:
                    await drafts_page.evaluate("() => { try { window.focus(); } catch(e) {} }")
                except Exception:
                    pass

                await asyncio.sleep(1.0)

            # 若出现未登录提示，尽量先触发登录（不改变“只保留 drafts 单页”的约束）
            try:
                clicked = await self._maybe_click_login_button_if_prompted(drafts_page)
                if clicked:
                    try:
                        await asyncio.sleep(0.8)
                    except Exception:
                        pass
            except Exception:
                pass

            # Cloudflare interstitial 自愈：先等最多 10 秒自动放行，超时仍是 Cloudflare 才重启一次。
            try:
                maybe_cf = await self._is_cloudflare_page(drafts_page, deep=False)
                if maybe_cf:
                    still_cf_after_wait = await self._wait_cloudflare_auto_pass(drafts_page, max_wait_seconds=10.0)
                    if still_cf_after_wait and await self._is_cloudflare_page(drafts_page, deep=True):
                        new_page = await self._restart_window_and_restore_single_drafts(
                            drafts_url=drafts_url, sora_host=sora_host
                        )
                        if new_page is not None:
                            drafts_page = new_page
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
        # 串行化窗口 open/close：避免并发 ensure_open 与 Cloudflare 自愈重启产生竞态。
        async with self._bring_drafts_lock:
            # 仅要求“指纹浏览器窗口已打开且 CDP 已连接”，不要求确认/创建 page（page 由 _bring_sora_drafts_to_front 负责）。
            await self.pw_ctx.ensure_open(args=args, force_open=force_open, headless=headless, require_page=False)

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
        await self._bring_sora_drafts_to_front(refresh_target=False)
        # 余额查询属于“仍在使用窗口”的行为：不要触发倒计时关窗
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
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
        await self._bring_sora_drafts_to_front(refresh_target=False)
        # 查询邀请码属于“仍在使用窗口”的行为：不要触发倒计时关窗
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
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

    async def api_characters_upload_video(
        self,
        *,
        target_url: str,
        video_path: Path,
        timestamps: str = "0,3",
    ) -> str:
        """创建角色：POST /characters/upload，返回 cameo_id。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front(refresh_target=False)
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            if self.pw_ctx.page is None:
                raise RuntimeError("page 未初始化")
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
            # 不新开页面：直接复用当前已登录的 drafts 页面，降低 429/机器人风控概率
            return await _sora_api_upload_character_video_pw(
                self.pw_ctx.page,
                target_url=target_url,
                bearer_token=str(token),
                video_path=Path(video_path),
                timestamps=str(timestamps or "0,3"),
                log_file=log_file,
            )

    async def api_cameo_status(self, *, target_url: str, cameo_id: str) -> Dict[str, Any]:
        """GET /project_y/cameos/in_progress/{cameo_id}。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front(refresh_target=False)
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            if self.pw_ctx.page is None:
                raise RuntimeError("page 未初始化")
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
            headers: Dict[str, str] = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US"}
            try:
                if self.oai_device_id:
                    headers["OAI-Device-Id"] = str(self.oai_device_id)
            except Exception:
                pass

            # 优先 /backend 前缀（与 sora2api base_url 一致），失败再降级
            urls = [
                _sora_backend_url_from_target(target_url, f"/backend/project_y/cameos/in_progress/{str(cameo_id)}"),
                _sora_backend_url_from_target(target_url, f"/project_y/cameos/in_progress/{str(cameo_id)}"),
            ]
            last_err: Optional[str] = None
            for url in urls:
                try:
                    tx = await page_fetch_json(self.pw_ctx.page, url=url, method="GET", headers=headers, json_data=None, log_file=log_file)
                    obj = tx.get("_json")
                    data = obj if isinstance(obj, dict) else {}
                    # 若接口不存在，有时会返回 HTML 字符串（_json 解析失败 -> None/{}），这里用 status 兜底
                    status = int(tx.get("status") or 0) if tx.get("status") is not None else 0
                    if status == 404 and not data:
                        last_err = f"url={url!r} status=404"
                        continue
                    return data
                except Exception as e:
                    last_err = f"url={url!r} err={e}"
                    continue
            raise RuntimeError(f"获取 cameo 状态失败：{last_err}")

    async def api_character_upload_image(self, *, target_url: str, image_path: Path) -> str:
        """POST /project_y/file/upload，返回 asset_pointer。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front(refresh_target=False)
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            if self.pw_ctx.page is None:
                raise RuntimeError("page 未初始化")
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
            # 不新开页面：直接复用当前已登录的 drafts 页面，降低 429/机器人风控概率
            return await _sora_api_upload_character_image_pw(
                self.pw_ctx.page,
                target_url=target_url,
                bearer_token=str(token),
                image_path=Path(image_path),
                log_file=log_file,
            )

    async def api_character_finalize(
        self,
        *,
        target_url: str,
        cameo_id: str,
        username: str,
        display_name: str,
        profile_asset_pointer: str,
    ) -> str:
        """POST /characters/finalize，返回 character_id。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front(refresh_target=False)
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            if self.pw_ctx.page is None:
                raise RuntimeError("page 未初始化")
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US", "Content-Type": "application/json"}
            payload = {
                "cameo_id": str(cameo_id),
                "username": str(username),
                "display_name": str(display_name),
                "profile_asset_pointer": str(profile_asset_pointer),
                "instruction_set": None,
                "safety_instruction_set": None,
            }

            urls = [
                _sora_backend_url_from_target(target_url, "/backend/characters/finalize"),
                _sora_backend_url_from_target(target_url, "/characters/finalize"),
            ]
            last_err: Optional[str] = None
            for url in urls:
                try:
                    tx = await page_fetch_json(self.pw_ctx.page, url=url, method="POST", headers=headers, json_data=payload, log_file=log_file)
                    obj = tx.get("_json")
                    data = obj if isinstance(obj, dict) else {}
                    status = int(tx.get("status") or 0) if tx.get("status") is not None else 0
                    if status == 404 and not data:
                        last_err = f"url={url!r} status=404"
                        continue
                    character_id = None
                    try:
                        character_id = (data or {}).get("character", {}).get("character_id")
                    except Exception:
                        character_id = None
                    cid = str(character_id or "").strip()
                    if cid:
                        return cid
                    last_err = f"url={url!r} missing character_id body={safe_trim(json.dumps(data, ensure_ascii=False), 600)}"
                except Exception as e:
                    last_err = f"url={url!r} err={e}"
                    continue
            raise RuntimeError(f"finalize 失败：{last_err}")

    async def api_character_set_public(self, *, target_url: str, cameo_id: str) -> bool:
        """POST /project_y/cameos/by_id/{cameo_id}/update_v2（visibility=public）。"""
        self.last_used_at = time.time()
        await self.ensure_open(args=self.browser_open_args, force_open=self.browser_force_open, headless=self.browser_headless)
        await self._bring_sora_drafts_to_front(refresh_target=False)
        self._cancel_idle_close()
        async with self._bring_drafts_lock:
            if self.pw_ctx.page is None:
                raise RuntimeError("page 未初始化")
            log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
            token = self._get_bearer_token_required()
            headers = {"Authorization": f"Bearer {token}", "OAI-Language": "en-US", "Content-Type": "application/json"}
            payload = {"visibility": "public"}

            urls = [
                _sora_backend_url_from_target(target_url, f"/backend/project_y/cameos/by_id/{str(cameo_id)}/update_v2"),
                _sora_backend_url_from_target(target_url, f"/project_y/cameos/by_id/{str(cameo_id)}/update_v2"),
            ]
            last_err: Optional[str] = None
            for url in urls:
                try:
                    tx = await page_fetch_tx(self.pw_ctx.page, url=url, method="POST", headers=headers, json_data=payload, log_file=log_file)
                    status = int(tx.get("status") or 0) if tx.get("status") is not None else 0
                    body = str(tx.get("response_body") or "")
                    if status in (200, 201):
                        return True
                    if status == 404 and body and "<!DOCTYPE html" in body:
                        last_err = f"url={url!r} status=404"
                        continue
                    last_err = f"url={url!r} status={status} body={safe_trim(body, 300)}"
                except Exception as e:
                    last_err = f"url={url!r} err={e}"
                    continue
            raise RuntimeError(f"设置角色公开失败：{last_err}")

    async def _ensure_bearer_token(self, *, target_url: str, log_file: Path) -> str:
        if self.pw_ctx.page is None:
            raise RuntimeError("page 未初始化")
        if self.access_token:
            self.bearer_token = self.access_token
            return str(self.access_token)
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
                await self._bring_sora_drafts_to_front(refresh_target=False)

                log_file = Path(monitor_log_path) if monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")

                # 先生成 sentinel_token，并填充/更新 oai_device_id
                async with self._bring_drafts_lock:
                    if self.pw_ctx.page is None:
                        raise RuntimeError("无法获取可用 page（context/pages 不可用或 drafts 打开失败）")
                    self.sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(
                        self.pw_ctx.page, device_id=self.oai_device_id, log_file=log_file
                    )
                    if not self.sentinel_token:
                        raise RuntimeError("未能生成 SentinelToken，触发了429")
                    try:
                        sentinel_data = json.loads(str(self.sentinel_token))
                        if isinstance(sentinel_data, dict):
                            did = str(sentinel_data.get("id") or "").strip() or None
                            if did:
                                self.oai_device_id = did
                    except Exception:
                        pass
                    if not self.oai_device_id:
                        self.oai_device_id = str(uuid4())

                # bearer_token 优先复用会话缓存；缺失则 bring_to_front + refresh 一次后再抓取
                async with self._bring_drafts_lock:
                    if self.pw_ctx.page is None:
                        raise RuntimeError("page 未初始化")
                    await _sora_ui_fill_prompt_textarea(self.pw_ctx.page, prompt=prompt, log_file=log_file)

                if not self.bearer_token:
                    await self._bring_sora_drafts_to_front(refresh_target=False)
                    async with self._bring_drafts_lock:
                        if self.pw_ctx.page is None:
                            raise RuntimeError("page 未初始化")
                        info = await _sora_extract_bearer_from_any_post_pw(self.pw_ctx.page, timeout_seconds=20.0, log_file=log_file)
                        tok = str(info.get("token") or "").strip()
                        if not tok:
                            raise RuntimeError("未抓到 Bearer token（请确认窗口已登录 Sora）")
                        self.bearer_token = tok
                        try:
                            ua = str(info.get("user_agent") or "").strip() or None
                            if ua:
                                self.user_agent = ua
                        except Exception:
                            pass
                        if not self.user_agent:
                            try:
                                self.user_agent = await _pw_get_user_agent(self.pw_ctx.page)
                            except Exception:
                                pass

                async with self._bring_drafts_lock:
                    task_id, create_tx, auth_state = await _sora_create_task_pw(
                        page=self.pw_ctx.page,
                        prompt=prompt,
                        target_url=target_url,
                        monitor_log_path=monitor_log_path,
                        first_image_url=first_image_url,
                        orientation=orientation,
                        n_frames=n_frames,
                        bearer_token=str(self.bearer_token or "") or None,
                        sentinel_token=str(self.sentinel_token or "") or None,
                        user_agent=str(self.user_agent or "") or None,
                        oai_device_id=str(self.oai_device_id or "") or None,
                    )
                    try:
                        self.bearer_token = auth_state.get("bearer_token") or self.bearer_token
                        self.sentinel_token = auth_state.get("sentinel_token") or self.sentinel_token
                        self.user_agent = auth_state.get("user_agent") or self.user_agent
                        self.oai_device_id = auth_state.get("oai_device_id") or self.oai_device_id
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
                    # monitor_loop 内也要保证有可用 page（ensure_open 已不再强制挑 page）
                    page_closed = False
                    if self.pw_ctx.page is not None:
                        try:
                            page_closed = bool(getattr(self.pw_ctx.page, "is_closed", lambda: False)())
                        except Exception:
                            page_closed = False
                    if self.pw_ctx.page is None or page_closed:
                        await self._bring_sora_drafts_to_front()
                    if self.pw_ctx.page is None:
                        raise RuntimeError("无法获取可用 page（context/pages 不可用或 drafts 打开失败）")
                except Exception as e:
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError(f"浏览器/driver 不可用：{e}"))
                        self.watchers.pop(tid, None)
                    return

                log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
                if not (self.access_token or self.bearer_token):
                    for tid, w in list(self.watchers.items()):
                        if not w.future.done():
                            w.future.set_exception(RuntimeError("缺少 access_token，无法轮询 pending（请在管理台为该窗口转换并保存 access_token）"))
                        self.watchers.pop(tid, None)
                    return

                token = self._get_bearer_token_required()
                try:
                    pending_url = _sora_backend_url_from_target(
                        getattr(self.pw_ctx.page, "url", "") or "https://sora.chatgpt.com", "/backend/nf/pending/v2"
                    )
                except Exception:
                    pending_url = "https://sora.chatgpt.com/backend/nf/pending/v2"

                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {token}",
                    "OAI-Language": "en-US",
                    "OAI-Device-Id": str(self.oai_device_id or ""),
                }

                tx: Optional[Dict[str, Any]] = None
                async with self._bring_drafts_lock:
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
                    async with self._bring_drafts_lock:
                        try:
                            drafts = await _sora_api_get_video_drafts_pw(
                                self.pw_ctx.page,
                                target_url=str(getattr(self.pw_ctx.page, "url", "") or "https://sora.chatgpt.com/drafts"),
                                bearer_token=str(token),
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
        await asyncio.sleep(5)
        await self._bring_sora_drafts_to_front()
        
        log_file = Path(self.monitor_log_path) if self.monitor_log_path else (Path(__file__).resolve().parents[2] / "logs.txt")
        token = self._get_bearer_token_required()


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
            items = []
            async with self._bring_drafts_lock:
                try:
                    drafts = await _sora_api_get_video_drafts_pw(
                        self.pw_ctx.page,
                        target_url=target_url,
                        bearer_token=str(token),
                        limit=int(drafts_limit),
                        log_file=log_file,
                    )
                    items = drafts.get("items", []) if isinstance(drafts, dict) else []
                    if not isinstance(items, list):
                        items = []
                except Exception as e:
                    items = []
                    try:
                        append_log(log_file, f"[sora][drafts] fetch failed: {e}")
                    except Exception:
                        pass

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
            await self._bring_sora_drafts_to_front(refresh_target=False)

        if not draft_item:
            raise RuntimeError(
                f"草稿箱未找到任务对应视频（已轮询 {drafts_wait_seconds:.0f}s）：task_id={task_id} "
                f"sample_task_ids={safe_trim(','.join(last_items_sample), 260)}"
            )

        generation_id = str(draft_item.get("id") or "").strip()
        if not generation_id:
            raise RuntimeError("草稿箱记录缺少 generation_id（draft_item.id）")

        async with self._bring_drafts_lock:
            self.sentinel_token = await _sora_generate_sentinel_token_in_fp_context_pw(self.pw_ctx.page, device_id=self.oai_device_id, log_file=log_file)
            if not self.sentinel_token:
                raise RuntimeError("缺少 sentinel_token，无法发布草稿")
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

        # 优先回传可下载的视频直链（downloadable_url）；取不到再回退到帖子链接
        downloadable_url = ""
        try:
            post_obj = (post_resp or {}).get("post") or {}
            attachments = post_obj.get("attachments")
            if isinstance(attachments, list):
                for att in attachments:
                    if not isinstance(att, dict):
                        continue
                    du = str(att.get("downloadable_url") or "").strip()
                    if du:
                        downloadable_url = du
                        break
        except Exception:
            downloadable_url = ""
            raise RuntimeError(f"发布草稿失败,生成视频包含违规内容")

        share_url = downloadable_url or f"https://sora.chatgpt.com/p/{post_id}"
        # https://oscdn2.dyysy.com/MP4/{post_id}.mp4
        watermark_free_url = ""
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
    access_token: Optional[str] = None,
    access_expires: Optional[str] = None,
) -> Dict[str, Any]:
    """Sora 生视频：复用同一指纹浏览器窗口 + Playwright(CDP) 轻量连接，拆分“创建任务”和“进度轮询”。"""
    payload = payload or {}
    video_url = str(payload.get("video_url") or payload.get("videoUrl") or payload.get("videoURL") or "").strip() or None
    prompt = str(payload.get("prompt") or "").strip()
    if not video_url and not prompt:
        raise ValueError("payload.prompt 不能为空（或提供 payload.video_url 用于创建角色）")

    first_image_url = str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip() or None
    ratio = str(payload.get("size_ratio") or payload.get("aspect_ratio") or payload.get("ratio") or payload.get("尺寸") or "").strip() or None
    orientation = _pick_orientation_from_ratio(ratio) or str(payload.get("orientation") or "").strip() or None
    duration_v = payload.get("n_frames") or payload.get("duration_frames") or payload.get("duration") or payload.get("时长")
    n_frames = _pick_n_frames(duration_v)

    target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
    monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)

    max_wait_seconds = float(payload.get("sora_pending_max_wait_seconds") or max(30.0, min(float(timeout_seconds), 60.0 * 10)))
    poll_interval_seconds = float(payload.get("sora_pending_poll_interval_seconds") or 5.0)
    sniff_timeout_seconds = float(payload.get("sora_pending_sniff_timeout_seconds") or 4.0)
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)

    sess = get_or_create_sora_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    # 由管理台预先转换并写入 DB 的 access_token：直接写入会话，避免抓包/ensure
    try:
        if access_token:
            sess.set_access_token(access_token, access_expires)
    except Exception:
        pass

    # 新分支：若提供 video_url，则先创建角色（Character Creation Only）
    if video_url:
        target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
        monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)
        max_wait_seconds = float(payload.get("character_pending_max_wait_seconds") or payload.get("sora_pending_max_wait_seconds") or max(60.0, min(float(timeout_seconds), 60.0 * 15)))
        poll_interval_seconds = float(payload.get("character_pending_poll_interval_seconds") or 5.0)
        idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)
        sess.monitor_log_path = monitor_log_path
        sess.idle_close_seconds = max(0.0, float(idle_close_seconds))

        tmp_video: Optional[Path] = None
        tmp_img: Optional[Path] = None
        cameo_status: Dict[str, Any] = {}
        try:
            await progress_cb(0, {"stage": "character_download_video", "video_url": video_url})
            # 下载到本地临时文件（用于浏览器上下文上传）
            tmp_video, _hdrs = _download_to_tempfile_local(
                video_url,
                suffix=".mp4",
                timeout_seconds=float(payload.get("video_download_timeout_seconds") or 120.0),
                user_agent=sess.user_agent,
                max_bytes=10 * 1024 * 1024,  # 10MB
            )

            # 校验大小（<=10MB）与时长（<=5秒）
            try:
                sz = int(tmp_video.stat().st_size)
            except Exception:
                sz = -1
            if sz < 0:
                raise NonPenalizedTaskError("无法读取视频文件大小", status_code=400)
            if sz > 10 * 1024 * 1024:
                raise NonPenalizedTaskError(f"视频文件过大：{sz} bytes（限制 10MB）", status_code=400)

            try:
                dur = _mp4_duration_seconds(tmp_video)
            except Exception as e:
                raise NonPenalizedTaskError(f"无法解析视频时长（仅支持 MP4）：{e}", status_code=400)
            if float(dur) > 5.0 + 1e-6:
                raise NonPenalizedTaskError(f"视频时长过长：{dur:.3f}s（限制 ≤5s）", status_code=400)

            await progress_cb(5, {"stage": "character_upload_video"})

            cameo_id = await sess.api_characters_upload_video(
                target_url=target_url,
                video_path=tmp_video,
                timestamps=str(payload.get("character_video_timestamps") or "0,3"),
            )
            await progress_cb(10, {"stage": "character_processing", "cameo_id": cameo_id})

            # 轮询 cameo 状态
            start = time.time()
            consecutive_errors = 0
            last_status = None
            while True:
                if time.time() - start > max(1.0, float(max_wait_seconds)):
                    raise RuntimeError(f"角色处理超时：cameo_id={cameo_id} waited={int(time.time() - start)}s")
                try:
                    await asyncio.sleep(max(0.8, float(poll_interval_seconds)))
                except Exception:
                    pass

                try:
                    cameo_status = await sess.api_cameo_status(target_url=target_url, cameo_id=cameo_id)
                    consecutive_errors = 0
                except Exception as e:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        raise RuntimeError(f"轮询 cameo 状态失败次数过多：{e}")
                    continue

                cur = cameo_status.get("status")
                msg = str(cameo_status.get("status_message") or "")
                if cur != last_status:
                    last_status = cur
                    try:
                        pct = 10 + int(min(55.0, (time.time() - start) / max(1.0, float(max_wait_seconds)) * 55.0))
                    except Exception:
                        pct = 20
                    await progress_cb(int(max(10, min(65, pct))), {"stage": "character_processing", "cameo_id": cameo_id, "status": cur, "status_message": msg})

                if str(cur) == "failed":
                    raise RuntimeError(f"角色创建失败：{msg or 'failed'}")
                if msg == "Completed" or str(cur) == "finalized":
                    break

            username_hint = str(cameo_status.get("username_hint") or "character")
            display_name = str(cameo_status.get("display_name_hint") or "Character")
            base_username = username_hint.split(".")[-1] if "." in username_hint else username_hint
            base_username = str(base_username or "").strip().lstrip("@").strip() or "character"
            # Sora 对 username 约束更严格：尽量只保留 [a-z0-9_]
            safe_base = "".join([ch for ch in base_username.lower() if (ch.isalnum() or ch == "_")])
            if not safe_base:
                safe_base = "character"
            username = f"{safe_base}{random.randint(100, 999)}"
            await progress_cb(70, {"stage": "character_identified", "cameo_id": cameo_id, "display_name": display_name, "username": username})

            profile_asset_url = str(cameo_status.get("profile_asset_url") or "").strip() or None
            if not profile_asset_url:
                raise RuntimeError("cameo_status 缺少 profile_asset_url，无法完成角色创建")

            await progress_cb(75, {"stage": "character_download_avatar"})
            tmp_img, _hdrs2 = _download_to_tempfile_local(
                profile_asset_url,
                suffix=".webp",
                timeout_seconds=float(payload.get("avatar_download_timeout_seconds") or 60.0),
                user_agent=sess.user_agent,
            )

            await progress_cb(80, {"stage": "character_upload_avatar"})
            asset_pointer = await sess.api_character_upload_image(target_url=target_url, image_path=tmp_img)

            await progress_cb(90, {"stage": "character_finalize"})
            character_id = await sess.api_character_finalize(
                target_url=target_url,
                cameo_id=cameo_id,
                username=username,
                display_name=display_name,
                profile_asset_pointer=asset_pointer,
            )

            await progress_cb(95, {"stage": "character_set_public"})
            await sess.api_character_set_public(target_url=target_url, cameo_id=cameo_id)

            try:
                await sess._bring_sora_drafts_to_front(refresh_target=False)
            except Exception:
                pass

            await progress_cb(100, {"stage": "done", "cameo_id": cameo_id, "character_id": character_id})
            return {
                "type": "character",
                "message": "Sora角色创建完成",
                "cameo_id": cameo_id,
                "character_id": character_id,
                "display_name": display_name,
                "username": username,
                "profile_asset_url": profile_asset_url,
                "raw_cameo_status": cameo_status,
            }
        finally:
            for p in [tmp_video, tmp_img]:
                if p is None:
                    continue
                try:
                    p.unlink(missing_ok=True)  # type: ignore[call-arg]
                except Exception:
                    try:
                        os.unlink(str(p))
                    except Exception:
                        pass

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
        drafts_limit=int(payload.get("sora_drafts_limit") or 15),
    )

    await sess._bring_sora_drafts_to_front();

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

