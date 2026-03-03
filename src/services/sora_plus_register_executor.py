"""Sora Plus 注册执行器（窗口账号自动填充流程）。

流程（当前实现）：
1) 根据 window_pk 查找窗口绑定的平台网址/账号/密码
2) 打开对应指纹浏览器窗口，关闭其他所有标签页，仅保留一个页面并 bring to front
3) 访问平台网址（Google 登录页），判断是否已登录
   - 是 → 跳过登录步骤
   - 否 → 填邮箱 → Next → 填密码 → Next → 2FA → Next
4) Google 登录成功后，跳转 https://chatgpt.com/
5) 点击页面中的 "Sign in" 按钮
6) 在弹出的登录选项中点击 "Continue with Google"
7) 若出现 2FA 验证页面，重新计算 TOTP code 并输入
8) 完成 ChatGPT 的 Google 账号登录
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import struct
import time
from typing import Any, Dict, Optional

from ..core.database import Database
from .playwright_broswer_context import get_or_create_ctx, pick_working_page_from_context
from .task_executor_types import ProgressCB


def _s(v: Any) -> str:
    return str(v or "").strip()


GOOGLE_PASSWORD_PAGE_URL = "https://myaccount.google.com/signinoptions/password?continue=https%3A%2F%2Fmyaccount.google.com%2Fsecurity"
NEW_PASSWORD_VALUE = "Laky373155210"


async def _click_next_button(page: Any, *, timeout_ms: int) -> str:
    """点击 Next 按钮（兼容不同站点/页面结构），返回命中的 selector。"""
    selectors = [
        "#identifierNext",
        "button#identifierNext",
        "#passwordNext",
        "button#passwordNext",
        'button:has-text("Next")',
        'div[role="button"]:has-text("Next")',
        'input[type="submit"][value="Next"]',
    ]
    per_try_timeout = int(max(1000, min(5000, timeout_ms // 4)))
    last_err: Optional[Exception] = None
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            await loc.wait_for(state="visible", timeout=per_try_timeout)
            await loc.click(timeout=per_try_timeout)
            return sel
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"未找到可点击的 Next 按钮（已尝试 {selectors}），last_err={last_err}")


async def _click_save_or_next_button(page: Any, *, timeout_ms: int) -> str:
    """在密码修改页点击 Change password / Save / Next。"""
    selectors = [
        'button:has-text("Change password")',
        'div[role="button"]:has-text("Change password")',
        'input[type="submit"][value="Change password"]',
        "#passwordNext",
        "button#passwordNext",
        'button:has-text("Save")',
        'div[role="button"]:has-text("Save")',
        'button:has-text("Next")',
        'div[role="button"]:has-text("Next")',
        'button:has-text("更改密码")',
        'div[role="button"]:has-text("更改密码")',
        'button:has-text("保存")',
        'div[role="button"]:has-text("保存")',
        'button:has-text("下一步")',
        'div[role="button"]:has-text("下一步")',
        'input[type="submit"][value="Save"]',
        'input[type="submit"][value="Next"]',
    ]
    per_try_timeout = int(max(1000, min(5000, timeout_ms // 4)))
    last_err: Optional[Exception] = None
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            await loc.wait_for(state="visible", timeout=per_try_timeout)
            await loc.click(timeout=per_try_timeout)
            return sel
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"未找到可点击的 Save/Next 按钮（已尝试 {selectors}），last_err={last_err}")


def _generate_totp_code(secret: str, *, digits: int = 6, period_seconds: int = 30) -> str:
    """基于 Google 2FA Secret 生成当前 TOTP code。"""
    s = str(secret or "").strip().replace(" ", "").replace("-", "").upper()
    if not s:
        raise RuntimeError("EFA(2FA Secret) 为空，无法生成验证码")
    pad_len = (-len(s)) % 8
    s_padded = s + ("=" * pad_len)
    try:
        key = base64.b32decode(s_padded, casefold=True)
    except Exception as e:
        raise RuntimeError(f"EFA(2FA Secret) 非法，无法 Base32 解码：{e}") from e

    counter = int(time.time() // max(1, int(period_seconds)))
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code_int = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    code = code_int % (10**int(digits))
    return str(code).zfill(int(digits))


async def _generate_fresh_totp_code(secret: str, previous_code: str) -> str:
    """尽量生成与 previous_code 不同的新 code（最多等待约 35 秒）。"""
    old = str(previous_code or "").strip()
    cur = _generate_totp_code(secret)
    if cur != old:
        return cur
    for _ in range(35):
        await asyncio.sleep(1)
        cur = _generate_totp_code(secret)
        if cur != old:
            return cur
    return cur


async def _resolve_window_platform_credentials(db: Database, *, window_pk: int) -> Dict[str, Optional[str]]:
    """解析窗口绑定的平台凭据（url / username / password）。

    优先级：
    1) windows.platform_account_id 关联 platform_accounts.account_id
    2) 若未绑定 account_id，则按 windows.platform_account + platform_url 在本地账号库匹配
    """
    win = await db.get_window(int(window_pk))
    if not win:
        raise RuntimeError(f"窗口不存在：window_pk={window_pk}")

    platform_url = _s(getattr(win, "platform_url", None)) or None
    platform_username = _s(getattr(win, "platform_account", None)) or None
    platform_password: Optional[str] = None
    platform_efa: Optional[str] = None

    space_pk = int(getattr(win, "space_pk", 0) or 0)
    if space_pk <= 0:
        raise RuntimeError(f"窗口缺少 space_pk：window_pk={window_pk}")

    account_id = int(getattr(win, "platform_account_id", 0) or 0)
    if account_id > 0:
        acc = await db.get_platform_account(space_pk=space_pk, account_id=account_id)
        if acc:
            platform_url = _s(getattr(acc, "platform_url", None)) or platform_url
            platform_username = _s(getattr(acc, "platform_username", None)) or platform_username
            platform_password = _s(getattr(acc, "platform_password", None)) or None
            platform_efa = _s(getattr(acc, "platform_efa", None)) or None
    else:
        cands = await db.list_platform_accounts(space_pk)
        target_user = (platform_username or "").strip().lower()
        target_url = (platform_url or "").strip().lower()
        for acc in cands:
            u = _s(getattr(acc, "platform_username", None)).lower()
            u_ok = bool(target_user) and u == target_user
            if not u_ok:
                continue
            a_url = _s(getattr(acc, "platform_url", None)).lower()
            if target_url and a_url and a_url != target_url:
                continue
            platform_url = _s(getattr(acc, "platform_url", None)) or platform_url
            platform_password = _s(getattr(acc, "platform_password", None)) or None
            platform_efa = _s(getattr(acc, "platform_efa", None)) or None
            break

    if not platform_url:
        raise RuntimeError(f"窗口未绑定平台网址：window_pk={window_pk}")
    if not platform_username:
        raise RuntimeError(f"窗口未绑定平台账号(邮箱)：window_pk={window_pk}")
    if not platform_password:
        raise RuntimeError(f"窗口绑定账号缺少密码：window_pk={window_pk}")
    if not platform_efa:
        raise RuntimeError(f"窗口绑定账号缺少 EFA(2FA Secret)：window_pk={window_pk}")

    return {
        "platform_url": platform_url,
        "platform_username": platform_username,
        "platform_password": platform_password,
        "platform_efa": platform_efa,
    }


async def _close_other_pages(context: Any, keep_page: Any) -> int:
    """关闭 context 中除 keep_page 以外的所有页面，返回关闭数量。"""
    closed = 0
    try:
        pages = list(getattr(context, "pages", []) or [])
    except Exception:
        pages = []
    for p in pages:
        if p is keep_page:
            continue
        try:
            is_closed = bool(getattr(p, "is_closed", lambda: False)())
        except Exception:
            is_closed = True
        if is_closed:
            continue
        try:
            await p.close()
            closed += 1
        except Exception:
            pass
    return closed


def _is_already_logged_in(current_url: str) -> bool:
    """判断当前页面是否已跳转到 Google 账户页（说明已处于登录态）。"""
    u = str(current_url or "").strip().lower()
    return u.startswith("https://myaccount.google.com")


async def _wait_for_login_redirect(page: Any, *, timeout_ms: int) -> None:
    """等待 Google 登录完成并跳转离开登录页（accounts.google.com）。

    点击 2FA Next 后 Google 需要几秒处理，如果立刻 goto 新页面会打断 session 写入，
    导致到达目标页后仍未登录。这里轮询等待 URL 不再是登录页即可。
    """
    login_prefixes = (
        "https://accounts.google.com/v3/signin",
        "https://accounts.google.com/signin",
        "https://accounts.google.com/servicelog",
        "https://accounts.google.com/checkmyactivity",
    )
    deadline = time.time() + max(5, timeout_ms / 1000)
    while time.time() < deadline:
        try:
            cur = str(page.url or "").strip().lower()
        except Exception:
            cur = ""
        if cur and not cur.startswith("https://accounts.google.com"):
            return
        if cur and not any(cur.startswith(p) for p in login_prefixes):
            return
        await asyncio.sleep(1)
    # 超时也不报错——可能是慢网络，后续 goto 如果失败会自然抛异常


async def _do_login_flow(
    page: Any,
    *,
    platform_username: str,
    platform_password: str,
    platform_efa: str,
    timeout_ms: int,
    progress_cb: ProgressCB,
) -> str:
    """完整的登录流程：邮箱 -> Next -> 密码 -> Next -> 2FA -> Next。

    返回本次使用的 totp_code（供后续"换新 code"逻辑参考）。
    """
    email_input = page.locator('input[type="email"]').first
    await email_input.wait_for(state="visible", timeout=timeout_ms)
    await email_input.fill(platform_username, timeout=timeout_ms)
    await progress_cb(20, {"stage": "email_filled"})

    clicked_email_next = await _click_next_button(page, timeout_ms=timeout_ms)
    await progress_cb(30, {"stage": "email_next_clicked", "selector": clicked_email_next})

    password_input = page.locator('input[type="password"]').first
    await password_input.wait_for(state="visible", timeout=timeout_ms)
    await password_input.fill(platform_password, timeout=timeout_ms)
    await progress_cb(40, {"stage": "password_filled"})

    clicked_password_next = await _click_next_button(page, timeout_ms=timeout_ms)
    await progress_cb(45, {"stage": "password_next_clicked", "selector": clicked_password_next})

    tel_input = page.locator('input[type="tel"]').first
    await tel_input.wait_for(state="visible", timeout=timeout_ms)
    first_totp_code = _generate_totp_code(platform_efa)
    await tel_input.fill(first_totp_code, timeout=timeout_ms)
    await progress_cb(50, {"stage": "totp_filled"})

    clicked_totp_next = await _click_next_button(page, timeout_ms=timeout_ms)
    await progress_cb(52, {"stage": "totp_next_clicked", "selector": clicked_totp_next})

    await _wait_for_login_redirect(page, timeout_ms=timeout_ms)
    await progress_cb(55, {"stage": "login_redirect_done", "url": str(page.url or "")})

    return first_totp_code


CHATGPT_URL = "https://chatgpt.com/"


async def _do_chatgpt_google_login(
    page: Any,
    *,
    platform_efa: str,
    previous_totp_code: str,
    timeout_ms: int,
    progress_cb: ProgressCB,
) -> None:
    """跳转到 ChatGPT 并通过 "Continue with Google" 登录。

    流程：
    1) goto chatgpt.com
    2) 点击 "Sign in" (或 "Log in") 按钮
    3) 点击 "Continue with Google" 按钮
    4) 如果弹出 2FA 验证（input[type="tel"]），计算新的 TOTP code 并填入
    """
    await page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    await asyncio.sleep(2)
    await progress_cb(60, {"stage": "chatgpt_page_loaded", "url": str(page.url or "")})

    sign_in_selectors = [
        'button:has-text("Sign in")',
        'a:has-text("Sign in")',
        'button:has-text("Log in")',
        'a:has-text("Log in")',
        '[data-testid="login-button"]',
        'button:has-text("登录")',
        'a:has-text("登录")',
    ]
    per_try = int(max(2000, min(8000, timeout_ms // 4)))
    clicked_sign_in = False
    for sel in sign_in_selectors:
        loc = page.locator(sel).first
        try:
            await loc.wait_for(state="visible", timeout=per_try)
            await loc.click(timeout=per_try)
            clicked_sign_in = True
            break
        except Exception:
            continue
    if not clicked_sign_in:
        raise RuntimeError(f"ChatGPT 页面未找到 Sign in 按钮（已尝试 {sign_in_selectors}）")
    await asyncio.sleep(2)
    await progress_cb(65, {"stage": "chatgpt_sign_in_clicked"})

    google_selectors = [
        'button:has-text("Continue with Google")',
        '[data-provider="google"]',
        'button:has-text("继续使用 Google")',
        'button:has-text("使用 Google 继续")',
        'form[action*="google"] button',
        'button[data-action-button-secondary="true"]:has-text("Google")',
        'button:has-text("Google")',
    ]
    clicked_google = False
    for sel in google_selectors:
        loc = page.locator(sel).first
        try:
            await loc.wait_for(state="visible", timeout=per_try)
            await loc.click(timeout=per_try)
            clicked_google = True
            break
        except Exception:
            continue
    if not clicked_google:
        raise RuntimeError(f"未找到 Continue with Google 按钮（已尝试 {google_selectors}）")
    await asyncio.sleep(3)
    await progress_cb(70, {"stage": "chatgpt_continue_with_google_clicked"})

    # Google OAuth 可能要求再次 2FA 验证
    try:
        tel_input = page.locator('input[type="tel"]').first
        await tel_input.wait_for(state="visible", timeout=min(10_000, timeout_ms))
        totp_code = await _generate_fresh_totp_code(platform_efa, previous_totp_code)
        await tel_input.fill(totp_code, timeout=timeout_ms)
        await progress_cb(80, {"stage": "chatgpt_2fa_filled"})
        await _click_next_button(page, timeout_ms=timeout_ms)
        await progress_cb(85, {"stage": "chatgpt_2fa_next_clicked"})
    except Exception:
        await progress_cb(80, {"stage": "chatgpt_no_2fa_needed"})

    # 等待登录完成，URL 应该回到 chatgpt.com
    deadline = time.time() + max(15, timeout_ms / 1000)
    while time.time() < deadline:
        cur = str(page.url or "").strip().lower()
        if "chatgpt.com" in cur and "auth" not in cur and "accounts.google.com" not in cur:
            break
        await asyncio.sleep(1)

    await progress_cb(95, {"stage": "chatgpt_login_done", "url": str(page.url or "")})


async def _navigate_to_password_page(page: Any, *, timeout_ms: int, progress_cb: ProgressCB) -> None:
    """尝试跳转到 Google 密码修改页；如果 goto 后未到达，则在页面中查找并点击相关入口。"""
    try:
        await page.goto(GOOGLE_PASSWORD_PAGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    await asyncio.sleep(2)

    cur = str(page.url or "").strip().lower()
    if "signinoptions/password" in cur:
        return

    await progress_cb(62, {"stage": "goto_password_page_fallback", "current_url": cur})

    btn_selectors = [
        'a:has-text("Google password")',
        'button:has-text("Google password")',
        'div[role="link"]:has-text("Google password")',
        'a:has-text("Password")',
        'button:has-text("Password")',
        'a:has-text("Google 密码")',
        'a:has-text("密码")',
    ]
    per_try = int(max(1000, min(5000, timeout_ms // 4)))
    for sel in btn_selectors:
        loc = page.locator(sel).first
        try:
            await loc.wait_for(state="visible", timeout=per_try)
            await loc.click(timeout=per_try)
            await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            return
        except Exception:
            continue

    await page.goto(GOOGLE_PASSWORD_PAGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)


async def _do_change_password_flow(
    page: Any,
    *,
    platform_efa: str,
    previous_totp_code: str,
    timeout_ms: int,
    progress_cb: ProgressCB,
) -> None:
    """修改密码流程：跳转安全页 -> 2FA -> 填两次新密码 -> Change password。"""
    await _navigate_to_password_page(page, timeout_ms=timeout_ms, progress_cb=progress_cb)
    await progress_cb(65, {"stage": "security_password_page_loaded", "url": str(page.url or "")})

    second_tel_input = page.locator('input[type="tel"]').first
    await second_tel_input.wait_for(state="visible", timeout=timeout_ms)
    second_totp_code = await _generate_fresh_totp_code(platform_efa, previous_totp_code)
    await second_tel_input.fill(second_totp_code, timeout=timeout_ms)
    await progress_cb(75, {"stage": "second_totp_filled"})

    clicked_second_totp_next = await _click_next_button(page, timeout_ms=timeout_ms)
    await progress_cb(80, {"stage": "second_totp_next_clicked", "selector": clicked_second_totp_next})

    new_password_inputs = page.locator('input[type="password"]')
    count = await new_password_inputs.count()
    if count < 2:
        raise RuntimeError(f"密码修改页未找到两个密码输入框，当前匹配数量={count}")
    await new_password_inputs.nth(0).fill(NEW_PASSWORD_VALUE, timeout=timeout_ms)
    await new_password_inputs.nth(1).fill(NEW_PASSWORD_VALUE, timeout=timeout_ms)
    await progress_cb(90, {"stage": "new_passwords_filled"})

    clicked_save_or_next = await _click_save_or_next_button(page, timeout_ms=timeout_ms)
    await progress_cb(100, {"stage": "password_change_submitted", "selector": clicked_save_or_next})


async def sora_plus_register(
    payload: Dict[str, Any],
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
) -> Dict[str, Any]:
    """打开平台页 -> Google 登录 -> 跳转 ChatGPT -> Continue with Google 登录。"""
    _ = payload or {}
    timeout_ms = int(max(10_000, min(float(timeout_seconds) * 1000, 120_000)))

    await progress_cb(1, {"stage": "resolve_credentials", "window_pk": int(window_pk)})
    creds = await _resolve_window_platform_credentials(db, window_pk=int(window_pk))
    platform_url = str(creds["platform_url"] or "").strip()
    platform_username = str(creds["platform_username"] or "").strip()
    platform_password = str(creds["platform_password"] or "").strip()
    platform_efa = str(creds["platform_efa"] or "").strip()

    ctx = get_or_create_ctx(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    await ctx.ensure_open(args=[], force_open=False, headless=False)

    await progress_cb(3, {"stage": "open_browser", "platform_url": platform_url})
    async with ctx.driver_lock:
        if ctx.context is None:
            raise RuntimeError("浏览器上下文不可用：context is None")
        page = await pick_working_page_from_context(ctx.context)
        ctx.page = page

        closed_count = await _close_other_pages(ctx.context, page)
        await progress_cb(5, {"stage": "other_tabs_closed", "closed_count": closed_count})

        try:
            await page.bring_to_front()
        except Exception:
            pass

        await page.goto(platform_url, wait_until="domcontentloaded", timeout=timeout_ms)
        await progress_cb(10, {"stage": "page_loaded", "url": platform_url})

        current_url = str(page.url or "").strip()
        already_logged_in = _is_already_logged_in(current_url)

        if already_logged_in:
            await progress_cb(55, {"stage": "already_logged_in", "current_url": current_url})
            last_totp_code = ""
        else:
            await progress_cb(12, {"stage": "need_login", "current_url": current_url})
            last_totp_code = await _do_login_flow(
                page,
                platform_username=platform_username,
                platform_password=platform_password,
                platform_efa=platform_efa,
                timeout_ms=timeout_ms,
                progress_cb=progress_cb,
            )

        await _do_chatgpt_google_login(
            page,
            platform_efa=platform_efa,
            previous_totp_code=last_totp_code,
            timeout_ms=timeout_ms,
            progress_cb=progress_cb,
        )

    return {
        "ok": True,
        "stage": "chatgpt_google_login_done",
        "platform_url": platform_url,
        "platform_username": platform_username,
        "already_logged_in": already_logged_in,
        "message": "已完成 Google 登录并通过 ChatGPT Continue with Google 登录",
    }
