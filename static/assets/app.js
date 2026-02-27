// 基础工具（无构建版）

export function getAdminToken() {
  return localStorage.getItem("adminToken");
}

export function setAdminToken(token) {
  localStorage.setItem("adminToken", token);
}

export function clearAdminToken() {
  localStorage.removeItem("adminToken");
}

export function requireAdminAuth() {
  const t = getAdminToken();
  if (!t) {
    window.location.href = "/login";
    return null;
  }
  return t;
}

export async function adminFetch(url, options = {}) {
  const token = requireAdminAuth();
  if (!token) return null;
  const headers = new Headers(options.headers || {});
  headers.set("Authorization", `Bearer ${token}`);
  // 仅在 body 是纯对象/字符串时设置 JSON；FormData 交给浏览器处理
  if (!(options.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const resp = await fetch(url, { ...options, headers });
  if (resp.status === 401) {
    clearAdminToken();
    window.location.href = "/login";
    return null;
  }
  return resp;
}

export function toast(message, type = "info") {
  const colors = {
    success: "bg-success",
    error: "bg-danger",
    info: "bg-primary",
    warn: "bg-warning text-dark",
  };
  const el = document.createElement("div");
  el.className = `toast align-items-center text-white ${colors[type] || colors.info} border-0`;
  el.setAttribute("role", "alert");
  el.setAttribute("aria-live", "assertive");
  el.setAttribute("aria-atomic", "true");
  el.innerHTML = `
    <div class="d-flex">
      <div class="toast-body">${escapeHtml(message || "")}</div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
    </div>
  `;
  const container = document.getElementById("toastContainer");
  (container || document.body).appendChild(el);
  const t = new bootstrap.Toast(el, { delay: 2200 });
  t.show();
  el.addEventListener("hidden.bs.toast", () => el.remove());
}

export function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function setActiveNav(id) {
  document.querySelectorAll("[data-nav]").forEach((a) => a.classList.remove("active"));
  const el = document.querySelector(`[data-nav="${id}"]`);
  if (el) el.classList.add("active");
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatLocalDateTime(d = new Date()) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

export function mountNavbarClock({ selector = "[data-role='navbarClock']" } = {}) {
  if (typeof window === "undefined" || typeof document === "undefined") return;

  const w = window;
  const update = () => {
    const text = formatLocalDateTime(new Date());
    document.querySelectorAll(selector).forEach((el) => {
      try { el.textContent = text; } catch (_e) {}
    });
  };

  update();

  if (w.__fpbrowser2api_navbarClockTimer) return;
  w.__fpbrowser2api_navbarClockTimer = window.setInterval(update, 1000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) update(); });
}

export async function logout() {
  const r = await adminFetch("/api/logout", { method: "POST" });
  if (!r) return;
  clearAdminToken();
  window.location.href = "/login";
}

// 自动挂载：页面只要放了 data-role="navbarClock" 占位即可显示当前时间
try {
  if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", () => mountNavbarClock());
    } else {
      mountNavbarClock();
    }
  }
} catch (_e) {}

