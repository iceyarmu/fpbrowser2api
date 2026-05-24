function $(id) { return document.getElementById(id); }

function send(type, payload = {}) {
  return chrome.runtime.sendMessage({ type, ...payload });
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]));
}

function renderStatus(st) {
  const dot = $("dot");
  dot.className = "dot";
  if (st.connected && st.helloOk) dot.classList.add("ok");
  else if (st.wsState === "connecting" || st.reconnectScheduled) dot.classList.add("warn");
  else dot.classList.add("bad");

  $("connText").textContent = st.connected && st.helloOk
    ? "已连接并注册"
    : (st.connected ? "WebSocket 已连接，等待注册" : `未连接：${st.wsState || "unknown"}`);

  const active = st.activeTask
    ? `${st.activeTask.provider || ""} / ${st.activeTask.task_id || ""} / ${st.activeTask.started_at || ""}`
    : "无";
  $("statusKv").innerHTML = `
    <div>bridge: ${esc(st.bridgeUrl)}</div>
    <div>space/window: ${esc(st.spaceId)} / ${esc(st.windowKey)}</div>
    <div>client_id: ${esc(st.clientId || "")}</div>
    <div>active_task: ${esc(active)}</div>
    <div>last_event: ${esc(st.lastEventAt || "")}</div>
    <div>last_error: ${esc(st.lastError || "")}</div>
  `;
}

const TRANSFER_URL_RE = /(https?:\/\/[^\s<>'"]+)/ig;

function linkifyEscapedLine(line) {
  const raw = String(line ?? "");
  let out = "";
  let last = 0;
  raw.replace(TRANSFER_URL_RE, (m, _g, idx) => {
    out += esc(raw.slice(last, idx));
    const href = esc(m);
    out += `<a href="${href}" target="_blank" rel="noopener noreferrer">${href}</a>`;
    last = idx + m.length;
    return m;
  });
  out += esc(raw.slice(last));
  return out;
}

function getTransferLines(data) {
  if (Array.isArray(data?.lines)) return data.lines.map(x => String(x ?? ""));
  const text = String(data?.text || "");
  return text ? text.split(/\r?\n/) : [];
}

async function setActiveTab(tab) {
  const name = tab === "transfer" ? "transfer" : "debug";
  document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.toggle("active", btn.dataset.tab === name));
  $("debugPanel")?.classList.toggle("active", name === "debug");
  $("transferPanel")?.classList.toggle("active", name === "transfer");
  try { await send("popup.setActiveTab", { tab: name }); } catch (_) {}
}

function renderTransferData(data) {
  const root = $("transferData");
  const meta = $("transferMeta");
  if (!root) return;
  const lines = getTransferLines(data);
  if (meta) {
    const bits = [];
    if (data?.title) bits.push(data.title);
    if (data?.source) bits.push(`来源: ${data.source}`);
    if (data?.received_at) bits.push(`接收: ${data.received_at}`);
    meta.textContent = bits.join(" · ");
  }
  if (!lines.length) {
    root.innerHTML = `<div class="empty">暂无接收数据</div>`;
    return;
  }
  root.innerHTML = lines.map((line, idx) => `
    <div class="transfer-line">
      <div class="transfer-text">${linkifyEscapedLine(line)}</div>
      <button class="copy-line-btn" data-copy-line="${idx}" type="button">复制</button>
    </div>
  `).join("");
}

function renderLogs(logs) {
  const root = $("logs");
  if (!logs || !logs.length) {
    root.innerHTML = "<div class='hint'>暂无日志</div>";
    return;
  }
  root.innerHTML = logs.map(x => {
    const data = x.data ? "\n" + JSON.stringify(x.data, null, 2) : "";
    return `<div class="log ${esc(x.level || "")}">
      <span class="ts">${esc(x.ts || "")}</span>
      <span class="lvl">[${esc(x.level || "info")}]</span>
      ${esc(x.message || "")}${esc(data)}
    </div>`;
  }).join("");
}

async function refresh() {
  const resp = await send("popup.getState");
  if (!resp || !resp.ok) {
    $("connText").textContent = "读取 background 状态失败";
    return;
  }
  const cfg = resp.config || {};
  const st = resp.status || {};
  $("bridgeUrl").value = cfg.bridgeUrl || st.bridgeUrl || "";
  $("bridgeToken").value = cfg.bridgeToken || "";
  $("spaceId").value = cfg.spaceId || st.spaceId || "";
  $("windowKey").value = cfg.windowKey || st.windowKey || "";
  $("veoArchiveEnabled").checked = cfg.veoArchiveEnabled !== false;
  renderStatus(st);
  renderLogs(resp.logs || []);
  renderTransferData(resp.transferData || null);
  const active = resp.activeTab || "debug";
  document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.toggle("active", btn.dataset.tab === active));
  $("debugPanel")?.classList.toggle("active", active !== "transfer");
  $("transferPanel")?.classList.toggle("active", active === "transfer");
}

async function save() {
  await send("popup.saveConfig", {
    config: {
      bridgeUrl: $("bridgeUrl").value.trim(),
      bridgeToken: $("bridgeToken").value.trim(),
      spaceId: $("spaceId").value.trim(),
      windowKey: $("windowKey").value.trim(),
      veoArchiveEnabled: $("veoArchiveEnabled").checked
    }
  });
  setTimeout(refresh, 300);
}

async function reconnect() {
  await send("popup.reconnect");
  setTimeout(refresh, 300);
}

async function clearLogs() {
  await send("popup.clearLogs");
  await refresh();
}

async function copyText(text) {
  const t = String(text || "");
  if (!t) return;
  if (navigator?.clipboard?.writeText) return navigator.clipboard.writeText(t);
  const ta = document.createElement("textarea");
  ta.value = t;
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  document.execCommand("copy");
  document.body.removeChild(ta);
}

async function clearTransfer() {
  await send("popup.clearTransferData");
  await refresh();
}

async function copyAllTransfer() {
  const resp = await send("popup.getState");
  const lines = getTransferLines(resp?.transferData || null);
  await copyText(lines.join("\n"));
}

document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
});
$("transferData")?.addEventListener("click", async (e) => {
  const btn = e.target?.closest?.("[data-copy-line]");
  if (!btn) return;
  const resp = await send("popup.getState");
  const lines = getTransferLines(resp?.transferData || null);
  await copyText(lines[Number(btn.dataset.copyLine)] || "");
  btn.textContent = "已复制";
  setTimeout(() => { btn.textContent = "复制"; }, 900);
});

$("saveBtn").addEventListener("click", save);
$("reconnectBtn").addEventListener("click", reconnect);
$("refreshBtn").addEventListener("click", refresh);
$("clearBtn").addEventListener("click", clearLogs);
$("clearTransferBtn")?.addEventListener("click", clearTransfer);
$("copyAllTransferBtn")?.addEventListener("click", copyAllTransfer);
$("closePanelBtn")?.addEventListener("click", () => window.close());

refresh();
setInterval(refresh, 2000);
