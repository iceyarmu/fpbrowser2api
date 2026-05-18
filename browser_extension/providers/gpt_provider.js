import { fetchJson } from "./common.js";

const CHATGPT = "https://chatgpt.com";
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function uuid() { return crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`; }

async function ensureGptTab(targetUrl = CHATGPT) {
  const tabs = await chrome.tabs.query({});
  const found = tabs.find(t => (t.url || "").startsWith("https://chatgpt.com/"));
  if (found && found.id) return found.id;
  const tab = await chrome.tabs.create({ url: targetUrl || CHATGPT, active: true });
  await sleep(2500);
  return tab.id;
}

async function pageFetch(tabId, url, { method = "GET", headers = {}, body = null } = {}) {
  const frames = await chrome.scripting.executeScript({
    target: { tabId },
    world: "MAIN",
    args: [url, { method, headers, body }],
    func: async (u, opts) => {
      const init = { method: opts.method || "GET", headers: opts.headers || {}, credentials: "include" };
      if (opts.body !== null && opts.body !== undefined) init.body = JSON.stringify(opts.body);
      const r = await fetch(u, init);
      const text = await r.text();
      let json = null; try { json = text ? JSON.parse(text) : null; } catch (_) {}
      return { status: r.status, text, json, url: r.url };
    }
  });
  return Array.isArray(frames) && frames[0] ? frames[0].result : null;
}

async function getAccessToken(tabId) {
  const r = await pageFetch(tabId, `${CHATGPT}/api/auth/session`);
  const j = r && r.json || {};
  const tok = j.accessToken || j.access_token || j.token;
  if (!tok) throw new Error(`GPT access token not found: ${JSON.stringify(j).slice(0, 200)}`);
  return { type: "gpt_access_token", access_token: tok, expires: j.expires || null, raw: j };
}

function authHeaders(token, accept = "application/json") {
  return { "Authorization": `Bearer ${token}`, "Content-Type": "application/json", "Accept": accept };
}

async function requirements(tabId, token) {
  const r = await pageFetch(tabId, `${CHATGPT}/backend-api/sentinel/chat-requirements`, {
    method: "POST",
    headers: authHeaders(token),
    body: { p: "" }
  });
  if (!r || r.status >= 400) throw new Error(`chat-requirements failed ${r && r.status}: ${r && r.text}`);
  return r.json || {};
}

async function prepare(tabId, token, reqs, model) {
  const r = await pageFetch(tabId, `${CHATGPT}/backend-api/f/conversation/prepare`, {
    method: "POST",
    headers: { ...authHeaders(token), "OpenAI-Sentinel-Chat-Requirements-Token": reqs.token || "" },
    body: {
      action: "next", parent_message_id: "client-created-root", model,
      client_prepare_state: "none", timezone: "Asia/Shanghai", timezone_offset_min: -480,
      conversation_mode: { kind: "primary_assistant" }, system_hints: ["picture_v2"],
      supports_buffering: true, supported_encodings: ["v1"]
    }
  });
  if (!r || r.status >= 400) throw new Error(`conversation prepare failed ${r && r.status}: ${r && r.text}`);
  return (r.json || {}).conduit_token || "";
}

function extractAssetUrls(text) {
  const s = String(text || "");
  const urls = [];
  for (const m of s.matchAll(/https:\/\/[^\s"'\\]+/g)) {
    const u = m[0].replace(/\\u0026/g, "&");
    if (/files|sediment|oaiusercontent|videos|download/.test(u) && !urls.includes(u)) urls.push(u);
  }
  const ids = [];
  for (const m of s.matchAll(/file-[a-zA-Z0-9_-]+|file_[a-zA-Z0-9_-]+/g)) if (!ids.includes(m[0])) ids.push(m[0]);
  return { urls, ids };
}

async function startConversation(tabId, token, reqs, conduit, payload, kind) {
  const model = payload.model || payload.model_code || (kind === "video" ? "gpt-4o" : "gpt-4o");
  const prompt = payload.prompt;
  const hints = kind === "image" ? ["picture_v2"] : [];
  const content = { content_type: "text", parts: [prompt] };
  const body = {
    action: "next", parent_message_id: "client-created-root", model,
    client_prepare_state: "success", timezone: "Asia/Shanghai", timezone_offset_min: -480,
    conversation_mode: { kind: "primary_assistant" }, system_hints: hints,
    supports_buffering: true, supported_encodings: ["v1"], enable_message_followups: true,
    messages: [{ id: uuid(), author: { role: "user" }, create_time: Math.floor(Date.now()/1000), content, metadata: { system_hints: hints } }]
  };
  const headers = { ...authHeaders(token, "text/event-stream"), "OpenAI-Sentinel-Chat-Requirements-Token": reqs.token || "" };
  if (conduit) headers["OpenAI-Conduit-Token"] = conduit;
  const r = await pageFetch(tabId, `${CHATGPT}/backend-api/f/conversation`, { method: "POST", headers, body });
  if (!r || r.status >= 400) throw new Error(`conversation failed ${r && r.status}: ${r && r.text}`);
  const got = extractAssetUrls(r.text);
  const cid = ((r.text || "").match(/"conversation_id"\s*:\s*"([^"]+)"/) || [])[1] || "";
  return { conversation_id: cid, ...got, raw_text: (r.text || "").slice(-2000) };
}

async function pollConversation(tabId, token, cid, maxWaitMs, runtime) {
  const end = Date.now() + maxWaitMs;
  let last = { urls: [], ids: [] };
  while (cid && Date.now() < end) {
    const r = await pageFetch(tabId, `${CHATGPT}/backend-api/conversation/${cid}`, { headers: authHeaders(token) });
    if (r && r.status < 400) {
      last = extractAssetUrls(r.text);
      if (last.urls.length || last.ids.length) return last;
    }
    await runtime.progress(50, { stage: "poll", conversation_id: cid });
    await sleep(5000);
  }
  return last;
}

export async function runGptTask(msg, runtime) {
  const p = msg.payload || {};
  const tabId = await ensureGptTab(p.target_url || CHATGPT);
  if (p.action === "get_access_token") return await getAccessToken(tabId);
  const token = p.access_token || (await getAccessToken(tabId)).access_token;
  const kind = p.workflow_kind || "image";
  await runtime.progress(5, { stage: "requirements", workflow_kind: kind });
  const reqs = await requirements(tabId, token);
  const model = p.model || p.model_code || "gpt-4o";
  const conduit = await prepare(tabId, token, reqs, model);
  await runtime.progress(15, { stage: "submit_conversation" });
  const started = await startConversation(tabId, token, reqs, conduit, p, kind);
  const polled = await pollConversation(tabId, token, started.conversation_id, Number(p.max_wait_seconds || p.timeout_seconds || 600) * 1000, runtime);
  const urls = [...new Set([...(started.urls || []), ...(polled.urls || [])])];
  const ids = [...new Set([...(started.ids || []), ...(polled.ids || [])])];
  if (!urls.length && !ids.length) throw new Error(`GPT ${kind} returned no assets; cid=${started.conversation_id}`);
  await runtime.progress(100, { stage: "done", asset_count: urls.length, file_ids: ids.length });
  return {
    type: kind === "video" ? "gpt_workflow_video" : "gpt_workflow_image",
    conversation_id: started.conversation_id,
    urls,
    file_ids: ids,
    image_url: kind === "image" ? (urls[0] || "") : undefined,
    video_url: kind === "video" ? (urls[0] || "") : undefined,
    raw_text_tail: started.raw_text
  };
}

