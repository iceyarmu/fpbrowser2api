// Network capture provider for FPBrowser2API extension.
// Captures fetch/XMLHttpRequest/sendBeacon traffic from the page context and keeps
// a DevTools-like in-memory log in the extension service worker.

const DEFAULT_METHODS = ["GET", "POST", "PATCH"];
const DEFAULT_MAX_ENTRIES = 2000;
const DEFAULT_MAX_BODY_CHARS = 60000;

let capture = {
  sessionId: "",
  running: false,
  paused: false,
  tabId: null,
  targetUrl: "",
  targetOrigin: "",
  methods: new Set(DEFAULT_METHODS),
  maxEntries: DEFAULT_MAX_ENTRIES,
  maxBodyChars: DEFAULT_MAX_BODY_CHARS,
  events: [],
  seq: 0,
  startedAt: null,
  updatedAt: null,
  lastError: "",
  includeWebRequestMeta: true
};

let webRequestInstalled = false;
const webRequests = new Map();

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeMethods(methods) {
  const arr = Array.isArray(methods) && methods.length ? methods : DEFAULT_METHODS;
  const out = arr.map(x => String(x || "").trim().toUpperCase()).filter(Boolean);
  return new Set(out.length ? out : DEFAULT_METHODS);
}

function normalizeUrl(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  try {
    const u = new URL(s);
    if (u.protocol === "http:" || u.protocol === "https:") return u.href;
  } catch (_) {}
  return "";
}

function originOf(raw) {
  try { return new URL(raw).origin; } catch (_) { return ""; }
}

function pathOf(raw) {
  try {
    const u = new URL(raw);
    return `${u.pathname || "/"}${u.search || ""}`;
  } catch (_) {
    return String(raw || "");
  }
}

function methodAllowed(method) {
  return capture.methods.has(String(method || "GET").toUpperCase());
}

function clipString(value, maxChars = capture.maxBodyChars) {
  if (value === null || value === undefined) return "";
  let s;
  try {
    s = typeof value === "string" ? value : JSON.stringify(value);
  } catch (_) {
    s = String(value);
  }
  const max = Math.max(1000, Number(maxChars || DEFAULT_MAX_BODY_CHARS));
  if (s.length <= max) return s;
  return `${s.slice(0, max)}\n...[truncated ${s.length - max} chars]`;
}

function headersArrayToObject(headers) {
  const out = {};
  if (!Array.isArray(headers)) return out;
  for (const h of headers) {
    const name = String(h && h.name || "").trim();
    if (!name) continue;
    const value = String(h && h.value || "");
    if (Object.prototype.hasOwnProperty.call(out, name)) out[name] = `${out[name]}, ${value}`;
    else out[name] = value;
  }
  return out;
}

function trimWebRequests() {
  const cutoff = Date.now() - 2 * 60 * 1000;
  for (const [rid, meta] of webRequests.entries()) {
    if (Number(meta.last_seen_ms || meta.start_ms || 0) < cutoff) webRequests.delete(rid);
  }
  if (webRequests.size > 2000) {
    const rows = Array.from(webRequests.entries()).sort((a, b) => Number(a[1].last_seen_ms || 0) - Number(b[1].last_seen_ms || 0));
    for (const [rid] of rows.slice(0, Math.max(0, rows.length - 1500))) webRequests.delete(rid);
  }
}

function shouldTrackWebRequest(details) {
  if (!capture.running || capture.paused || !capture.includeWebRequestMeta) return false;
  if (capture.tabId !== null && Number(details.tabId) !== Number(capture.tabId)) return false;
  return methodAllowed(details.method);
}

function ensureWebMeta(details) {
  let meta = webRequests.get(details.requestId);
  if (!meta) {
    meta = {
      request_id: details.requestId,
      tab_id: details.tabId,
      frame_id: details.frameId,
      type: details.type || "",
      method: String(details.method || "GET").toUpperCase(),
      url: details.url || "",
      start_ms: Number(details.timeStamp || Date.now()),
      last_seen_ms: Date.now()
    };
    webRequests.set(details.requestId, meta);
  }
  meta.last_seen_ms = Date.now();
  return meta;
}

function decodeRequestBody(requestBody) {
  if (!requestBody) return "";
  try {
    if (requestBody.formData && typeof requestBody.formData === "object") {
      return clipString(JSON.stringify(requestBody.formData));
    }
    if (Array.isArray(requestBody.raw) && requestBody.raw.length) {
      const parts = [];
      for (const item of requestBody.raw) {
        if (item && item.bytes) {
          try { parts.push(new TextDecoder("utf-8", { fatal: false }).decode(item.bytes)); }
          catch (_) { parts.push(`[binary ${item.bytes.byteLength || 0} bytes]`); }
        }
      }
      return clipString(parts.join(""));
    }
    if (requestBody.error) return `[requestBody error] ${requestBody.error}`;
  } catch (e) {
    return `[requestBody decode failed] ${String(e && e.message || e)}`;
  }
  return "";
}

function installWebRequestListeners() {
  if (webRequestInstalled) return;
  if (!chrome.webRequest) return;
  webRequestInstalled = true;
  const filter = { urls: ["<all_urls>"] };

  try {
    chrome.webRequest.onBeforeRequest.addListener((details) => {
      if (!shouldTrackWebRequest(details)) return;
      const meta = ensureWebMeta(details);
      meta.request_body = decodeRequestBody(details.requestBody);
      trimWebRequests();
    }, filter, ["requestBody"]);
  } catch (_) {
    try {
      chrome.webRequest.onBeforeRequest.addListener((details) => {
        if (!shouldTrackWebRequest(details)) return;
        ensureWebMeta(details);
        trimWebRequests();
      }, filter);
    } catch (__) {}
  }

  const addHeaderListener = (event, fn, specs) => {
    try { event.addListener(fn, filter, specs); }
    catch (_) {
      try { event.addListener(fn, filter, specs.filter(x => x !== "extraHeaders")); } catch (__) {}
    }
  };

  addHeaderListener(chrome.webRequest.onBeforeSendHeaders, (details) => {
    if (!shouldTrackWebRequest(details)) return;
    const meta = ensureWebMeta(details);
    meta.request_headers = headersArrayToObject(details.requestHeaders || []);
  }, ["requestHeaders", "extraHeaders"]);

  addHeaderListener(chrome.webRequest.onHeadersReceived, (details) => {
    if (!shouldTrackWebRequest(details)) return;
    const meta = ensureWebMeta(details);
    meta.status = Number(details.statusCode || 0) || 0;
    meta.response_headers = headersArrayToObject(details.responseHeaders || []);
  }, ["responseHeaders", "extraHeaders"]);

  chrome.webRequest.onCompleted.addListener((details) => {
    if (!shouldTrackWebRequest(details)) return;
    const meta = ensureWebMeta(details);
    meta.status = Number(details.statusCode || 0) || meta.status || 0;
    meta.from_cache = !!details.fromCache;
    meta.ip = details.ip || "";
    meta.completed_ms = Number(details.timeStamp || Date.now());
  }, filter);

  chrome.webRequest.onErrorOccurred.addListener((details) => {
    if (!shouldTrackWebRequest(details)) return;
    const meta = ensureWebMeta(details);
    meta.error = details.error || "network error";
    meta.completed_ms = Number(details.timeStamp || Date.now());
  }, filter);
}

function findBestWebMeta(event, tabId) {
  if (!event || !event.url) return null;
  const method = String(event.method || "GET").toUpperCase();
  const startMs = Number(event.start_epoch_ms || 0) || 0;
  const endMs = Number(event.end_epoch_ms || Date.now()) || Date.now();
  let best = null;
  let bestScore = Number.POSITIVE_INFINITY;
  for (const meta of webRequests.values()) {
    if (Number(meta.tab_id) !== Number(tabId)) continue;
    if (String(meta.method || "").toUpperCase() !== method) continue;
    if (String(meta.url || "") !== String(event.url || "")) continue;
    const ms = Number(meta.start_ms || 0);
    if (startMs && (ms < startMs - 5000 || ms > endMs + 5000)) continue;
    const score = Math.abs(ms - (startMs || endMs));
    if (score < bestScore) {
      bestScore = score;
      best = meta;
    }
  }
  return best;
}

function sanitizeEvent(raw, sender) {
  const event = raw && typeof raw === "object" ? { ...raw } : {};
  const tabId = sender && sender.tab && sender.tab.id !== undefined ? Number(sender.tab.id) : capture.tabId;
  const webMeta = findBestWebMeta(event, tabId);
  const method = String(event.method || (webMeta && webMeta.method) || "GET").toUpperCase();
  const url = String(event.url || (webMeta && webMeta.url) || "");
  const responseHeaders = event.response_headers && Object.keys(event.response_headers || {}).length ? event.response_headers : (webMeta && webMeta.response_headers) || {};
  const requestHeaders = event.request_headers && Object.keys(event.request_headers || {}).length ? event.request_headers : (webMeta && webMeta.request_headers) || {};
  const requestPayload = event.request_payload || (webMeta && webMeta.request_body) || "";
  return {
    seq: ++capture.seq,
    id: String(event.id || `${Date.now()}-${capture.seq}`),
    session_id: capture.sessionId,
    tab_id: tabId,
    frame_url: clipString(event.frame_url || "", 2000),
    source: String(event.source || "page"),
    resource_type: String(event.resource_type || (webMeta && webMeta.type) || "fetch/xhr"),
    method,
    url,
    origin: originOf(url),
    path: event.path || pathOf(url),
    status: Number(event.status || (webMeta && webMeta.status) || 0) || 0,
    ok: event.ok === undefined ? undefined : !!event.ok,
    duration_ms: Number(event.duration_ms || 0) || 0,
    started_at: event.started_at || nowIso(),
    completed_at: event.completed_at || nowIso(),
    start_epoch_ms: Number(event.start_epoch_ms || 0) || undefined,
    end_epoch_ms: Number(event.end_epoch_ms || 0) || undefined,
    request_headers: requestHeaders || {},
    request_payload: clipString(requestPayload),
    response_headers: responseHeaders || {},
    response_body: clipString(event.response_body || ""),
    response_type: String(event.response_type || ""),
    error: clipString(event.error || (webMeta && webMeta.error) || "", 4000),
    initiator_stack: clipString(event.initiator_stack || "", 20000),
    web_request_id: webMeta && webMeta.request_id || "",
    from_cache: !!(webMeta && webMeta.from_cache),
    ip: webMeta && webMeta.ip || ""
  };
}

function pushEvent(raw, sender) {
  if (!capture.running || capture.paused) return { ok: true, ignored: true, reason: "not_running_or_paused" };
  const ev = sanitizeEvent(raw, sender);
  if (!methodAllowed(ev.method)) return { ok: true, ignored: true, reason: "method_filtered" };
  if (capture.tabId !== null && ev.tab_id !== null && Number(ev.tab_id) !== Number(capture.tabId)) {
    return { ok: true, ignored: true, reason: "tab_filtered" };
  }
  capture.events.push(ev);
  const max = Math.max(100, Number(capture.maxEntries || DEFAULT_MAX_ENTRIES));
  if (capture.events.length > max) capture.events.splice(0, capture.events.length - max);
  capture.updatedAt = nowIso();
  return { ok: true, seq: ev.seq };
}

function getStatus() {
  const counts = { total: capture.events.length, GET: 0, POST: 0, PATCH: 0, errors: 0 };
  for (const ev of capture.events) {
    const m = String(ev.method || "").toUpperCase();
    if (Object.prototype.hasOwnProperty.call(counts, m)) counts[m] += 1;
    if (ev.error || (Number(ev.status || 0) >= 400)) counts.errors += 1;
  }
  return {
    success: true,
    running: capture.running,
    paused: capture.paused,
    session_id: capture.sessionId,
    tab_id: capture.tabId,
    target_url: capture.targetUrl,
    target_origin: capture.targetOrigin,
    started_at: capture.startedAt,
    updated_at: capture.updatedAt,
    last_error: capture.lastError,
    seq: capture.seq,
    count: capture.events.length,
    counts
  };
}

async function getActiveHttpTab() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  const tab = tabs && tabs[0];
  if (tab && tab.id && /^https?:\/\//i.test(tab.url || "")) return tab;
  const all = await chrome.tabs.query({});
  return all.find(t => t.id && /^https?:\/\//i.test(t.url || "")) || null;
}

async function findOrOpenTargetTab(targetUrl, { active = true, navigate = true } = {}) {
  const target = normalizeUrl(targetUrl);
  if (!target) {
    const tab = await getActiveHttpTab();
    if (!tab || !tab.id) throw new Error("没有可注入的 http/https 标签页，请先打开目标页面");
    return tab.id;
  }
  const origin = originOf(target);
  const tabs = await chrome.tabs.query({});
  let found = tabs.find(t => t.id && (t.url || "") === target);
  if (!found && origin) found = tabs.find(t => t.id && String(t.url || "").startsWith(origin + "/"));
  if (found && found.id) {
    if (navigate && found.url !== target) await chrome.tabs.update(found.id, { url: target, active });
    else if (active) await chrome.tabs.update(found.id, { active: true });
    return found.id;
  }
  const tab = await chrome.tabs.create({ url: target, active });
  if (!tab || !tab.id) throw new Error("打开目标标签页失败");
  return tab.id;
}

async function waitForTabReady(tabId, timeoutMs = 15000) {
  const end = Date.now() + Math.max(1000, Number(timeoutMs || 15000));
  while (Date.now() < end) {
    try {
      const tab = await chrome.tabs.get(tabId);
      if (tab && /^https?:\/\//i.test(tab.url || "") && (tab.status === "complete" || tab.status === "loading")) return tab;
    } catch (_) {}
    await sleep(250);
  }
  return await chrome.tabs.get(tabId);
}

function installNetworkCaptureBridge(config) {
  try {
    window.__FPB_NETWORK_CAPTURE_BRIDGE_CONFIG__ = Object.assign({}, window.__FPB_NETWORK_CAPTURE_BRIDGE_CONFIG__ || {}, config || {});
    if (window.__FPB_NETWORK_CAPTURE_BRIDGE_INSTALLED__) return { ok: true, reused: true };
    window.__FPB_NETWORK_CAPTURE_BRIDGE_INSTALLED__ = true;
    window.addEventListener("message", (ev) => {
      try {
        if (ev.source !== window) return;
        const data = ev.data || {};
        if (!data || data.source !== "fpb-network-capture" || !data.event) return;
        chrome.runtime.sendMessage({ type: "fpb.networkCapture.event", event: data.event }, () => void chrome.runtime.lastError);
      } catch (_) {}
    }, false);
    return { ok: true, installed: true };
  } catch (e) {
    return { ok: false, error: String(e && e.message || e) };
  }
}

function installMainWorldCapture(config) {
  const CFG_KEY = "__FPB_NETWORK_CAPTURE_CONFIG__";
  const ORIG_KEY = "__FPB_NETWORK_CAPTURE_ORIGINALS__";
  const MARK = "__FPB_NETWORK_CAPTURE_INSTALLED__";
  const cfg = Object.assign({
    enabled: true,
    paused: false,
    sessionId: "",
    methods: ["GET", "POST", "PATCH"],
    maxBodyChars: 60000
  }, window[CFG_KEY] || {}, config || {});
  cfg.methods = (Array.isArray(cfg.methods) && cfg.methods.length ? cfg.methods : ["GET", "POST", "PATCH"]).map(x => String(x || "").toUpperCase());
  window[CFG_KEY] = cfg;

  function iso() { return new Date().toISOString(); }
  function clip(value, maxChars) {
    if (value === null || value === undefined) return "";
    let s = "";
    try { s = typeof value === "string" ? value : JSON.stringify(value); } catch (_) { s = String(value); }
    const max = Math.max(1000, Number(maxChars || window[CFG_KEY].maxBodyChars || 60000));
    if (s.length <= max) return s;
    return s.slice(0, max) + "\n...[truncated " + (s.length - max) + " chars]";
  }
  function allowed(method) {
    const c = window[CFG_KEY] || {};
    if (!c.enabled || c.paused) return false;
    const ms = Array.isArray(c.methods) ? c.methods : ["GET", "POST", "PATCH"];
    return ms.includes(String(method || "GET").toUpperCase());
  }
  function pathOfLocal(url) {
    try { const u = new URL(String(url || ""), location.href); return (u.pathname || "/") + (u.search || ""); } catch (_) { return String(url || ""); }
  }
  function absUrl(url) {
    try { return new URL(String(url || ""), location.href).href; } catch (_) { return String(url || ""); }
  }
  function headersToObj(headers) {
    const out = {};
    try {
      if (!headers) return out;
      if (headers instanceof Headers) {
        headers.forEach((v, k) => { out[k] = v; });
      } else if (Array.isArray(headers)) {
        headers.forEach(([k, v]) => { out[String(k)] = String(v); });
      } else if (typeof headers === "object") {
        Object.keys(headers).forEach(k => { out[k] = String(headers[k]); });
      }
    } catch (_) {}
    return out;
  }
  function parseRawHeaders(raw) {
    const out = {};
    try {
      String(raw || "").trim().split(/\r?\n/).forEach(line => {
        const i = line.indexOf(":");
        if (i > 0) out[line.slice(0, i).trim()] = line.slice(i + 1).trim();
      });
    } catch (_) {}
    return out;
  }
  async function bodyPreview(body) {
    try {
      if (body === null || body === undefined) return "";
      if (typeof body === "string") return clip(body);
      if (body instanceof URLSearchParams) return clip(body.toString());
      if (typeof FormData !== "undefined" && body instanceof FormData) {
        const rows = [];
        body.forEach((v, k) => {
          if (typeof File !== "undefined" && v instanceof File) rows.push([k, `[File name=${v.name} type=${v.type} size=${v.size}]`]);
          else rows.push([k, String(v)]);
        });
        return clip(JSON.stringify(rows));
      }
      if (typeof Blob !== "undefined" && body instanceof Blob) {
        const txt = await body.slice(0, Number((window[CFG_KEY] || {}).maxBodyChars || 60000)).text();
        return clip(txt);
      }
      if (body instanceof ArrayBuffer) return clip(new TextDecoder("utf-8", { fatal: false }).decode(body));
      if (ArrayBuffer.isView(body)) return clip(new TextDecoder("utf-8", { fatal: false }).decode(body));
      if (typeof ReadableStream !== "undefined" && body instanceof ReadableStream) return "[ReadableStream]";
      return clip(body);
    } catch (e) {
      return `[body preview failed] ${String(e && e.message || e)}`;
    }
  }
  function emit(event) {
    try {
      const c = window[CFG_KEY] || {};
      event.session_id = c.sessionId || "";
      event.frame_url = location.href;
      window.postMessage({ source: "fpb-network-capture", event }, "*");
    } catch (_) {}
  }
  function stack() {
    try { throw new Error("FPB network initiator"); } catch (e) { return String(e && e.stack || ""); }
  }
  function mimicNativeToString(patchedFn, originalFn) {
    try {
      Object.defineProperty(patchedFn, "toString", {
        value: function toString() { return Function.prototype.toString.call(originalFn); },
        configurable: true
      });
    } catch (_) {}
  }
  function baseEvent(source, method, url, startedAtMs, startedIso, st) {
    const u = absUrl(url);
    return {
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      source,
      method: String(method || "GET").toUpperCase(),
      url: u,
      path: pathOfLocal(u),
      started_at: startedIso,
      start_epoch_ms: st || Date.now(),
      duration_ms: Math.max(0, Math.round((performance.now() - startedAtMs) * 10) / 10),
      completed_at: iso(),
      end_epoch_ms: Date.now()
    };
  }

  if (window[MARK]) return { ok: true, reused: true };
  window[MARK] = true;
  const orig = window[ORIG_KEY] || {};
  orig.fetch = orig.fetch || window.fetch;
  orig.xhrOpen = orig.xhrOpen || XMLHttpRequest.prototype.open;
  orig.xhrSend = orig.xhrSend || XMLHttpRequest.prototype.send;
  orig.xhrSetRequestHeader = orig.xhrSetRequestHeader || XMLHttpRequest.prototype.setRequestHeader;
  orig.sendBeacon = orig.sendBeacon || (navigator && navigator.sendBeacon ? navigator.sendBeacon.bind(navigator) : null);
  window[ORIG_KEY] = orig;

  if (typeof orig.fetch === "function") {
    window.fetch = function fpbNetworkCaptureFetch(input, init) {
      let method = "GET";
      let url = "";
      let reqHeaders = {};
      let reqBody = null;
      try {
        if (typeof Request !== "undefined" && input instanceof Request) {
          method = (init && init.method) || input.method || "GET";
          url = input.url;
          reqHeaders = Object.assign({}, headersToObj(input.headers), headersToObj(init && init.headers));
          reqBody = init && Object.prototype.hasOwnProperty.call(init, "body") ? init.body : null;
        } else {
          method = (init && init.method) || "GET";
          url = String(input || "");
          reqHeaders = headersToObj(init && init.headers);
          reqBody = init && Object.prototype.hasOwnProperty.call(init, "body") ? init.body : null;
        }
      } catch (_) {}
      if (!allowed(method)) return orig.fetch.apply(this, arguments);

      const startedPerf = performance.now();
      const startedIso = iso();
      const startedEpoch = Date.now();
      const initiator = stack();
      const reqPayloadPromise = (async () => {
        if (reqBody !== null && reqBody !== undefined) return bodyPreview(reqBody);
        try {
          if (typeof Request !== "undefined" && input instanceof Request) return await input.clone().text();
        } catch (_) {}
        return "";
      })();
      return orig.fetch.apply(this, arguments).then((resp) => {
        Promise.resolve(reqPayloadPromise).then((requestPayload) => {
          const responseHeaders = headersToObj(resp && resp.headers);
          const eventBase = baseEvent("fetch", method, url, startedPerf, startedIso, startedEpoch);
          eventBase.status = resp && resp.status || 0;
          eventBase.ok = !!(resp && resp.ok);
          eventBase.response_type = resp && resp.type || "";
          eventBase.request_headers = reqHeaders;
          eventBase.request_payload = clip(requestPayload);
          eventBase.response_headers = responseHeaders;
          eventBase.initiator_stack = initiator;
          try {
            resp.clone().text().then((txt) => {
              eventBase.response_body = clip(txt);
              emit(eventBase);
            }).catch((e) => {
              eventBase.response_body = "";
              eventBase.error = `[response read failed] ${String(e && e.message || e)}`;
              emit(eventBase);
            });
          } catch (e) {
            eventBase.error = `[response clone failed] ${String(e && e.message || e)}`;
            emit(eventBase);
          }
        });
        return resp;
      }).catch((e) => {
        Promise.resolve(reqPayloadPromise).then((requestPayload) => {
          const ev = baseEvent("fetch", method, url, startedPerf, startedIso, startedEpoch);
          ev.request_headers = reqHeaders;
          ev.request_payload = clip(requestPayload);
          ev.error = String(e && e.message || e);
          ev.initiator_stack = initiator;
          emit(ev);
        });
        throw e;
      });
    };
    try { Object.defineProperty(window.fetch, "name", { value: "fetch" }); } catch (_) {}
    mimicNativeToString(window.fetch, orig.fetch);
  }

  XMLHttpRequest.prototype.open = function fpbNetworkCaptureXhrOpen(method, url) {
    try {
      this.__fpbNetworkCapture = {
        method: String(method || "GET").toUpperCase(),
        url: absUrl(url),
        request_headers: {},
        initiator_stack: stack()
      };
    } catch (_) {}
    return orig.xhrOpen.apply(this, arguments);
  };
  mimicNativeToString(XMLHttpRequest.prototype.open, orig.xhrOpen);

  XMLHttpRequest.prototype.setRequestHeader = function fpbNetworkCaptureXhrSetHeader(name, value) {
    try {
      if (this.__fpbNetworkCapture && name) this.__fpbNetworkCapture.request_headers[String(name)] = String(value);
    } catch (_) {}
    return orig.xhrSetRequestHeader.apply(this, arguments);
  };
  mimicNativeToString(XMLHttpRequest.prototype.setRequestHeader, orig.xhrSetRequestHeader);

  XMLHttpRequest.prototype.send = function fpbNetworkCaptureXhrSend(body) {
    const meta = this.__fpbNetworkCapture || { method: "GET", url: "" };
    if (!allowed(meta.method)) return orig.xhrSend.apply(this, arguments);
    const startedPerf = performance.now();
    const startedIso = iso();
    const startedEpoch = Date.now();
    const reqPayloadPromise = bodyPreview(body);
    const xhr = this;
    let emitted = false;
    function done(kind) {
      if (emitted) return;
      emitted = true;
      Promise.resolve(reqPayloadPromise).then((requestPayload) => {
        const ev = baseEvent("xhr", meta.method, meta.url, startedPerf, startedIso, startedEpoch);
        ev.status = Number(xhr.status || 0) || 0;
        ev.ok = ev.status >= 200 && ev.status < 400;
        ev.request_headers = meta.request_headers || {};
        ev.request_payload = clip(requestPayload);
        try { ev.response_headers = parseRawHeaders(xhr.getAllResponseHeaders()); } catch (_) { ev.response_headers = {}; }
        ev.response_type = xhr.responseType || "";
        try {
          if (!xhr.responseType || xhr.responseType === "text") ev.response_body = clip(xhr.responseText || "");
          else if (xhr.responseType === "json") ev.response_body = clip(xhr.response);
          else ev.response_body = `[${xhr.responseType || "binary"} response]`;
        } catch (e) { ev.response_body = `[response read failed] ${String(e && e.message || e)}`; }
        if (kind && kind !== "loadend") ev.error = kind;
        ev.initiator_stack = meta.initiator_stack || "";
        emit(ev);
      });
    }
    try {
      xhr.addEventListener("loadend", () => done("loadend"), { once: true });
      xhr.addEventListener("error", () => done("error"), { once: true });
      xhr.addEventListener("timeout", () => done("timeout"), { once: true });
      xhr.addEventListener("abort", () => done("abort"), { once: true });
    } catch (_) {}
    return orig.xhrSend.apply(this, arguments);
  };
  mimicNativeToString(XMLHttpRequest.prototype.send, orig.xhrSend);

  if (orig.sendBeacon) {
    navigator.sendBeacon = function fpbNetworkCaptureSendBeacon(url, data) {
      const method = "POST";
      if (!allowed(method)) return orig.sendBeacon.apply(navigator, arguments);
      const startedPerf = performance.now();
      const startedIso = iso();
      const startedEpoch = Date.now();
      const initiator = stack();
      const result = orig.sendBeacon.apply(navigator, arguments);
      bodyPreview(data).then((requestPayload) => {
        const ev = baseEvent("sendBeacon", method, url, startedPerf, startedIso, startedEpoch);
        ev.status = 0;
        ev.ok = !!result;
        ev.request_payload = clip(requestPayload);
        ev.response_body = "[sendBeacon has no readable response body]";
        ev.initiator_stack = initiator;
        emit(ev);
      });
      return result;
    };
    mimicNativeToString(navigator.sendBeacon, orig.sendBeacon);
  }

  return { ok: true, installed: true };
}

function updateMainWorldCaptureConfig(patch) {
  const key = "__FPB_NETWORK_CAPTURE_CONFIG__";
  window[key] = Object.assign({}, window[key] || {}, patch || {});
  if (Array.isArray(window[key].methods)) window[key].methods = window[key].methods.map(x => String(x || "").toUpperCase());
  return { ok: true, config: window[key] };
}

async function injectCapture(tabId) {
  const cfg = {
    enabled: capture.running,
    paused: capture.paused,
    sessionId: capture.sessionId,
    methods: Array.from(capture.methods),
    maxBodyChars: capture.maxBodyChars
  };
  const run = async (allFrames) => {
    await chrome.scripting.executeScript({
      target: { tabId, allFrames },
      world: "ISOLATED",
      func: installNetworkCaptureBridge,
      args: [cfg]
    });
    await chrome.scripting.executeScript({
      target: { tabId, allFrames },
      world: "MAIN",
      func: installMainWorldCapture,
      args: [cfg]
    });
  };
  try {
    await run(true);
  } catch (e) {
    capture.lastError = `allFrames inject failed, retried main frame: ${String(e && e.message || e)}`;
    await run(false);
  }
}

async function updateInjectedConfig(patch) {
  if (capture.tabId === null) return;
  try {
    try {
      await chrome.scripting.executeScript({
        target: { tabId: capture.tabId, allFrames: true },
        world: "MAIN",
        func: updateMainWorldCaptureConfig,
        args: [patch]
      });
    } catch (_) {
      await chrome.scripting.executeScript({
        target: { tabId: capture.tabId, allFrames: false },
        world: "MAIN",
        func: updateMainWorldCaptureConfig,
        args: [patch]
      });
    }
  } catch (e) {
    capture.lastError = String(e && e.message || e);
  }
}

async function startCapture(payload, runtime) {
  installWebRequestListeners();
  const targetUrl = normalizeUrl(payload.target_url || payload.targetUrl || "");
  const tabId = await findOrOpenTargetTab(targetUrl, {
    active: payload.active !== false,
    navigate: payload.navigate !== false
  });
  await waitForTabReady(tabId, Number(payload.wait_tab_ms || 15000));

  capture.sessionId = String(payload.session_id || `netcap-${Date.now()}`);
  capture.running = true;
  capture.paused = false;
  capture.tabId = tabId;
  capture.targetUrl = targetUrl;
  capture.targetOrigin = originOf(targetUrl);
  capture.methods = normalizeMethods(payload.methods);
  capture.maxEntries = Math.max(100, Math.min(20000, Number(payload.max_entries || DEFAULT_MAX_ENTRIES)));
  capture.maxBodyChars = Math.max(1000, Math.min(1000000, Number(payload.max_body_chars || DEFAULT_MAX_BODY_CHARS)));
  capture.includeWebRequestMeta = payload.include_web_request_meta !== false;
  capture.startedAt = nowIso();
  capture.updatedAt = capture.startedAt;
  capture.lastError = "";
  if (payload.clear !== false) {
    capture.events = [];
    capture.seq = 0;
    webRequests.clear();
  }

  await runtime.progress(5, { stage: "network_capture_injecting", tab_id: tabId, target_url: targetUrl });
  await injectCapture(tabId);
  await runtime.progress(100, { stage: "network_capture_started", tab_id: tabId });
  return { ...getStatus(), message: "network capture started" };
}

async function pauseCapture(paused) {
  capture.paused = !!paused;
  capture.updatedAt = nowIso();
  await updateInjectedConfig({ paused: capture.paused, enabled: capture.running });
  return { ...getStatus(), message: capture.paused ? "network capture paused" : "network capture resumed" };
}

async function stopCapture() {
  capture.running = false;
  capture.paused = false;
  capture.updatedAt = nowIso();
  await updateInjectedConfig({ enabled: false, paused: false });
  return { ...getStatus(), message: "network capture stopped" };
}

function clearCapture() {
  capture.events = [];
  capture.seq = 0;
  capture.updatedAt = nowIso();
  webRequests.clear();
  return { ...getStatus(), message: "network capture cleared" };
}

function snapshot(payload = {}) {
  const sinceSeq = Math.max(0, Number(payload.since_seq || payload.sinceSeq || 0));
  const limit = Math.max(1, Math.min(5000, Number(payload.limit || 1000)));
  let items = capture.events.filter(ev => Number(ev.seq || 0) > sinceSeq);
  if (items.length > limit) items = items.slice(items.length - limit);
  return { ...getStatus(), events: items };
}

export async function handleNetworkRuntimeMessage(message, sender) {
  if (!message || message.type !== "fpb.networkCapture.event") return { ok: false, ignored: true };
  return pushEvent(message.event || {}, sender || {});
}

export async function runNetworkTask(msg, runtime) {
  const payload = (msg && msg.payload) || {};
  const action = String(payload.action || payload.workflow_kind || "snapshot").trim().toLowerCase();
  if (action === "start" || action === "start_capture" || action === "network_capture_start") {
    return startCapture(payload, runtime);
  }
  if (action === "pause" || action === "pause_capture") return pauseCapture(true);
  if (action === "resume" || action === "resume_capture") return pauseCapture(false);
  if (action === "stop" || action === "stop_capture") return stopCapture();
  if (action === "clear" || action === "clear_capture") return clearCapture();
  if (action === "status") return getStatus();
  if (action === "snapshot" || action === "get_snapshot") return snapshot(payload);
  throw new Error(`unsupported network action: ${action}`);
}

try {
  chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    if (!capture.running || capture.paused) return;
    if (capture.tabId === null || Number(tabId) !== Number(capture.tabId)) return;
    const url = String((tab && tab.url) || changeInfo.url || "");
    if (url && !/^https?:\/\//i.test(url)) return;
    if (changeInfo.status === "complete" || changeInfo.url) {
      setTimeout(() => {
        if (capture.running && !capture.paused && Number(capture.tabId) === Number(tabId)) {
          injectCapture(tabId).catch((e) => {
            capture.lastError = `reinjection failed: ${String(e && e.message || e)}`;
          });
        }
      }, 300);
    }
  });
} catch (_) {}
