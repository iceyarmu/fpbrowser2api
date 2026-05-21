export async function ensureTab(urlPrefix, targetUrl, options = {}) {
  const active = options.active !== false;
  const navigate = options.navigate !== false;
  const tabs = await chrome.tabs.query({});
  const exact = targetUrl ? tabs.find(t => (t.url || "") === targetUrl) : null;
  const found = exact || tabs.find(t => (t.url || "").startsWith(urlPrefix));
  if (found && found.id) {
    if (navigate && targetUrl && found.url !== targetUrl) {
      await chrome.tabs.update(found.id, { url: targetUrl, active });
    } else {
      await chrome.tabs.update(found.id, { active });
    }
    return found.id;
  }
  const tab = await chrome.tabs.create({ url: targetUrl || urlPrefix, active });
  return tab.id;
}

export async function fetchJson(url, { method = "GET", headers = {}, body = null } = {}) {
  const reqMethod = String(method || "GET").toUpperCase();
  const init = { method: reqMethod, headers: { ...headers }, credentials: "include" };
  if (body !== null && body !== undefined) init.body = JSON.stringify(body);
  let resp;
  try {
    resp = await fetch(url, init);
  } catch (e) {
    const rawMsg = String((e && e.message) || e || "unknown error");
    throw new Error(`fetchJson failed; Request Method: ${reqMethod}; url=${url}; error=${rawMsg}`);
  }
  const text = await resp.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) {}
  return { status: resp.status, headers: Object.fromEntries(resp.headers.entries()), text, json };
}

export function compactErrorResponse(tx) {
  if (!tx) return "";
  return `${tx.status || ""} ${String(tx.text || JSON.stringify(tx.json || "")).slice(0, 500)}`;
}
