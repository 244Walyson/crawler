"""Web search interface for the BM25 inverted index."""
from __future__ import annotations

import argparse
import asyncio
import bisect
import sys

from aiohttp import web
from redis.asyncio import Redis

from src.config.settings import settings
from src.indexer.search import search

PORT = 8888


async def _build_vocab(redis: Redis) -> list[str]:
    """Scan all idx:term:* keys and return a sorted list of stems."""
    terms: list[str] = []
    cursor = 0
    prefix_len = len("idx:term:")
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="idx:term:*", count=5000)
        for k in keys:
            terms.append(k[prefix_len:])
        if cursor == 0:
            break
    terms.sort()
    return terms


async def api_suggest(request: web.Request) -> web.Response:
    prefix = request.rel_url.query.get("q", "").strip().lower()
    vocab: list[str] = request.app["vocab"]
    if not prefix or not vocab:
        return web.json_response({"suggestions": []})

    lo = bisect.bisect_left(vocab, prefix)
    suggestions: list[str] = []
    for term in vocab[lo : lo + 80]:
        if not term.startswith(prefix):
            break
        suggestions.append(term)
        if len(suggestions) >= 8:
            break

    return web.json_response({"suggestions": suggestions})


async def api_search(request: web.Request) -> web.Response:
    q = request.rel_url.query.get("q", "").strip()
    mode = request.rel_url.query.get("mode", "and")
    k = min(int(request.rel_url.query.get("k", "10")), 50)
    redis_url = request.app["redis_url"]

    if not q:
        return web.json_response({"results": [], "mode": mode, "query": q, "highlight": []})

    try:
        results, actual_mode, highlight = await search(q, redis_url, k, mode=mode)
    except Exception as exc:
        return web.json_response({"error": str(exc)}, status=500)

    payload = []
    for r in results:
        payload.append({
            "chunk_id": r.get("chunk_id", ""),
            "score":    round(float(r.get("score", 0)), 4),
            "lang":     r.get("lang", "?"),
            "url":      r.get("doc_url", r.get("url", "")),
            "snippet":  (r.get("raw") or r.get("text") or "")[:300],
        })

    return web.json_response({
        "results":   payload,
        "mode":      actual_mode,
        "query":     q,
        "highlight": highlight,
    })


async def index_handler(_: web.Request) -> web.Response:
    return web.Response(text=_HTML, content_type="text/html")


_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sports Odds Search</title>
<style>
:root {
  --bg:      #0d0f18;
  --surface: #151822;
  --border:  #232640;
  --text:    #d4d8f0;
  --muted:   #5a5f7a;
  --green:   #22c55e;
  --yellow:  #f59e0b;
  --blue:    #3b82f6;
  --purple:  #8b5cf6;
  --cyan:    #06b6d4;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Courier New', monospace; font-size: 14px;
  min-height: 100vh; padding: 0 16px 40px;
}

/* ── HERO ── */
.hero { text-align: center; padding: 48px 0 32px; }
.hero h1 { font-size: 2rem; letter-spacing: 4px; color: var(--purple); margin-bottom: 8px; }
.hero p { color: var(--muted); font-size: .85rem; }

/* ── SEARCH BAR ── */
.search-wrap { max-width: 720px; margin: 0 auto 32px; }
.search-row  { display: flex; gap: 8px; margin-bottom: 10px; }

.input-wrapper { position: relative; flex: 1; }

#q {
  width: 100%;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 8px; color: var(--text); font-family: inherit;
  font-size: 1rem; padding: 12px 16px; outline: none;
  transition: border-color .2s;
}
#q:focus { border-color: var(--purple); }
#q.open  { border-radius: 8px 8px 0 0; border-bottom-color: var(--border); }
#q::placeholder { color: var(--muted); }

button#btn {
  background: var(--purple); border: none; border-radius: 8px;
  color: #fff; cursor: pointer; font-family: inherit; font-size: .9rem;
  padding: 12px 24px; transition: background .2s; white-space: nowrap;
}
button#btn:hover    { background: #7c3aed; }
button#btn:disabled { background: var(--muted); cursor: default; }

/* ── AUTOCOMPLETE ── */
#suggestions {
  display: none;
  position: absolute; top: 100%; left: 0; right: 0; z-index: 200;
  background: var(--surface);
  border: 1px solid var(--purple);
  border-top: none;
  border-radius: 0 0 8px 8px;
  overflow: hidden;
}
.sugg-item {
  padding: 10px 16px; cursor: pointer; font-size: .9rem;
  display: flex; align-items: center; gap: 8px;
  color: var(--text); transition: background .1s;
}
.sugg-item::before { content: '⌕'; color: var(--muted); font-size: .8rem; }
.sugg-item:hover,
.sugg-item.active { background: rgba(139,92,246,.18); color: var(--purple); }
.sugg-item b { color: var(--purple); font-weight: bold; }

/* ── OPTIONS ── */
.options { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
.option-group { display: flex; align-items: center; gap: 8px; }
label { font-size: .78rem; color: var(--muted); }
select, input[type=number] {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 6px; color: var(--text); font-family: inherit;
  font-size: .78rem; padding: 4px 8px; outline: none;
}

/* ── STATUS ── */
#status { max-width: 720px; margin: 0 auto 16px; font-size: .78rem; color: var(--muted); min-height: 20px; }

/* ── RESULTS ── */
#results { max-width: 720px; margin: 0 auto; }
.result-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px 18px; margin-bottom: 12px;
  transition: border-color .2s;
}
.result-card:hover { border-color: var(--purple); }
.result-header {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 6px;
}
.rank  { color: var(--green); font-weight: bold; font-size: 1rem; min-width: 28px; }
.score { background: rgba(245,158,11,.15); color: var(--yellow); border-radius: 4px; padding: 2px 8px; font-size: .72rem; }
.lang-badge { background: rgba(6,182,212,.15); color: var(--cyan); border-radius: 4px; padding: 2px 8px; font-size: .72rem; }
.result-url { color: var(--blue); font-size: .8rem; text-decoration: none; word-break: break-all; }
.result-url:hover { text-decoration: underline; }
.snippet { color: var(--muted); font-size: .8rem; line-height: 1.6; margin-top: 6px; white-space: pre-wrap; word-break: break-word; }
mark { background: rgba(139,92,246,.3); color: var(--text); border-radius: 2px; padding: 0 2px; }

/* ── SPINNER ── */
.spinner {
  display: none; width: 20px; height: 20px; margin: 24px auto;
  border: 2px solid var(--border); border-top-color: var(--purple);
  border-radius: 50%; animation: spin .7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ── EMPTY ── */
.empty { text-align: center; color: var(--muted); padding: 48px 0; display: none; }
.empty span { font-size: 2rem; display: block; margin-bottom: 8px; }
</style>
</head>
<body>

<div class="hero">
  <h1>⚡ SPORTS ODDS SEARCH</h1>
  <p>BM25 · Inverted Index · Redis · NLTK stemming</p>
</div>

<div class="search-wrap">
  <div class="search-row">
    <div class="input-wrapper">
      <input id="q" type="text" placeholder="basketball odds, futebol gol, tennis match..."
             autofocus autocomplete="off" spellcheck="false">
      <div id="suggestions"></div>
    </div>
    <button id="btn">Buscar</button>
  </div>
  <div class="options">
    <div class="option-group">
      <label for="mode">Modo</label>
      <select id="mode">
        <option value="and">AND — todos os termos</option>
        <option value="or">OR  — algum termo</option>
      </select>
    </div>
    <div class="option-group">
      <label for="k">Resultados</label>
      <input id="k" type="number" value="10" min="1" max="50" style="width:56px">
    </div>
  </div>
</div>

<div id="status"></div>
<div class="spinner" id="spinner"></div>

<div id="results">
  <div class="empty" id="empty">
    <span>🔍</span>
    Nenhum resultado encontrado.
  </div>
</div>

<script>
const qEl      = document.getElementById('q');
const btn      = document.getElementById('btn');
const modeEl   = document.getElementById('mode');
const kEl      = document.getElementById('k');
const statusEl = document.getElementById('status');
const spinner  = document.getElementById('spinner');
const resultsEl= document.getElementById('results');
const empty    = document.getElementById('empty');
const suggEl   = document.getElementById('suggestions');

/* ── Highlight: only content words returned by backend ── */
function hl(text, terms) {
  if (!terms || !terms.length) return text;
  const escaped = terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const re = new RegExp(`(${escaped.join('|')})`, 'gi');
  return text.replace(re, '<mark>$1</mark>');
}

/* ── Autocomplete ── */
let suggTimer  = null;
let activeIdx  = -1;

function openSugg(items, prefix) {
  if (!items.length) { closeSugg(); return; }
  const escaped = prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`^(${escaped})`, 'i');
  suggEl.innerHTML = items.map(s =>
    `<div class="sugg-item" data-val="${s}">${s.replace(re, '<b>$1</b>')}</div>`
  ).join('');
  suggEl.style.display = 'block';
  qEl.classList.add('open');
  activeIdx = -1;

  suggEl.querySelectorAll('.sugg-item').forEach(el => {
    el.addEventListener('mousedown', e => {
      e.preventDefault();
      qEl.value = el.dataset.val;
      closeSugg();
      doSearch();
    });
  });
}

function closeSugg() {
  suggEl.style.display = 'none';
  qEl.classList.remove('open');
  activeIdx = -1;
}

async function fetchSuggestions() {
  const q = qEl.value.trim();
  if (q.length < 2) { closeSugg(); return; }
  const last = q.split(/\s+/).pop();
  if (last.length < 2) { closeSugg(); return; }
  try {
    const res  = await fetch(`/suggest?q=${encodeURIComponent(last)}`);
    const data = await res.json();
    openSugg(data.suggestions || [], last);
  } catch (_) { closeSugg(); }
}

qEl.addEventListener('input', () => {
  clearTimeout(suggTimer);
  suggTimer = setTimeout(fetchSuggestions, 160);
});

qEl.addEventListener('keydown', e => {
  const items = [...suggEl.querySelectorAll('.sugg-item')];
  const open  = suggEl.style.display !== 'none';

  if (e.key === 'ArrowDown' && open) {
    e.preventDefault();
    activeIdx = Math.min(activeIdx + 1, items.length - 1);
    items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
    if (activeIdx >= 0) qEl.value = items[activeIdx].dataset.val;
    return;
  }
  if (e.key === 'ArrowUp' && open) {
    e.preventDefault();
    activeIdx = Math.max(activeIdx - 1, -1);
    items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
    return;
  }
  if (e.key === 'Escape') { closeSugg(); return; }
  if (e.key === 'Enter') {
    if (open && activeIdx >= 0) {
      qEl.value = items[activeIdx].dataset.val;
      closeSugg();
    }
    doSearch();
    return;
  }
});

document.addEventListener('click', e => {
  if (!qEl.contains(e.target) && !suggEl.contains(e.target)) closeSugg();
});

/* ── Search ── */
async function doSearch() {
  const q    = qEl.value.trim();
  const mode = modeEl.value;
  const k    = Math.min(Math.max(parseInt(kEl.value) || 10, 1), 50);
  if (!q) return;
  closeSugg();

  btn.disabled = true;
  spinner.style.display = 'block';
  statusEl.textContent  = 'Buscando…';
  empty.style.display   = 'none';
  [...resultsEl.children].forEach(c => { if (c !== empty) c.remove(); });

  const t0 = Date.now();
  try {
    const res  = await fetch(`/search?q=${encodeURIComponent(q)}&mode=${mode}&k=${k}`);
    const data = await res.json();
    const ms   = Date.now() - t0;

    if (data.error) { statusEl.textContent = '⚠ ' + data.error; return; }

    const n = data.results.length;
    statusEl.innerHTML =
      `<b>${n}</b> resultado${n !== 1 ? 's' : ''}` +
      ` · modo <b>${data.mode.toUpperCase()}</b>` +
      ` · ${ms} ms` +
      (data.highlight && data.highlight.length
        ? ` · termos: <span style="color:var(--cyan)">${data.highlight.join(', ')}</span>`
        : '');

    if (n === 0) { empty.style.display = 'block'; return; }

    const terms = data.highlight || [];

    data.results.forEach((r, i) => {
      const card = document.createElement('div');
      card.className = 'result-card';
      const safe = r.snippet.replace(/</g, '&lt;');
      card.innerHTML = `
        <div class="result-header">
          <span class="rank">#${i + 1}</span>
          <span class="score">score ${r.score.toFixed(3)}</span>
          <span class="lang-badge">${r.lang}</span>
          <a class="result-url" href="${r.url}" target="_blank" rel="noopener">${r.url}</a>
        </div>
        ${r.snippet ? `<div class="snippet">${hl(safe, terms)}</div>` : ''}
      `;
      resultsEl.appendChild(card);
    });

  } catch (e) {
    statusEl.textContent = '⚠ Erro: ' + e.message;
  } finally {
    btn.disabled = false;
    spinner.style.display = 'none';
  }
}

btn.addEventListener('click', doSearch);
</script>
</body>
</html>
"""


async def on_startup(app: web.Application) -> None:
    redis = Redis.from_url(app["redis_url"], decode_responses=True)
    try:
        app["vocab"] = await _build_vocab(redis)
    finally:
        await redis.aclose()


def make_app(redis_url: str) -> web.Application:
    app = web.Application()
    app["redis_url"] = redis_url
    app["vocab"] = []
    app.on_startup.append(on_startup)
    app.router.add_get("/",       index_handler)
    app.router.add_get("/search", api_search)
    app.router.add_get("/suggest", api_suggest)
    return app


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Web search interface for the inverted index")
    p.add_argument("--port",      type=int, default=PORT)
    p.add_argument("--host",      type=str, default="0.0.0.0")
    p.add_argument("--redis-url", type=str, default=settings.REDIS_URL)
    args = p.parse_args(argv)

    app = make_app(args.redis_url)
    print(f"Search UI → http://localhost:{args.port}")
    web.run_app(app, host=args.host, port=args.port, print=lambda *_: None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
