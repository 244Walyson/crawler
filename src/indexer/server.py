"""Web search interface for the BM25 inverted index."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from aiohttp import web

from src.config.settings import settings
from src.indexer.search import search

PORT = 8888


async def api_search(request: web.Request) -> web.Response:
    q = request.rel_url.query.get("q", "").strip()
    mode = request.rel_url.query.get("mode", "and")
    k = min(int(request.rel_url.query.get("k", "10")), 50)
    redis_url = request.app["redis_url"]

    if not q:
        return web.json_response({"results": [], "mode": mode, "query": q})

    try:
        results, actual_mode = await search(q, redis_url, k, mode=mode)
    except Exception as exc:
        return web.json_response({"error": str(exc)}, status=500)

    # Sanitise for JSON
    payload = []
    for r in results:
        payload.append({
            "chunk_id": r.get("chunk_id", ""),
            "score":    round(float(r.get("score", 0)), 4),
            "lang":     r.get("lang", "?"),
            "url":      r.get("doc_url", r.get("url", "")),
            "snippet":  (r.get("raw") or r.get("text") or "")[:300],
        })

    return web.json_response({"results": payload, "mode": actual_mode, "query": q})


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
  --red:     #ef4444;
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
.hero {
  text-align: center; padding: 48px 0 32px;
}
.hero h1 {
  font-size: 2rem; letter-spacing: 4px; color: var(--purple);
  margin-bottom: 8px;
}
.hero p { color: var(--muted); font-size: .85rem; }

/* ── SEARCH BAR ── */
.search-wrap {
  max-width: 720px; margin: 0 auto 32px;
}
.search-row {
  display: flex; gap: 8px; margin-bottom: 10px;
}
#q {
  flex: 1; background: var(--surface); border: 1px solid var(--border);
  border-radius: 8px; color: var(--text); font-family: inherit;
  font-size: 1rem; padding: 12px 16px; outline: none;
  transition: border-color .2s;
}
#q:focus { border-color: var(--purple); }
#q::placeholder { color: var(--muted); }
button#btn {
  background: var(--purple); border: none; border-radius: 8px;
  color: #fff; cursor: pointer; font-family: inherit; font-size: .9rem;
  padding: 12px 24px; transition: background .2s;
}
button#btn:hover { background: #7c3aed; }
button#btn:disabled { background: var(--muted); cursor: default; }

.options {
  display: flex; gap: 16px; align-items: center; flex-wrap: wrap;
}
.option-group { display: flex; align-items: center; gap: 8px; }
label { font-size: .78rem; color: var(--muted); }
select, input[type=number] {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 6px; color: var(--text); font-family: inherit;
  font-size: .78rem; padding: 4px 8px; outline: none;
}

/* ── STATUS ── */
#status {
  max-width: 720px; margin: 0 auto 16px;
  font-size: .78rem; color: var(--muted); min-height: 20px;
}

/* ── RESULTS ── */
#results { max-width: 720px; margin: 0 auto; }

.result-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px 18px; margin-bottom: 12px;
  transition: border-color .2s;
}
.result-card:hover { border-color: var(--purple); }

.result-header {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  margin-bottom: 6px;
}
.rank {
  color: var(--green); font-weight: bold; font-size: 1rem; min-width: 28px;
}
.score {
  background: rgba(245,158,11,.15); color: var(--yellow);
  border-radius: 4px; padding: 2px 8px; font-size: .72rem;
}
.lang-badge {
  background: rgba(6,182,212,.15); color: var(--cyan);
  border-radius: 4px; padding: 2px 8px; font-size: .72rem;
}
.result-url {
  color: var(--blue); font-size: .8rem; text-decoration: none;
  word-break: break-all;
}
.result-url:hover { text-decoration: underline; }
.snippet {
  color: var(--muted); font-size: .8rem; line-height: 1.6;
  margin-top: 6px; white-space: pre-wrap; word-break: break-word;
}

/* ── SPINNER ── */
.spinner {
  display: none; width: 20px; height: 20px; margin: 24px auto;
  border: 2px solid var(--border); border-top-color: var(--purple);
  border-radius: 50%; animation: spin .7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ── EMPTY ── */
.empty {
  text-align: center; color: var(--muted); padding: 48px 0;
  display: none;
}
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
    <input id="q" type="text" placeholder="basketball odds, futebol gol, tennis match..."
           autofocus autocomplete="off" spellcheck="false">
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
const results  = document.getElementById('results');
const empty    = document.getElementById('empty');

function hl(text, terms) {
  // Simple highlight: wrap query stems in a span (case-insensitive, first 3 terms)
  let out = text;
  terms.slice(0, 3).forEach(t => {
    if (!t) return;
    const re = new RegExp(`(${t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    out = out.replace(re, '<mark style="background:rgba(139,92,246,.3);color:#d4d8f0;border-radius:2px;padding:0 2px">$1</mark>');
  });
  return out;
}

async function doSearch() {
  const q    = qEl.value.trim();
  const mode = modeEl.value;
  const k    = Math.min(Math.max(parseInt(kEl.value) || 10, 1), 50);
  if (!q) return;

  btn.disabled = true;
  spinner.style.display = 'block';
  statusEl.textContent  = 'Buscando…';
  empty.style.display   = 'none';
  // clear previous results (keep empty div)
  [...results.children].forEach(c => { if (c !== empty) c.remove(); });

  const t0 = Date.now();
  try {
    const res  = await fetch(`/search?q=${encodeURIComponent(q)}&mode=${mode}&k=${k}`);
    const data = await res.json();
    const ms   = Date.now() - t0;

    if (data.error) {
      statusEl.textContent = '⚠ ' + data.error;
      return;
    }

    const n = data.results.length;
    const modeLabel = data.mode.toUpperCase();
    statusEl.innerHTML =
      `<b>${n}</b> resultado${n !== 1 ? 's' : ''}` +
      ` · modo <b>${modeLabel}</b>` +
      ` · ${ms} ms`;

    if (n === 0) { empty.style.display = 'block'; return; }

    const terms = q.toLowerCase().split(/\s+/);

    data.results.forEach((r, i) => {
      const card = document.createElement('div');
      card.className = 'result-card';
      card.innerHTML = `
        <div class="result-header">
          <span class="rank">#${i + 1}</span>
          <span class="score">score ${r.score.toFixed(3)}</span>
          <span class="lang-badge">${r.lang}</span>
          <a class="result-url" href="${r.url}" target="_blank" rel="noopener">${r.url}</a>
        </div>
        ${r.snippet ? `<div class="snippet">${hl(r.snippet.replace(/</g,'&lt;'), terms)}</div>` : ''}
      `;
      results.appendChild(card);
    });

  } catch (e) {
    statusEl.textContent = '⚠ Erro: ' + e.message;
  } finally {
    btn.disabled = false;
    spinner.style.display = 'none';
  }
}

btn.addEventListener('click', doSearch);
qEl.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });
</script>
</body>
</html>
"""


async def on_startup(app: web.Application) -> None:
    pass


def make_app(redis_url: str) -> web.Application:
    app = web.Application()
    app["redis_url"] = redis_url
    app.router.add_get("/",       index_handler)
    app.router.add_get("/search", api_search)
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
