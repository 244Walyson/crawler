"""
Real-time web dashboard for the distributed crawler.
Run standalone: REDIS_URL=redis://localhost:6379 uv run python -m src.monitor.server
"""

import json
import os
import time
import asyncio
from collections import deque
from aiohttp import web
import redis.asyncio as aioredis

REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT          = int(os.getenv("MONITOR_PORT", "8080"))

# Load MAX_PAGES from .env file if env var not set
def _load_max_pages() -> int:
    val = os.getenv("MAX_PAGES")
    if val:
        return int(val)
    try:
        for line in open(".env"):
            line = line.strip()
            if line.startswith("MAX_PAGES="):
                return int(line.split("=", 1)[1])
    except Exception:
        pass
    return 50000

MAX_PAGES     = _load_max_pages()
HISTORY_LEN   = 120   # seconds of throughput history


async def get_logs(redis: aioredis.Redis) -> list:
    raw = await redis.lrange("crawler:logs", 0, 99)
    return [(r.decode() if isinstance(r, bytes) else r) for r in (raw or [])]


async def get_stats(redis: aioredis.Redis, app: web.Application) -> dict:
    async with redis.pipeline(transaction=False) as pipe:
        pipe.get("crawler:total")
        pipe.get("crawler:errors")
        pipe.get("crawler:busy")
        pipe.zcard("crawler:queue")
        pipe.scard("crawler:seen")
        pipe.get("crawler:start_time")
        pipe.lrange("crawler:activity", 0, 49)
        pipe.hgetall("crawler:workers")
        pipe.hgetall("crawler:error_types")
        results = await pipe.execute()

    total_raw, errors_raw, busy_raw, queue, seen, start_raw, activity_raw, workers_raw, error_types_raw = results

    total      = int(total_raw  or 0)
    errors     = int(errors_raw or 0)
    busy       = int(busy_raw   or 0)
    queue      = int(queue      or 0)
    seen       = int(seen       or 0)
    start_time = float(start_raw) if start_raw else None

    elapsed = (time.time() - start_time) if start_time else 0

    history: deque = app["history"]
    now = time.time()
    history.append((now, total))

    pages_per_sec = 0.0
    if len(history) >= 2:
        t0, c0 = history[0]
        t1, c1 = history[-1]
        dt = t1 - t0
        if dt > 0:
            pages_per_sec = (c1 - c0) / dt

    throughput_samples = app.get("throughput_samples", deque(maxlen=HISTORY_LEN))
    throughput_samples.append(round(pages_per_sec, 1))
    app["throughput_samples"] = throughput_samples

    remaining = max(0, MAX_PAGES - total)
    eta_seconds = int(remaining / pages_per_sec) if pages_per_sec > 0 else None

    requests_total = total + errors
    success_rate = (total  / requests_total * 100) if requests_total > 0 else 0.0
    error_rate   = (errors / requests_total * 100) if requests_total > 0 else 0.0

    activity = [
        (a.decode() if isinstance(a, bytes) else a)
        for a in (activity_raw or [])
    ]

    status_counts: dict[str, int] = {}
    if workers_raw:
        for v in workers_raw.values():
            s = v.decode() if isinstance(v, bytes) else v
            status_counts[s] = status_counts.get(s, 0) + 1

    error_types: dict[str, int] = {}
    if error_types_raw:
        for k, v in error_types_raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            error_types[key] = int(v)
    error_types = dict(sorted(error_types.items(), key=lambda x: -x[1])[:10])

    logs = await get_logs(redis)

    return {
        "total":              total,
        "errors":             errors,
        "busy":               busy,
        "queue":              queue,
        "seen":               seen,
        "elapsed":            int(elapsed),
        "pages_per_sec":      round(pages_per_sec, 1),
        "success_rate":       round(success_rate, 1),
        "error_rate":         round(error_rate, 1),
        "eta_seconds":        eta_seconds,
        "progress_pct":       round(min(total / MAX_PAGES * 100, 100), 2),
        "max_pages":          MAX_PAGES,
        "activity":           activity,
        "status_counts":      status_counts,
        "error_types":        error_types,
        "throughput_history": list(throughput_samples),
        "logs":               logs,
    }


async def sse_handler(request: web.Request) -> web.StreamResponse:
    redis: aioredis.Redis = request.app["redis"]
    resp = web.StreamResponse()
    resp.headers["Content-Type"]      = "text/event-stream"
    resp.headers["Cache-Control"]     = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    await resp.prepare(request)
    try:
        while True:
            stats = await get_stats(redis, request.app)
            await resp.write(f"data: {json.dumps(stats)}\n\n".encode())
            await asyncio.sleep(1)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    return resp


async def index_handler(request: web.Request) -> web.Response:
    html = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Crawler Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#0d0f18;--surface:#151822;--border:#232640;--text:#d4d8f0;--muted:#5a5f7a;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--blue:#3b82f6;--purple:#8b5cf6;--cyan:#06b6d4}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;font-size:13px;padding:16px}
header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;padding-bottom:12px;border-bottom:1px solid var(--border)}
header h1{font-size:1.1rem;letter-spacing:3px;color:var(--purple)}
.status-pill{display:flex;align-items:center;gap:8px;background:var(--surface);border:1px solid var(--border);padding:4px 12px;border-radius:20px;font-size:.75rem}
.pulse{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 1.2s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.8)}}
.metrics{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 12px}
.card-label{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.card-value{font-size:1.6rem;font-weight:bold;line-height:1}
.card-sub{font-size:.65rem;color:var(--muted);margin-top:4px}
.c-green{color:var(--green)}.c-red{color:var(--red)}.c-yellow{color:var(--yellow)}.c-blue{color:var(--blue)}.c-purple{color:var(--purple)}.c-cyan{color:var(--cyan)}
.progress-wrap{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 16px;margin-bottom:16px}
.progress-bar-bg{background:var(--border);border-radius:4px;height:8px;overflow:hidden}
.progress-bar-fill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--purple),var(--cyan));transition:width .8s ease}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px}
.panel-title{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.chart-wrap{position:relative;height:180px}
.activity-list{max-height:200px;overflow-y:auto}
.activity-item{font-size:.72rem;color:var(--muted);padding:4px 0;border-bottom:1px solid var(--border);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.activity-item.new{color:var(--cyan)}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid var(--border);font-size:.78rem}
.badge{background:var(--border);border-radius:4px;padding:2px 8px;font-size:.7rem;min-width:40px;text-align:right}
.badge-red{background:rgba(239,68,68,.2);color:var(--red)}.badge-green{background:rgba(34,197,94,.2);color:var(--green)}
.err-bar-label{display:flex;justify-content:space-between;font-size:.68rem;color:var(--muted);margin-bottom:2px}
.err-bar-bg{background:var(--border);border-radius:3px;height:5px;overflow:hidden;margin-bottom:8px}
.err-bar-fill{height:100%;border-radius:3px;background:var(--red);transition:width .8s}
footer{font-size:.65rem;color:var(--muted);text-align:right;margin-top:8px;padding-top:8px;border-bottom:1px solid var(--border)}
</style>
</head>
<body>
<header>
  <h1>⚡ CRAWLER MONITOR</h1>
  <div style="display:flex;gap:10px;align-items:center">
    <span style="font-size:.75rem;color:var(--muted)" id="elapsed-header">00:00:00</span>
    <div class="status-pill"><div class="pulse" id="status-dot"></div><span id="status-text">CONNECTING</span></div>
  </div>
</header>
<div class="metrics">
  <div class="card"><div class="card-label">Páginas Coletadas</div><div class="card-value c-green" id="m-total">0</div><div class="card-sub" id="m-pps">0 pág/s</div></div>
  <div class="card"><div class="card-label">Erros</div><div class="card-value c-red" id="m-errors">0</div><div class="card-sub" id="m-err-rate">0% taxa de erro</div></div>
  <div class="card"><div class="card-label">Taxa de Sucesso</div><div class="card-value c-green" id="m-success">—</div><div class="card-sub">das requisições</div></div>
  <div class="card"><div class="card-label">Workers Ativos</div><div class="card-value c-yellow" id="m-busy">0</div><div class="card-sub">processando agora</div></div>
  <div class="card"><div class="card-label">Fila</div><div class="card-value c-blue" id="m-queue">0</div><div class="card-sub">URLs pendentes</div></div>
  <div class="card"><div class="card-label">URLs Vistas</div><div class="card-value c-purple" id="m-seen">0</div><div class="card-sub">deduplicadas</div></div>
</div>
<div class="progress-wrap">
  <div style="display:flex;justify-content:space-between;font-size:.7rem;color:var(--muted);margin-bottom:8px">
    <span>Progresso até <span id="max-pages-label">50000</span> páginas</span><span id="pct-text">0%</span>
  </div>
  <div class="progress-bar-bg"><div class="progress-bar-fill" id="progress-fill" style="width:0%"></div></div>
  <div style="display:flex;justify-content:space-between;font-size:.7rem;color:var(--muted);margin-top:6px">
    <span id="pgs-done">0 coletadas</span><span id="eta-text">ETA: calculando...</span>
  </div>
</div>
<div class="grid-2">
  <div class="panel">
    <div class="panel-title">Throughput — Páginas/segundo</div>
    <div class="chart-wrap"><canvas id="throughputChart"></canvas></div>
  </div>
  <div class="panel">
    <div class="panel-title">Erros por Tipo</div>
    <div id="error-bars"></div>
  </div>
</div>
<div class="grid-2">
  <div class="panel"><div class="panel-title">Atividade Recente</div><div class="activity-list" id="activity"></div></div>
  <div class="panel">
    <div class="panel-title">Status dos Workers</div><div id="worker-status"></div>
    <div style="margin-top:14px"><div class="panel-title">Resumo de Requisições</div><div id="req-summary"></div></div>
  </div>
</div>
<div class="panel" style="margin-bottom:16px">
  <div class="panel-title" style="display:flex;justify-content:space-between;align-items:center">
    <span>⬛ Terminal — Logs em Tempo Real</span>
    <div style="display:flex;gap:8px;align-items:center">
      <input id="log-filter" type="text" placeholder="filtrar..." style="background:var(--bg);border:1px solid var(--border);color:var(--text);padding:2px 8px;border-radius:4px;font-family:monospace;font-size:.7rem;width:140px">
      <label style="font-size:.7rem;color:var(--muted);cursor:pointer"><input type="checkbox" id="log-scroll" checked style="margin-right:4px">auto-scroll</label>
      <button onclick="document.getElementById('terminal').innerHTML=''" style="background:transparent;border:1px solid var(--border);color:var(--muted);padding:2px 8px;border-radius:4px;cursor:pointer;font-size:.7rem">limpar</button>
    </div>
  </div>
  <div id="terminal" style="background:#030507;border-radius:6px;padding:10px;height:280px;overflow-y:auto;font-family:'Courier New',monospace;font-size:.72rem;line-height:1.6;border:1px solid #111;margin-top:4px">
    <span style="color:#3a4a3a">Aguardando logs...</span>
  </div>
</div>
<footer id="last-updated">Conectando...</footer>
<script>
const MAX_PAGES=""" + str(MAX_PAGES) + r""";
const ctx=document.getElementById("throughputChart").getContext("2d");
const throughputChart=new Chart(ctx,{type:"line",data:{labels:[],datasets:[{data:[],borderColor:"#8b5cf6",backgroundColor:"rgba(139,92,246,.1)",borderWidth:2,pointRadius:0,fill:true,tension:0.4}]},options:{animation:false,responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{grid:{color:"#232640"},ticks:{color:"#5a5f7a",font:{size:10}},beginAtZero:true}}}});
function fmt(n){if(n>=1e6)return(n/1e6).toFixed(2)+"M";if(n>=1e3)return(n/1e3).toFixed(1)+"k";return n.toLocaleString()}
function fmtTime(s){if(s===null||s===undefined)return"—";const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;return[h,m,ss].map(v=>String(v).padStart(2,"0")).join(":")}
const COLORS=["#ef4444","#f59e0b","#3b82f6","#8b5cf6","#06b6d4","#22c55e","#ec4899","#14b8a6","#f97316","#a855f7"];
let prevActivity=[],prevLogs=[];
const evtSource=new EventSource("/stream");
evtSource.onmessage=(e)=>{
  const d=JSON.parse(e.data);
  document.getElementById("status-dot").style.background=d.total>0?"#22c55e":"#f59e0b";
  document.getElementById("status-text").textContent=d.busy>0?"RUNNING":(d.total>0?"IDLE":"STARTING");
  document.getElementById("elapsed-header").textContent=fmtTime(d.elapsed);
  document.getElementById("m-total").textContent=fmt(d.total);
  document.getElementById("m-errors").textContent=fmt(d.errors);
  document.getElementById("m-busy").textContent=d.busy;
  document.getElementById("m-queue").textContent=fmt(d.queue);
  document.getElementById("m-seen").textContent=fmt(d.seen);
  document.getElementById("m-pps").textContent=d.pages_per_sec+" pág/s";
  document.getElementById("m-err-rate").textContent=d.error_rate+"% taxa de erro";
  document.getElementById("m-success").textContent=d.success_rate+"%";
  document.getElementById("pct-text").textContent=d.progress_pct+"%";
  document.getElementById("progress-fill").style.width=d.progress_pct+"%";
  document.getElementById("pgs-done").textContent=fmt(d.total)+" coletadas";
  document.getElementById("max-pages-label").textContent=fmt(d.max_pages);
  document.getElementById("eta-text").textContent=d.eta_seconds!==null?"ETA: "+fmtTime(d.eta_seconds):"ETA: calculando...";
  throughputChart.data.labels=d.throughput_history.map((_,i)=>i);
  throughputChart.data.datasets[0].data=d.throughput_history;
  throughputChart.update("none");
  const errEntries=Object.entries(d.error_types);
  const maxErr=errEntries.length>0?Math.max(...errEntries.map(x=>x[1])):1;
  const errC=document.getElementById("error-bars");
  errC.innerHTML=errEntries.length===0?'<div style="color:var(--muted);font-size:.78rem;padding:8px 0">Nenhum erro registrado</div>':errEntries.map(([t,c],i)=>`<div class="err-bar-label"><span style="color:${COLORS[i%COLORS.length]}">${t}</span><span>${c}</span></div><div class="err-bar-bg"><div class="err-bar-fill" style="width:${(c/maxErr*100).toFixed(1)}%;background:${COLORS[i%COLORS.length]}"></div></div>`).join("");
  const actEl=document.getElementById("activity");
  const newUrls=d.activity.filter(a=>!prevActivity.includes(a));
  prevActivity=d.activity;
  actEl.innerHTML=d.activity.map(item=>`<div class="activity-item${newUrls.includes(item)?" new":""}">${item}</div>`).join("");
  const wkEl=document.getElementById("worker-status");
  const entries=Object.entries(d.status_counts);
  wkEl.innerHTML=entries.length===0?'<div style="color:var(--muted)">Nenhum worker ativo</div>':entries.map(([s,c])=>`<div class="stat-row"><span>${s}</span><span class="badge">${c}</span></div>`).join("");
  const rt=d.total+d.errors;
  document.getElementById("req-summary").innerHTML=`<div class="stat-row"><span>Total</span><span class="badge">${fmt(rt)}</span></div><div class="stat-row"><span>Sucessos</span><span class="badge badge-green">${fmt(d.total)}</span></div><div class="stat-row"><span>Falhas</span><span class="badge badge-red">${fmt(d.errors)}</span></div><div class="stat-row"><span>Tempo</span><span class="badge">${fmtTime(d.elapsed)}</span></div>`;
  document.getElementById("last-updated").textContent="Atualizado: "+new Date().toLocaleTimeString();
  if(d.logs)updateTerminal(d.logs);
};
evtSource.onerror=()=>{document.getElementById("status-text").textContent="RECONECTANDO";document.getElementById("status-dot").style.background="#ef4444"};
const terminal=document.getElementById("terminal");
const filterInput=document.getElementById("log-filter");
const autoScroll=document.getElementById("log-scroll");
const LEVEL_COLOR={info:"#22c55e",warning:"#f59e0b",error:"#ef4444",debug:"#5a5f7a"};
function renderLine(entry){const parts=entry.split("|");const ts=parts[0]||"";const level=(parts[1]||"info").toLowerCase();const msg=parts.slice(2).join("|");const lvlColor=LEVEL_COLOR[level]||"#a0aec0";return`<div style="display:flex;gap:8px;padding:1px 0"><span style="color:#2d4a2d;min-width:60px">${ts}</span><span style="color:${lvlColor};min-width:50px;text-transform:uppercase;font-size:.65rem;padding-top:2px">${level}</span><span style="color:#d4d8f0;word-break:break-all">${msg}</span></div>`}
function updateTerminal(logs){const filter=filterInput.value.toLowerCase();const newLogs=logs.filter(l=>!prevLogs.includes(l));prevLogs=logs;if(newLogs.length===0)return;if(terminal.innerHTML.includes("Aguardando"))terminal.innerHTML="";newLogs.reverse().forEach(entry=>{if(filter&&!entry.toLowerCase().includes(filter))return;terminal.insertAdjacentHTML("beforeend",renderLine(entry))});while(terminal.children.length>300)terminal.removeChild(terminal.children[0]);if(autoScroll.checked)terminal.scrollTop=terminal.scrollHeight}
filterInput.addEventListener("input",()=>{terminal.innerHTML="";prevLogs.forEach(entry=>{const f=filterInput.value.toLowerCase();if(!f||entry.toLowerCase().includes(f))terminal.insertAdjacentHTML("beforeend",renderLine(entry))});if(autoScroll.checked)terminal.scrollTop=terminal.scrollHeight});
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def on_startup(app: web.Application) -> None:
    app["redis"]             = aioredis.from_url(REDIS_URL, decode_responses=False)
    app["history"]           = deque(maxlen=HISTORY_LEN)
    app["throughput_samples"] = deque(maxlen=HISTORY_LEN)


async def on_cleanup(app: web.Application) -> None:
    await app["redis"].aclose()


def make_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/",       index_handler)
    app.router.add_get("/stream", sse_handler)
    return app


if __name__ == "__main__":
    app = make_app()
    web.run_app(app, host="0.0.0.0", port=PORT, print=lambda *a: None)
    print(f"Monitor running at http://localhost:{PORT}")
