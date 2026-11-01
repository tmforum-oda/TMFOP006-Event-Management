"""
Trap Receiver - Dashboard Blueprint
Adds 3 new routes, zero touch to existing app.py logic:
  GET /dashboard             -> HTML UI (browser)
  GET /api/v1/events         -> JSON events list
  GET /api/v1/events/summary -> JSON counts per action
"""
from flask import Blueprint, jsonify, request, render_template_string
from . import store

dashboard_bp = Blueprint("dashboard", __name__)

# ── HTML head section ──────────────────────────────────────────────────────────
_H = (
    "<!DOCTYPE html>"
    '<html lang="en">'
    "<head>"
    '<meta charset="UTF-8"/>'
    '<meta name="viewport" content="width=device-width,initial-scale=1.0"/>'
    "<title>Trap Receiver - Alarm Dashboard</title>"
    "<style>"
    "*{box-sizing:border-box;margin:0;padding:0}"
    "body{font-family:'Segoe UI',Arial,sans-serif;background:#0f1117;color:#e0e0e0}"
    ".header{background:linear-gradient(135deg,#1a1f2e,#0d1117);border-bottom:2px solid #2a9d8f;"
    "padding:18px 32px;display:flex;align-items:center;justify-content:space-between}"
    ".header h1{font-size:1.4rem;color:#2a9d8f;letter-spacing:1px}"
    ".header .sub{font-size:.78rem;color:#888;margin-top:3px}"
    ".live-dot{width:10px;height:10px;border-radius:50%;background:#2a9d8f;"
    "display:inline-block;animation:pulse 1.5s infinite;margin-right:6px}"
    "@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}"
    ".btn{background:#2a9d8f;color:#fff;border:none;padding:7px 18px;border-radius:6px;"
    "cursor:pointer;font-size:.85rem}"
    ".btn:hover{background:#21867a}"
    ".cards{display:flex;gap:16px;padding:24px 32px 8px;flex-wrap:wrap}"
    ".card{flex:1;min-width:160px;background:#1a1f2e;border-radius:10px;padding:20px 24px;"
    "border-left:4px solid #2a9d8f;box-shadow:0 2px 8px rgba(0,0,0,.4)}"
    ".card.create{border-color:#e76f51}.card.clear{border-color:#2a9d8f}.card.total{border-color:#e9c46a}"
    ".card .num{font-size:2.2rem;font-weight:700;color:#fff}"
    ".card .lbl{font-size:.78rem;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:1px}"
    ".card.create .num{color:#e76f51}.card.clear .num{color:#2a9d8f}.card.total .num{color:#e9c46a}"
    ".bar{display:flex;gap:10px;padding:16px 32px;align-items:center;flex-wrap:wrap}"
    ".bar label{font-size:.82rem;color:#888}"
    ".bar select,.bar input{background:#1a1f2e;color:#e0e0e0;border:1px solid #2a9d8f44;"
    "padding:6px 12px;border-radius:6px;font-size:.85rem;outline:none}"
    ".badge{padding:3px 10px;border-radius:12px;font-size:.75rem;font-weight:600;text-transform:uppercase}"
    ".badge.create{background:#e76f5122;color:#e76f51;border:1px solid #e76f5155}"
    ".badge.clear{background:#2a9d8f22;color:#2a9d8f;border:1px solid #2a9d8f55}"
    ".wrap{padding:0 32px 32px;overflow-x:auto}"
    "table{width:100%;border-collapse:collapse;font-size:.84rem}"
    "thead tr{background:#1a1f2e}"
    "thead th{padding:12px 14px;text-align:left;color:#2a9d8f;font-weight:600;"
    "border-bottom:1px solid #2a9d8f33;white-space:nowrap}"
    "tbody tr{border-bottom:1px solid #ffffff08}"
    "tbody tr:hover{background:#1a1f2e99}"
    "tbody td{padding:11px 14px;vertical-align:middle}"
    ".mono{font-family:monospace;font-size:.82rem;color:#a0a0b0}"
    ".empty{text-align:center;padding:60px;color:#555}"
    ".footer{text-align:center;padding:12px;color:#444;font-size:.75rem}"
    "</style>"
    "</head>"
)

# ── HTML body + JS ────────────────────────────────────────────────────────────
_B = (
    "<body>"
    '<div class="header">'
    '<div><h1><span class="live-dot"></span> Trap Receiver &#8212; Alarm Dashboard</h1>'
    '<div class="sub">AWS EKS &#183; ap-south-1 &#183; ihpp-canvas-dev-01 &#183; trapreceiver</div></div>'
    '<button class="btn" onclick="loadData()">&#x21BA; Refresh</button>'
    "</div>"
    '<div class="cards">'
    '<div class="card total"><div class="num" id="c-total">0</div><div class="lbl">Total Events</div></div>'
    '<div class="card create"><div class="num" id="c-create">0</div><div class="lbl">Alarm Create</div></div>'
    '<div class="card clear"><div class="num" id="c-clear">0</div><div class="lbl">Alarm Clear</div></div>'
    '<div class="card"><div class="num" id="c-last" style="font-size:1rem;margin-top:6px">&#8212;</div>'
    '<div class="lbl">Last Event</div></div>'
    "</div>"
    '<div class="bar">'
    "<label>Action:</label>"
    '<select id="f-action" onchange="applyFilter()">'
    '<option value="">All</option><option value="create">Create</option><option value="clear">Clear</option>'
    "</select>"
    '<label style="margin-left:12px">Correlation ID:</label>'
    '<input id="f-cid" type="text" placeholder="e.g. 32928" oninput="applyFilter()" style="width:140px"/>'
    '<label style="margin-left:12px">Limit:</label>'
    '<select id="f-limit" onchange="loadData()">'
    '<option value="50">50</option><option value="100" selected>100</option>'
    '<option value="200">200</option><option value="500">All 500</option>'
    "</select>"
    "</div>"
    '<div class="wrap"><table>'
    "<thead><tr>"
    "<th>#</th><th>Received At (UTC)</th><th>Action</th><th>Correlation ID</th>"
    "<th>Event ID</th><th>Host</th><th>Severity</th><th>Problem</th>"
    "<th>State</th><th>Recovery ID</th><th>Conversation</th>"
    "</tr></thead>"
    '<tbody id="tb"><tr><td colspan="11" class="empty">Loading...</td></tr></tbody>'
    "</table></div>"
    '<div class="footer" id="ts"></div>'
    "<script>"
    "let A=[];"
    "async function loadData(){"
    "const lim=document.getElementById('f-limit').value;"
    "try{const s=await fetch('/api/v1/events/summary').then(r=>r.json());"
    "document.getElementById('c-total').textContent=s.total??0;"
    "document.getElementById('c-create').textContent=s.counters?.create??0;"
    "document.getElementById('c-clear').textContent=s.counters?.clear??0;"
    "document.getElementById('c-last').textContent=s.last_event?new Date(s.last_event).toLocaleString():'&#8212;';"
    "}catch(e){}"
    "try{A=await fetch('/api/v1/events?limit='+lim).then(r=>r.json());applyFilter();}catch(e){}"
    "document.getElementById('ts').textContent='Last refreshed: '+new Date().toLocaleString();}"
    "function applyFilter(){"
    "const act=document.getElementById('f-action').value;"
    "const cid=document.getElementById('f-cid').value.trim().toLowerCase();"
    "let rows=A;"
    "if(act) rows=rows.filter(e=>e.action===act);"
    "if(cid) rows=rows.filter(e=>(e.correlation_id||'').toLowerCase().includes(cid));"
    "const tb=document.getElementById('tb');"
    "if(!rows.length){tb.innerHTML='<tr><td colspan=\"11\" class=\"empty\">No events found.</td></tr>';return;}"
    "tb.innerHTML=rows.map((e,i)=>"
    "'<tr>'"
    "+'<td class=\"mono\">'+(i+1)+'</td>'"
    "+'<td class=\"mono\">'+(e.received_at||'').replace('T',' ').replace('Z',' UTC')+'</td>'"
    "+'<td><span class=\"badge '+e.action+'\">'+e.action+'</span></td>'"
    "+'<td class=\"mono\">'+(e.correlation_id||'&#8212;')+'</td>'"
    "+'<td class=\"mono\">'+(e.event_id||'&#8212;')+'</td>'"
    "+'<td>'+(e.host||'&#8212;')+'</td>'"
    "+'<td>'+(e.severity||'&#8212;')+'</td>'"
    "+'<td>'+(e.problem||'&#8212;')+'</td>'"
    "+'<td>'+(e.state||'&#8212;')+'</td>'"
    "+'<td class=\"mono\">'+(e.recovery_event_id||'&#8212;')+'</td>'"
    "+'<td>'+(e.conversation_name||'&#8212;')+'</td>'"
    "+'</tr>'"
    ").join('');}"
    "loadData();setInterval(loadData,30000);"
    "</script></body></html>"
)

_HTML = _H + _B


@dashboard_bp.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template_string(_HTML)


@dashboard_bp.route("/api/v1/events", methods=["GET"])
def get_events():
    action = request.args.get("action", "").strip().lower() or None
    limit  = min(int(request.args.get("limit", 100)), 500)
    return jsonify(store.get_events(action=action, limit=limit)), 200


@dashboard_bp.route("/api/v1/events/summary", methods=["GET"])
def get_summary():
    return jsonify(store.get_summary()), 200
