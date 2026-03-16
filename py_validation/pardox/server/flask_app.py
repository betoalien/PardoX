from __future__ import annotations
"""
pardox_server/flask_app.py
Flask control plane for pardoX Server.
Pages: dashboard, config (port/user/pass), databases (registry).
"""
import json
import os

from flask import Flask, redirect, render_template_string, request, url_for

from .pg_server  import PardoXPostgresServer
from .registry   import Registry

app = Flask(__name__)


# Injected at startup by cli.py
_server:   PardoXPostgresServer | None = None
_registry: Registry | None = None
_config_path: str = ""   # path to server.json


def init(server: PardoXPostgresServer, registry: Registry, config_path: str):
    global _server, _registry, _config_path
    _server      = server
    _registry    = registry
    _config_path = config_path


# ── Config persistence ─────────────────────────────────────────────────────

def _load_config() -> dict:
    if os.path.exists(_config_path):
        try:
            with open(_config_path) as f:
                return json.load(f)
        except Exception:
            pass
    return {'port': 5235, 'username': 'pardox_user', 'password': 'pardoX_secret'}


def _save_config(cfg: dict):
    os.makedirs(os.path.dirname(_config_path), exist_ok=True)
    with open(_config_path, 'w') as f:
        json.dump(cfg, f, indent=2)


# ── Common CSS ─────────────────────────────────────────────────────────────

_STYLE = """
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0f0f23;color:#e0e0e0}
.wrap{max-width:1100px;margin:0 auto;padding:24px}
header{background:linear-gradient(135deg,#667eea,#764ba2);padding:20px;border-radius:10px;margin-bottom:20px}
h1{font-size:1.8em;margin-bottom:4px}
.sub{opacity:.8;font-size:.9em}
nav{display:flex;gap:10px;margin-bottom:20px}
nav a{color:#667eea;text-decoration:none;padding:8px 16px;background:#16213e;border-radius:8px}
nav a:hover{background:#1a1a2e}
.card{background:#16213e;border-radius:10px;padding:20px;margin-bottom:20px}
.card h2{color:#667eea;margin-bottom:14px}
.stat-row{display:flex;gap:14px;flex-wrap:wrap}
.stat{background:#1a1a2e;padding:14px 20px;border-radius:8px;text-align:center;min-width:120px}
.stat-val{font-size:2em;font-weight:bold;color:#667eea}
.stat-lbl{color:#aaa;font-size:.85em}
.btn{padding:10px 20px;border:none;border-radius:8px;cursor:pointer;font-size:.95em;font-weight:bold;transition:.2s}
.btn-start{background:#00e676;color:#111}.btn-start:hover{background:#00c853}
.btn-stop{background:#ff5252;color:#fff}.btn-stop:hover{background:#c62828}
.btn-primary{background:#667eea;color:#fff}.btn-primary:hover{background:#5a6fd6}
.btn-danger{background:#ef5350;color:#fff;padding:6px 14px;font-size:.85em}
.btn-sm{padding:6px 14px;font-size:.85em}
.dot{width:16px;height:16px;border-radius:50%;display:inline-block;margin-right:8px;vertical-align:middle}
.dot-on{background:#00e676;box-shadow:0 0 8px #00e676}
.dot-off{background:#ff5252}
.status-row{display:flex;align-items:center;justify-content:space-between}
.fgrp{margin-bottom:14px}
.fgrp label{display:block;margin-bottom:5px;color:#aaa;font-size:.9em}
.fgrp input{width:100%;padding:10px;background:#1a1a2e;border:1px solid #333;border-radius:8px;color:#eee;font-size:.95em}
.fgrp input:focus{outline:none;border-color:#667eea}
table{width:100%;border-collapse:collapse}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #1a1a2e;font-size:.9em}
th{color:#667eea;background:#1a1a2e}
tr:hover td{background:#1a1a2e}
.alert{padding:12px;border-radius:8px;margin-bottom:14px;font-size:.9em}
.alert-ok{background:#00e67622;border:1px solid #00e676;color:#00e676}
.alert-err{background:#ff525222;border:1px solid #ff5252;color:#ff5252}
.conn-str{background:#1a1a2e;padding:10px;border-radius:6px;font-family:monospace;font-size:.85em;color:#a5d6a7;word-break:break-all}
</style>
"""

_NAV = """
<nav>
  <a href="/">Dashboard</a>
  <a href="/config">Configuration</a>
  <a href="/databases">Databases</a>
</nav>
"""

_HEAD = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>pardoX Server</title>""" + _STYLE + "</head><body><div class='wrap'>"

_FOOT = "</div></body></html>"


# ── Routes ─────────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    running = _server and _server.is_running()
    logs    = (_server.get_logs() if _server else [])[-50:][::-1]
    cfg     = _load_config()
    total   = len(logs)
    dbs     = len(_registry.list()) if _registry else 0

    html = _HEAD + """
    <header><h1>pardoX Server</h1>
    <p class="sub">PostgreSQL wire protocol server backed by pardox_cpu</p></header>
    """ + _NAV + f"""
    <div class="card">
      <div class="status-row">
        <div>
          <span class="dot {'dot-on' if running else 'dot-off'}"></span>
          <strong>{'RUNNING' if running else 'STOPPED'}</strong>
          {'&nbsp;&nbsp;port ' + str(cfg['port']) if running else ''}
        </div>
        <div style="display:flex;gap:10px">
          <form method="POST" action="/server/start" style="display:inline">
            <button class="btn btn-start" {'disabled' if running else ''}>Start</button>
          </form>
          <form method="POST" action="/server/stop" style="display:inline">
            <button class="btn btn-stop" {'disabled' if not running else ''}>Stop</button>
          </form>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Stats</h2>
      <div class="stat-row">
        <div class="stat"><div class="stat-val">{dbs}</div><div class="stat-lbl">Databases</div></div>
        <div class="stat"><div class="stat-val">{total}</div><div class="stat-lbl">Log entries</div></div>
      </div>
    </div>
    """

    if running:
        html += f"""
        <div class="card">
          <h2>Connection String</h2>
          <div class="conn-str">
            postgresql://{cfg['username']}:{cfg['password']}@127.0.0.1:{cfg['port']}/postgres
          </div>
          <p style="color:#aaa;font-size:.85em;margin-top:8px">
            psql: <code>psql postgresql://{cfg['username']}:{cfg['password']}@127.0.0.1:{cfg['port']}/postgres</code>
          </p>
        </div>
        """

    html += """<div class="card"><h2>Activity Log</h2><table>
    <thead><tr><th>Time</th><th>Message</th></tr></thead><tbody>"""
    if logs:
        for entry in logs:
            html += f"<tr><td style='white-space:nowrap'>{entry['ts']}</td><td>{entry['msg']}</td></tr>"
    else:
        html += "<tr><td colspan='2' style='color:#666;text-align:center'>No activity yet</td></tr>"
    html += "</tbody></table></div>"
    html += "<script>setTimeout(()=>location.reload(),5000)</script>"
    html += _FOOT
    return html


@app.route('/server/start', methods=['POST'])
def server_start():
    if _server:
        _server.start()
    return redirect(url_for('dashboard'))


@app.route('/server/stop', methods=['POST'])
def server_stop():
    if _server:
        _server.stop()
    return redirect(url_for('dashboard'))


@app.route('/config', methods=['GET', 'POST'])
def config():
    cfg = _load_config()
    alert = ''
    if request.method == 'POST':
        try:
            port     = int(request.form.get('port', 5235))
            username = request.form.get('username', 'pardox_user').strip()
            password = request.form.get('password', 'pardoX_secret').strip()
            if not (1024 <= port <= 65535):
                raise ValueError("Port must be between 1024 and 65535")
            cfg = {'port': port, 'username': username, 'password': password}
            _save_config(cfg)
            if _server:
                _server.reconfigure(port=port, username=username, password=password)
            alert = "<div class='alert alert-ok'>Configuration saved. Restart server to apply port changes.</div>"
        except Exception as e:
            alert = f"<div class='alert alert-err'>Error: {e}</div>"

    html = _HEAD + """
    <header><h1>Configuration</h1>
    <p class="sub">Server connection settings</p></header>
    """ + _NAV + f"""
    {alert}
    <div class="card" style="max-width:480px">
      <h2>Server Settings</h2>
      <form method="POST">
        <div class="fgrp"><label>Port</label>
          <input name="port" type="number" value="{cfg['port']}" min="1024" max="65535" required></div>
        <div class="fgrp"><label>Username</label>
          <input name="username" value="{cfg['username']}" required></div>
        <div class="fgrp"><label>Password</label>
          <input name="password" value="{cfg['password']}" required></div>
        <button class="btn btn-primary" type="submit">Save</button>
      </form>
    </div>
    """ + _FOOT
    return html


@app.route('/databases', methods=['GET'])
def databases():
    tables = _registry.list() if _registry else []
    alert  = request.args.get('alert', '')
    alert_type = request.args.get('at', 'ok')
    alert_html = f"<div class='alert alert-{alert_type}'>{alert}</div>" if alert else ''

    html = _HEAD + """
    <header><h1>Databases</h1>
    <p class="sub">Register .prdx and .parquet files as PostgreSQL tables</p></header>
    """ + _NAV + alert_html + """
    <div class="card" style="max-width:600px">
      <h2>Add Database</h2>
      <form method="POST" action="/databases/add">
        <div class="fgrp">
          <label>Table Name (used as <code>public.&lt;name&gt;</code>)</label>
          <input name="table_name" placeholder="e.g. sales_db" pattern="[a-zA-Z0-9_]+" required>
        </div>
        <div class="fgrp">
          <label>File Path (absolute path to .prdx or .parquet)</label>
          <input name="file_path" placeholder="/path/to/data.prdx" required>
        </div>
        <button class="btn btn-primary" type="submit">Register</button>
      </form>
    </div>

    <div class="card">
      <h2>Registered Tables</h2>"""

    if tables:
        html += """<table>
        <thead><tr><th>Table Name</th><th>Connection Path</th><th>File Path</th><th>Action</th></tr></thead>
        <tbody>"""
        for name, path in tables:
            size_mb = os.path.getsize(path) / 1e6 if os.path.exists(path) else 0
            html += f"""<tr>
              <td><code>public.{name}</code></td>
              <td><code style="color:#a5d6a7">SELECT * FROM public.{name} LIMIT 10</code></td>
              <td style="color:#aaa;font-size:.85em">{path} ({size_mb:.1f} MB)</td>
              <td>
                <form method="POST" action="/databases/remove" style="display:inline">
                  <input type="hidden" name="table_name" value="{name}">
                  <button class="btn btn-danger" type="submit">Remove</button>
                </form>
              </td>
            </tr>"""
        html += "</tbody></table>"
    else:
        html += "<p style='color:#666;text-align:center;padding:20px'>No databases registered yet.</p>"

    html += "</div>" + _FOOT
    return html


@app.route('/databases/add', methods=['POST'])
def databases_add():
    table_name = request.form.get('table_name', '').strip()
    file_path  = request.form.get('file_path', '').strip()
    if _registry:
        err = _registry.add(table_name, file_path)
        if err:
            return redirect(url_for('databases', alert=err, at='err'))
    return redirect(url_for('databases', alert=f"Table '{table_name}' registered successfully.", at='ok'))


@app.route('/databases/remove', methods=['POST'])
def databases_remove():
    table_name = request.form.get('table_name', '').strip()
    if _registry:
        _registry.remove(table_name)
    return redirect(url_for('databases', alert=f"Table '{table_name}' removed.", at='ok'))


# ── JSON API ───────────────────────────────────────────────────────────────

@app.route('/api/status')
def api_status():
    from flask import jsonify
    cfg = _load_config()
    return jsonify({
        'running':   _server.is_running() if _server else False,
        'port':      cfg['port'],
        'username':  cfg['username'],
        'databases': [{'name': n, 'path': p} for n, p in (_registry.list() if _registry else [])],
    })
