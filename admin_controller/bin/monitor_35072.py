#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitor_35072.py — Py3.11 + path_utils

Дашборд 35072:
  - список нод, проблемные, логи, очередь, правила
  - HAProxy Runtime API (enable/disable/drain/weight)
  - Редактирование haproxy.cfg (comment/uncomment server <node>) + reload
  - Асинхронная очередь (до 32 воркеров)
  - Автопарсер haproxy.cfg по выбранным backend’ам
  - Вкладки Peers (переключение между точками входа)

Все артефакты строго в /tmp/pattern_controller:
  /tmp/pattern_controller/
    ├─ signals/                # флаги и очереди (общая шина)
    ├─ report/<HOSTNAME>/...   # отчёты этой ноды
    └─ logs/<HOSTNAME>/...     # логи этой ноды

Суммарные графики: /tmp/pattern_controller/report/graphs/*.json
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import re
import shlex
import socket as pysock
import sys
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse, parse_qs

# ---- единые пути/идентичность
from path_utils import BASE, HOSTNAME as THIS_HOST, SIGNALS_DIR, LOGS_DIR, REPORT_DIR

# ---------- Базовые пути (дефолты можно переопределить флагами) ----------
DEFAULT_FLAG_DIR = str(SIGNALS_DIR)
DEFAULT_CONTROLLER_REPORTS = str(BASE / "report")   # агрегатор читает все /report/<NODE>
DEFAULT_WORKER_REPORT_DIRS = str(BASE / "report")   # просмотр логов/отчётов воркеров
DEFAULT_LOG_DIR = str(BASE / "logs")                # поиск ошибок в логах

# ---- правила
RULES_PROD_PATH   = str(BASE / "report" / "rules.json")
RULES_SAFE_PATH   = str(BASE / "report" / "rules_safe.json")
RULES_ACTIVE_LINK = str(BASE / "report" / "current_rules.json")

# --- HAProxy runtime/config defaults (переопределяются CLI-флагами) ---
HAPROXY_SOCKET = "/var/lib/haproxy/haproxy.sock"   # Unix socket
HAPROXY_TCP: Optional[str] = None                  # "127.0.0.1:9999"
HAPROXY_CFG = "/etc/haproxy/haproxy.cfg"
HAPROXY_RELOAD_CMD = "systemctl reload haproxy"
HAPROXY_BACKENDS = ["Jboss_client"]
HAPROXY_PARSE_INTERVAL_SEC = 60

# Ручные соответствия (при необходимости)
MANUAL_NODE_TO_BACKEND: Dict[str, Tuple[str, str]] = {
    # "node_97": ("Jboss_client", "node_97"),
}

# Автокарта из парсера + lock
_PARSED_MAP: Dict[str, Dict[str, Any]] = {}
_PARSED_TS: float = 0.0
_MAP_LOCK = threading.Lock()

HTML_HEAD = """<!doctype html><html><head>
<meta charset="utf-8"><title>JBoss Monitor</title>
<style>
body{font-family:Arial,Helvetica,sans-serif;margin:16px;background:#f7f7f7;color:#222}
h1,h2{margin:4px 0 10px} small{color:#666} a{color:#0a58ca;text-decoration:none}
.wrap{max-width:1280px;margin:0 auto}
.card{background:#fff;border:1px solid #e5e5e5;border-radius:8px;margin:12px 0;box-shadow:0 1px 2px rgba(0,0,0,.05)}
.card .hd{padding:10px 14px;border-bottom:1px solid #eee;font-weight:bold}
.card .bd{padding:12px 14px}
table{width:100%;border-collapse:collapse} th,td{padding:8px;border-bottom:1px solid #eee;text-align:left;font-size:14px}
th{background:#fafafa}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px}
.ok{background:#e6ffec;color:#137333;border:1px solid #b7f0c2}
.fail{background:#ffeaea;color:#a1181a;border:1px solid #f1b4b6}
.warn{background:#fff7e6;color:#8a5a00;border:1px solid #ffe0a3}
.info{background:#e7f1ff;color:#0b4aa2;border:1px solid #c9defd}
.off{background:#eee;color:#555;border:1px solid #ddd}
.mono{font-family:Menlo,Consolas,monospace;font-size:12px;white-space:pre-wrap}
.footer{color:#666;font-size:12px;margin-top:20px}
.row{display:flex;flex-wrap:wrap;gap:12px}
.col{flex:1 1 420px}
input[type=text],input[type=password]{padding:6px 8px;border:1px solid #ccc;border-radius:6px}
.btn{display:inline-block;padding:6px 10px;border:1px solid #0a58ca;border-radius:6px;color:#fff;background:#0a58ca}
.btn.warn{border-color:#a15a00;background:#a15a00}
.btn.safe{border-color:#0a9c3a;background:#0a9c3a}
.btn.prod{border-color:#a11a1a;background:#a11a1a}
.btn.red{border-color:#a11a1a;background:#a11a1a}
.kv td:first-child{width:180px;color:#666}
.small{font-size:12px;color:#666}
.nav{margin:8px 0 12px}
.nav a{margin-right:8px}
.peers{margin:6px 0 12px}
.peers a{margin-right:6px}
.right{float:right}
</style>
<script>
function ffilter(){
  var q=(document.getElementById('q').value||'').toLowerCase();
  var cards=document.getElementsByClassName('nodecard');
  for(var i=0;i<cards.length;i++){
    var t=cards[i].getAttribute('data-node')+' '+cards[i].getAttribute('data-state');
    cards[i].style.display=(t.toLowerCase().indexOf(q)>=0)?'':'none';
  }
}
</script>
</head><body><div class="wrap">
"""

HTML_TAIL = "<div class=\"footer\">JBoss Monitor • host: {host} • time: {now}</div></div></body></html>"

def now() -> dt.datetime: return dt.datetime.now()
def ts()  -> str:         return now().strftime("%Y-%m-%d %H:%M:%S")

def human_dt(epoch: Optional[float]) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch or 0))
    except Exception:
        return "-"

# ---------- utils ----------
def tail_lines(path: str, n: int = 200, max_bytes: int = 2 * 1024 * 1024) -> List[str]:
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
            data = f.read()
        try:
            text = data.decode("utf-8")
        except Exception:
            text = data.decode("latin-1", "ignore")
        lines = text.splitlines()
        return lines[-int(n):] if n > 0 else lines
    except Exception as e:
        return [f"<unable to read: {e}>"]

def ensure_dir(p: Optional[str]) -> None:
    if not p: return
    d = Path(p)
    d.mkdir(parents=True, exist_ok=True)

def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def _atomic_write(path: str, content_bytes: bytes) -> None:
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".haproxy_cfg.", dir=d)
    try:
        os.write(fd, content_bytes)
        os.close(fd)
        os.replace(tmp, path)
    except Exception:
        try: os.close(fd)
        except Exception: pass
        try:
            if os.path.exists(tmp): os.unlink(tmp)
        except Exception: pass
        raise

# ---------- files enumeration ----------
def _iter_all_subdirs(root: str) -> List[str]:
    """Возвращает [root] + подкаталоги первого уровня, если есть."""
    out = []
    r = Path(root)
    if not r.exists():
        return out
    out.append(str(r))
    for p in r.iterdir():
        if p.is_dir():
            out.append(str(p))
    return out

def list_error_logs(log_root: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    for d in _iter_all_subdirs(log_root):
        try:
            for name in os.listdir(d):
                cond_log = (name.endswith(".log") and (name.startswith("node_") or name.startswith("worker_") or name == "dispatcher.log"))
                if cond_log or name == "dispatcher.log":
                    p = os.path.join(d, name)
                    st = os.stat(p)
                    files.append({"path": p, "size": st.st_size, "mtime": st.st_mtime})
        except Exception:
            pass
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return files

def detect_nodes(flag_dir: str, worker_roots: List[str], controller_root: str) -> List[str]:
    nodes = set()
    # restart_/done_ флаги
    if flag_dir and os.path.isdir(flag_dir):
        for name in os.listdir(flag_dir):
            if name.startswith("restart_") and name.endswith(".txt"):
                nodes.add(name[len("restart_"):-4])
            if name.startswith("done_") and name.endswith(".txt"):
                nodes.add(name[len("done_"):-4])
    # worker_* в логах
    for root in worker_roots or []:
        for d in _iter_all_subdirs(root):
            try:
                for name in os.listdir(d):
                    if name.startswith("worker_") and name.endswith(".log"):
                        parts = name.split("_")
                        if len(parts) >= 3:
                            nodes.add(parts[1])
            except Exception:
                pass
    # controller_summary.csv из всех /report/<NODE>
    for d in _iter_all_subdirs(controller_root):
        csvp = os.path.join(d, "controller_summary.csv")
        if not os.path.exists(csvp):
            continue
        try:
            with open(csvp, "r", encoding="utf-8", newline="") as f:
                r = csv.reader(f)
                headers = None
                for row in r:
                    if headers is None:
                        headers = row
                        continue
                    if len(row) >= 3:
                        nodes.add(row[2])
        except Exception:
            pass
    return sorted(nodes)

def read_done_flag(flag_dir: str, node: str) -> Optional[Dict[str, Any]]:
    p = os.path.join(flag_dir, f"done_{node}.txt")
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
        st = os.stat(p)
        return {"path": p, "mtime": st.st_mtime, "raw": data.strip()}
    except Exception:
        return None

def restart_flag_present(flag_dir: str, node: str) -> Tuple[bool, Optional[float]]:
    p = os.path.join(flag_dir, f"restart_{node}.txt")
    if os.path.exists(p):
        try:
            st = os.stat(p)
            return True, st.st_mtime
        except Exception:
            return True, None
    return False, None

def guess_verify(raw_done: Optional[str]) -> Optional[str]:
    if not raw_done:
        return None
    s = raw_done.lower()
    if "verify=ok" in s or " verify ok" in s:
        return "OK"
    if "verify=fail" in s or " verify fail" in s:
        return "FAIL"
    return None

def _find_last_controller_row(controller_root: str, node: str) -> Optional[List[str]]:
    # последняя строка по ноде из любого controller_summary.csv
    last: Optional[List[str]] = None
    last_mtime = -1.0
    for d in _iter_all_subdirs(controller_root):
        csvp = os.path.join(d, "controller_summary.csv")
        if not os.path.exists(csvp):
            continue
        try:
            mtime = os.stat(csvp).st_mtime
            with open(csvp, "r", encoding="utf-8", newline="") as f:
                r = csv.reader(f)
                headers = None
                for row in r:
                    if headers is None:
                        headers = row
                        continue
                    if len(row) < 11:
                        continue
                    if row[2] == node and mtime >= last_mtime:
                        last = row; last_mtime = mtime
        except Exception:
            pass
    return last

# ---- rules profile ----
def active_rules_profile() -> Tuple[str, Optional[str]]:
    if not os.path.lexists(RULES_ACTIVE_LINK):
        return ("unknown", None)
    try:
        target = os.path.realpath(RULES_ACTIVE_LINK)
        base = os.path.basename(target)
        if base == os.path.basename(RULES_PROD_PATH):
            return ("prod", target)
        if base == os.path.basename(RULES_SAFE_PATH):
            return ("safe", target)
        return ("custom", target)
    except Exception:
        return ("unknown", None)

def switch_rules(to_profile: str) -> Tuple[bool, str]:
    target = RULES_PROD_PATH if to_profile == "prod" else RULES_SAFE_PATH
    if not os.path.exists(target):
        return (False, f"target rules file not found: {target}")
    try:
        temp_link = RULES_ACTIVE_LINK + ".tmp"
        try:
            if os.path.lexists(temp_link): os.unlink(temp_link)
        except Exception: pass
        os.symlink(os.path.relpath(target, os.path.dirname(RULES_ACTIVE_LINK)), temp_link)
        try:
            if os.path.lexists(RULES_ACTIVE_LINK): os.unlink(RULES_ACTIVE_LINK)
        except Exception: pass
        os.replace(temp_link, RULES_ACTIVE_LINK)
        return (True, f"switched to {to_profile}")
    except Exception as e:
        try:
            if os.path.lexists(temp_link): os.unlink(temp_link)
        except Exception: pass
        return (False, f"switch error: {e}")

# ---- queue helpers ----
def q_paths(flag_dir: str) -> Tuple[str, str, str, str]:
    q  = os.path.join(flag_dir, "queue")
    ip = os.path.join(flag_dir, "inprogress")
    d  = os.path.join(flag_dir, "done")
    f  = os.path.join(flag_dir, "failed")
    return q, ip, d, f

def list_json(dirpath: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if dirpath and os.path.isdir(dirpath):
        for name in os.listdir(dirpath):
            if name.endswith(".json"):
                p = os.path.join(dirpath, name)
                try:
                    st = os.stat(p)
                    items.append({"path": p, "name": name, "mtime": st.st_mtime, "size": st.st_size})
                except Exception:
                    pass
    items.sort(key=lambda x: x["name"])
    return items

def enqueue_haproxy_op(qdir: str, op: str, scope: str, backend: str, server: str, extra: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    ensure_dir(qdir)
    import uuid
    rid = str(uuid.uuid4())[:8]
    tsid = now().strftime("%Y%m%d_%H%M%S")
    payload: Dict[str, Any] = {"id": rid, "ts": tsid, "op": op, "scope": scope, "backend": backend, "server": server}
    if extra: payload.update(extra)
    name = f"rq_{tsid}_{backend or 'be'}_{server or 'srv'}_{rid}.json"
    path = os.path.join(qdir, name)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        return True, path
    except Exception as e:
        return False, str(e)

# ---- HAProxy Runtime API ----
def _haproxy_send(cmd: str, timeout: float = 2.0) -> Tuple[bool, str]:
    data = (cmd.strip() + "\n").encode("ascii", "ignore")
    # AF_UNIX
    if HAPROXY_SOCKET and os.path.exists(HAPROXY_SOCKET):
        import socket as sck
        try:
            s = sck.socket(sck.AF_UNIX, sck.SOCK_STREAM)
            s.settimeout(timeout); s.connect(HAPROXY_SOCKET); s.sendall(data)
            out = []
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                out.append(chunk)
            try: s.close()
            except Exception: pass
            txt = b"".join(out).decode("utf-8", "ignore")
            return True, txt
        except Exception as e:
            return False, f"unix error: {e}"
    # AF_INET
    if HAPROXY_TCP:
        import socket as sck
        try:
            host, port_s = HAPROXY_TCP.split(":", 1)
            port = int(port_s)
            s = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
            s.settimeout(timeout); s.connect((host, port)); s.sendall(data)
            out = []
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                out.append(chunk)
            try: s.close()
            except Exception: pass
            txt = b"".join(out).decode("utf-8", "ignore")
            return True, txt
        except Exception as e:
            return False, f"tcp error: {e}"
    return False, "no runtime endpoint configured"

def haproxy_set_state(backend: str, server: str, action: str) -> Tuple[bool, str]:
    if action == "disable": return _haproxy_send(f"disable server {backend}/{server}")
    if action == "enable":  return _haproxy_send(f"enable server {backend}/{server}")
    if action == "drain":   return _haproxy_send(f"set server {backend}/{server} state drain")
    return (False, f"unknown action: {action}")

# ---- haproxy.cfg edit helpers ----
def _reload_haproxy() -> int:
    cmd = shlex.quote(HAPROXY_RELOAD_CMD)
    return os.system(f"/bin/sh -lc {cmd}")

def edit_haproxy_cfg_server(node_name: str, do_comment: bool) -> Tuple[bool, str]:
    if not os.path.isfile(HAPROXY_CFG): return (False, f"cfg not found: {HAPROXY_CFG}")
    try:
        raw = _read_file_bytes(HAPROXY_CFG)
        try: text = raw.decode("utf-8")
        except Exception: text = raw.decode("latin-1", "ignore")
        rx = re.compile(rf'^(\s*)(#\s*)?(server\s+{re.escape(node_name)}\b.*)$', re.M)
        changed = [False]
        def _repl(m: re.Match[str]) -> str:
            indent, hashpart, rest = m.group(1), m.group(2), m.group(3)
            if do_comment:
                if hashpart: return m.group(0)
                changed[0] = True; return indent + "# " + rest
            else:
                if not hashpart: return m.group(0)
                changed[0] = True; return indent + rest
        new_text = rx.sub(_repl, text)
        if not changed[0]:
            return (False, "no matching line changed (maybe already desired)")
        # backup + write + reload
        try:
            backup_path = HAPROXY_CFG + ".bak_" + time.strftime("%Y%m%d_%H%M%S")
            with open(backup_path, "wb") as fb: fb.write(raw)
        except Exception: pass
        _atomic_write(HAPROXY_CFG, new_text.encode("utf-8"))
        rc = _reload_haproxy()
        return (True, "cfg updated & reloaded" if rc == 0 else f"cfg updated, reload rc={rc}")
    except Exception as e:
        return (False, f"edit error: {e}")

# ---- Парсер haproxy.cfg
def _parse_backends_from_cfg(cfg_path: str, backend_names: List[str]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    try:
        raw = _read_file_bytes(cfg_path)
        try: text = raw.decode("utf-8")
        except Exception: text = raw.decode("latin-1", "ignore")
    except Exception:
        return result
    lines = text.splitlines()
    cur: Optional[str] = None
    for s in lines:
        line = s.rstrip("\r\n")
        st = line.strip()
        if re.match(r"^(global|defaults|frontend|listen|backend)\b", st):
            if st.startswith("backend "):
                name = st.split(None, 1)[1].strip()
                cur = name if (name in backend_names) else None
            else:
                cur = None
            continue
        if not cur: continue
        m = re.match(r'^\s*(#\s*)?server\s+([^\s]+)\s+(\S+)\s*(.*)$', line)
        if m:
            commented = bool(m.group(1))
            srv_name = m.group(2)
            result[srv_name] = {"backend": cur, "server": srv_name, "commented": commented}
    return result

def get_node_info(node: str) -> Tuple[Optional[str], Optional[str], Optional[bool]]:
    with _MAP_LOCK:
        if node in _PARSED_MAP:
            info = _PARSED_MAP[node]
            return (info["backend"], info["server"], bool(info.get("commented")))
        if node in MANUAL_NODE_TO_BACKEND:
            bk, srv = MANUAL_NODE_TO_BACKEND[node]
            return (bk, srv, None)
    return (None, None, None)

def schedule_cfg_parser():
    def _tick():
        global _PARSED_MAP, _PARSED_TS
        try:
            pm = _parse_backends_from_cfg(HAPROXY_CFG, HAPROXY_BACKENDS)
            with _MAP_LOCK:
                _PARSED_MAP = pm; _PARSED_TS = time.time()
        except Exception:
            try: sys.stderr.write("CFG PARSE ERR:\n%s\n" % traceback.format_exc())
            except Exception: pass
        t = threading.Timer(HAPROXY_PARSE_INTERVAL_SEC, _tick)
        t.daemon = True; t.start()
    _tick()

# ---- Async job queue (32 workers) ----
try:
    JOBQ: "queue.Queue[Tuple]"  # type: ignore[name-defined]
    import queue
    JOBQ = queue.Queue()
except Exception:
    JOBQ = None  # type: ignore[assignment]

def _job_worker():
    while True:
        try:
            fn, args, kwargs = JOBQ.get()
            try:
                fn(*args, **(kwargs or {}))
            finally:
                JOBQ.task_done()
        except Exception:
            try: sys.stderr.write("JOB ERR:\n%s\n" % traceback.format_exc())
            except Exception: pass

def start_job_workers(n: int = 32):
    if JOBQ is None: return
    for i in range(int(n)):
        t = threading.Thread(target=_job_worker, name=f"jobw-{i}", daemon=True)
        t.start()

def submit_async(fn, *args, **kwargs):
    if JOBQ is None:
        return fn(*args, **(kwargs or {}))
    JOBQ.put((fn, args, kwargs))

# ---- Peers (переключение точек входа) ----
def parse_peers(peers_str: str, default_port: int) -> List[Dict[str, str]]:
    res: List[Dict[str, str]] = []
    for tok in (peers_str or "").split(","):
        t = tok.strip()
        if not t: continue
        label = None; hostport = t
        if "@" in t:
            parts = t.split("@", 1)
            if len(parts) == 2:
                label, hostport = parts[0].strip(), parts[1].strip()
        if ":" in hostport:
            host, port = hostport.split(":", 1); host, port = host.strip(), (port.strip() or str(default_port))
        else:
            host, port = hostport, str(default_port)
        if not label: label = host
        url = f"http://{host}:{port}/"
        res.append({"label": label, "url": url})
    return res

# ---- HTTP server ----
class Handler(BaseHTTPRequestHandler):
    flag_dir: Optional[str] = None
    haproxy_ops_queue: Optional[str] = None
    haproxy_backends: List[str] = []
    controller_dir: Optional[str] = None
    worker_dirs: List[str] = []
    error_log_dir: Optional[str] = None
    refresh: int = 5
    toggle_secret: Optional[str] = None
    peers: List[Dict[str, str]] = []

    def _write_html(self, html_str: str) -> None:
        data = html_str.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _peers_bar(self) -> str:
        if not self.peers: return ""
        buf = ["<div class='peers'><span class='small'>Peers:</span> "]
        for p in self.peers:
            label = html.escape(p.get("label") or "-")
            url = html.escape(p.get("url") or "#")
            buf.append(f"<a class='btn' href='{url}'>{label}</a>")
        buf.append("</div>")
        return "".join(buf)

    def _header_with_rules(self) -> str:
        prof, _tgt = active_rules_profile()
        label = {"prod": "боевой", "safe": "safe", "custom": "custom", "unknown": "unknown"}.get(prof, prof)
        cls = {"prod": "prod", "safe": "safe", "custom": "info", "unknown": "warn"}.get(prof, "info")
        with _MAP_LOCK: pt = _PARSED_TS
        sub = f" • cfg-parsed: {time.strftime('%H:%M:%S', time.localtime(pt)) if pt else '-'}"
        badge = f"<span class='badge {cls}'>rules: {html.escape(label)}</span><span class='small'>{html.escape(sub)}</span>"
        return badge

    def _collect_nodes(self) -> Tuple[List[Dict[str, Any]], int]:
        nodes = detect_nodes(self.flag_dir or "", self.worker_dirs, self.controller_dir or "")
        items: List[Dict[str, Any]] = []; problems = 0
        for n in nodes:
            has_restart, r_mtime = restart_flag_present(self.flag_dir or "", n)
            done = read_done_flag(self.flag_dir or "", n)
            ver = guess_verify(done.get("raw") if done else None)
            ctrl = _find_last_controller_row(self.controller_dir or "", n)
            is_problem = has_restart or (ver == "FAIL")
            if is_problem: problems += 1
            bk, srv, commented = get_node_info(n)
            items.append({
                "node": n, "has_restart": has_restart, "restart_mtime": r_mtime,
                "done": done, "verify": ver, "ctrl": ctrl, "problem": is_problem,
                "hap_backend": bk, "hap_server": srv, "cfg_commented": commented,
            })
        items.sort(key=lambda x: (not x["problem"], x["node"]))
        return items, problems

    # ---- routing ----
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"
            qs = parse_qs(parsed.query or "")
            if path in ("/", "/index", "/index.html"):
                self.handle_index(qs, problems_only=False)
            elif path == "/problems":
                self.handle_index(qs, problems_only=True)
            elif path == "/node":
                self.handle_node(qs)
            elif path == "/logs":
                self.handle_logs(qs)
            elif path == "/status":
                self.handle_status(qs)
            elif path == "/rules":
                self.handle_rules(qs, method="GET")
            elif path == "/queue":
                self.handle_queue(qs, method="GET")
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            sys.stderr.write(f"ERR: {e}\n{traceback.format_exc()}\n")
            try: self.send_error(500, "Internal error")
            except Exception: pass

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b""
            body = raw.decode("utf-8", "ignore")
            qs = parse_qs(body)
            if parsed.path == "/rules":
                self.handle_rules(qs, method="POST")
            elif parsed.path == "/queue":
                self.handle_queue(qs, method="POST")
            elif parsed.path == "/haproxy":
                self.handle_haproxy(qs)
            elif parsed.path == "/haproxy_cfg":
                self.handle_haproxy_cfg(qs)
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            sys.stderr.write(f"POST ERR: {e}\n{traceback.format_exc()}\n")
            try: self.send_error(500, "Internal error")
            except Exception: pass

    # ---- views ----
    def handle_index(self, qs: Dict[str, List[str]], problems_only: bool = False):
        host = THIS_HOST
        items, problems = self._collect_nodes()
        html_buf = [HTML_HEAD]
        html_buf.append(f"<h1>JBoss Monitor <small>host: {html.escape(host)}</small> <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append(f"<div class='nav'><a class='btn' href='/'>Все</a> <a class='btn' href='/problems'>Проблемные ({problems})</a> <a class='btn' href='/logs'>Логи</a> <a class='btn' href='/rules'>Правила</a> <a class='btn' href='/queue'>Очередь</a></div>")
        html_buf.append("<div class='card'><div class='hd'>Ноды</div><div class='bd'><input id='q' type='text' placeholder='фильтр' onkeyup='ffilter()'/><div class='row'>")
        for it in items:
            if problems_only and not it["problem"]:
                continue
            n = it["node"]
            st = ("problem" if it["problem"] else "ok")
            cfg = it["cfg_commented"]
            cfgb = ('<span class="badge off">cfg: OFF</span>' if cfg is True
                    else ('<span class="badge ok">cfg: ON</span>' if cfg is False else ''))
            html_buf.append(
                f"<div class='card nodecard' data-node='{html.escape(n)}' data-state='{html.escape(st)}'>"
                f"<div class='hd'>{html.escape(n)} {cfgb}</div><div class='bd'>"
                f"<a class='btn' href='/node?name={html.escape(n)}'>Открыть</a>"
            )
            if it["has_restart"]:
                html_buf.append(' <span class="badge warn">restart flag</span>')
            if it["verify"] == "FAIL":
                html_buf.append(' <span class="badge fail">verify=FAIL</span>')
            elif it["verify"] == "OK":
                html_buf.append(' <span class="badge ok">verify=OK</span>')
            html_buf.append("</div></div>")
        html_buf.append("</div></div></div>")
        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_node(self, qs: Dict[str, List[str]]):
        node = (qs.get("name", [""])[0]).strip()
        if not node:
            self.send_error(400, "name required"); return
        has_restart, r_mtime = restart_flag_present(self.flag_dir or "", node)
        done = read_done_flag(self.flag_dir or "", node)
        ver = guess_verify(done.get("raw") if done else None)
        ctrl = _find_last_controller_row(self.controller_dir or "", node)
        bk, srv, commented = get_node_info(node)

        host = THIS_HOST
        html_buf = [HTML_HEAD]
        badge_cfg = ('<span class="badge off">cfg: OFF</span>' if commented is True
                     else ('<span class="badge ok">cfg: ON</span>' if commented is False else ''))
        html_buf.append(f"<h1>Нода: {html.escape(node)} {badge_cfg} <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append(f"<div class='nav'><a class='btn' href='/'>← Все</a> <a class='btn' href='/queue'>Очередь</a> <a class='btn' href='/rules'>Правила</a></div>")

        html_buf.append("<div class='card'><div class='hd'>Состояние</div><div class='bd'><table class='kv'>")
        html_buf.append(f"<tr><td>restart flag</td><td>{'YES' if has_restart else 'NO'} {html.escape(human_dt(r_mtime) if r_mtime else '')}</td></tr>")
        html_buf.append(f"<tr><td>verify</td><td>{html.escape(ver or '-')}</td></tr>")
        html_buf.append(f"<tr><td>ctrl_summary.csv (последняя)</td><td>{html.escape(' | '.join(ctrl or []) if ctrl else '-')}</td></tr>")
        html_buf.append(f"<tr><td>HAProxy backend/server</td><td>{html.escape(bk or '-')} / {html.escape(srv or '-')}</td></tr>")
        html_buf.append("</table>")

        # --- HAProxy действия (через очередь) ---
        if self.haproxy_ops_queue and self.toggle_secret and self.haproxy_backends:
            be = self.haproxy_backends[0]
            html_buf.append("<div class='card'><div class='hd'>HAProxy</div><div class='bd'>")
            html_buf.append("<div class='small' style='color:#666;margin-bottom:6px'>Действия выполняются через очередь <code>haproxy_ops</code> на HAProxy-хосте.</div>")
            for op, label in (("enable", "Enable"), ("disable", "Disable"), ("drain", "Drain")):
                html_buf.append(
                    "<form method='post' action='/haproxy' style='display:inline-block;margin-right:6px'>"
                    f"<input type='hidden' name='op' value='{html.escape(op)}'/>"
                    "<input type='hidden' name='scope' value='runtime'/>"
                    f"<input type='hidden' name='backend' value='{html.escape(be)}'/>"
                    f"<input type='hidden' name='server' value='{html.escape(node)}'/>"
                    "<input type='password' name='secret' placeholder='секрет'/> "
                    f"<button class='btn' type='submit'>{html.escape(label)} (runtime)</button></form>"
                )
            for op, label in (("comment", "Comment in cfg"), ("uncomment", "Uncomment in cfg")):
                html_buf.append(
                    "<form method='post' action='/haproxy' style='display:inline-block;margin:6px 6px'>"
                    f"<input type='hidden' name='op' value='{html.escape(op)}'/>"
                    "<input type='hidden' name='scope' value='cfg'/>"
                    f"<input type='hidden' name='backend' value='{html.escape(be)}'/>"
                    f"<input type='hidden' name='server' value='{html.escape(node)}'/>"
                    "<input type='password' name='secret' placeholder='секрет'/> "
                    f"<button class='btn warn' type='submit'>{html.escape(label)}</button></form>"
                )
            html_buf.append(
                "<form method='post' action='/haproxy' style='display:inline-block;margin-left:6px'>"
                "<input type='hidden' name='op' value='weight'/>"
                "<input type='hidden' name='scope' value='runtime'/>"
                f"<input type='hidden' name='backend' value='{html.escape(be)}'/>"
                f"<input type='hidden' name='server' value='{html.escape(node)}'/>"
                "weight: <input type='text' name='weight' value='1' size='3'/> "
                "<input type='password' name='secret' placeholder='секрет'/> "
                "<button class='btn' type='submit'>Set weight</button></form>"
            )
            html_buf.append("</div></div>")
        html_buf.append("</div></div>")

        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_logs(self, qs: Dict[str, List[str]]):
        host = THIS_HOST; q = (qs.get("q", [""])[0]).strip()
        html_buf = [HTML_HEAD]
        html_buf.append(f"<h1>Логи <small>{html.escape(host)}</small> <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append("<div class='nav'><a class='btn' href='/'>← Все</a> <a class='btn' href='/queue'>Очередь</a> <a class='btn' href='/rules'>Правила</a></div>")
        html_buf.append("<div class='card'><div class='hd'>Ошибки/диспетчер</div><div class='bd'>")
        files = list_error_logs(self.error_log_dir or "")
        for it in files:
            if q and q not in it["path"]:
                continue
            html_buf.append(f"<h3>{html.escape(it['path'])}</h3><div class='mono'>{html.escape('\\n'.join(tail_lines(it['path'], 200)))}</div>")
        html_buf.append("</div></div>")
        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_status(self, qs: Dict[str, List[str]]):
        host = THIS_HOST
        html_buf = [HTML_HEAD]
        html_buf.append(f"<h1>Status <small>{html.escape(host)}</small> <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append("<div class='card'><div class='bd'><table class='kv'>")
        html_buf.append(f"<tr><td>flags</td><td>{html.escape(self.flag_dir or '-')}</td></tr>")
        html_buf.append(f"<tr><td>controller report</td><td>{html.escape(self.controller_dir or '-')}</td></tr>")
        html_buf.append(f"<tr><td>worker reports</td><td>{html.escape(':'.join(self.worker_dirs or []))}</td></tr>")
        html_buf.append(f"<tr><td>error logs</td><td>{html.escape(self.error_log_dir or '-')}</td></tr>")
        html_buf.append(f"<tr><td>haproxy cfg</td><td>{html.escape(HAPROXY_CFG)}</td></tr>")
        html_buf.append(f"<tr><td>haproxy backends</td><td>{html.escape(', '.join(self.haproxy_backends or []))}</td></tr>")
        with _MAP_LOCK:
            pt = _PARSED_TS; cnt = len(_PARSED_MAP or {})
        html_buf.append(f"<tr><td>cfg parsed</td><td>{html.escape(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pt)) if pt else '-')}, servers: {cnt}</td></tr>")
        html_buf.append("</table></div></div>")
        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_rules(self, qs: Dict[str, List[str]], method: str = "GET"):
        host = THIS_HOST
        if method == "POST":
            secret = (qs.get("secret", [""])[0]).strip()
            to = (qs.get("profile", [""])[0]).strip()
            if not self.toggle_secret or secret != self.toggle_secret:
                self.send_error(403, "Forbidden"); return
            ok, _msg = switch_rules("prod" if to == "prod" else "safe")
            self.send_response(303); self.send_header("Location", "/rules"); self.end_headers(); return
        # GET
        html_buf = [HTML_HEAD]
        html_buf.append(f"<h1>Правила <small>{html.escape(host)}</small> <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append("<div class='nav'><a class='btn' href='/'>← Все</a></div>")
        prof, tgt = active_rules_profile()
        html_buf.append("<div class='card'><div class='bd'><table class='kv'>")
        html_buf.append(f"<tr><td>active</td><td>{html.escape(prof)}</td></tr>")
        html_buf.append(f"<tr><td>target</td><td>{html.escape(tgt or '-')}</td></tr>")
        html_buf.append("</table>")
        html_buf.append("<form method='post' action='/rules'>"
                        "<input type='password' name='secret' placeholder='секрет'/> "
                        "<button class='btn prod' name='profile' value='prod'>prod</button> "
                        "<button class='btn safe' name='profile' value='safe'>safe</button>"
                        "</form>")
        html_buf.append("</div></div>")
        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_queue(self, qs: Dict[str, List[str]], method: str = "GET"):
        host = THIS_HOST
        q, ip, d, f = q_paths(self.flag_dir or "")
        if method == "POST":
            secret = (qs.get("secret", [""])[0]).strip()
            act = (qs.get("action", [""])[0]).strip()
            name = (qs.get("name", [""])[0]).strip()
            if not self.toggle_secret or secret != self.toggle_secret:
                self.send_error(403, "Forbidden"); return
            if act == "clear" and name in ("queue", "inprogress", "done", "failed"):
                try:
                    dirp = {"queue": q, "inprogress": ip, "done": d, "failed": f}[name]
                    for x in os.listdir(dirp):
                        if x.endswith(".json"):
                            os.unlink(os.path.join(dirp, x))
                except Exception: pass
            self.send_response(303); self.send_header("Location", "/queue"); self.end_headers(); return

        html_buf = [HTML_HEAD]
        html_buf.append(f"<h1>Очередь <small>{html.escape(host)}</small> <span class='right'>{self._header_with_rules()}</span></h1>")
        html_buf.append(self._peers_bar())
        html_buf.append("<div class='nav'><a class='btn' href='/'>← Все</a> <a class='btn' href='/logs'>Логи</a></div>")

        def _sec(title: str, dirp: str):
            html_buf.append(f"<div class='card'><div class='hd'>{html.escape(title)}</div><div class='bd'>")
            items = list_json(dirp)
            if not items:
                html_buf.append("<div class='small'>empty</div>")
            else:
                html_buf.append("<table><tr><th>file</th><th>mtime</th><th>size</th></tr>")
                for it in items:
                    html_buf.append("<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                        html.escape(it["name"]),
                        html.escape(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(it["mtime"]))),
                        str(it["size"]),
                    ))
                html_buf.append("</table>")
            html_buf.append("</div></div>")

        _sec("queue", q); _sec("inprogress", ip); _sec("done", d); _sec("failed", f)
        html_buf.append("<div class='card'><div class='hd'>Очистка</div><div class='bd'>")
        for n in ("queue", "inprogress", "done", "failed"):
            html_buf.append(
                "<form method='post' action='/queue' style='display:inline-block;margin:4px'>"
                f"<input type='hidden' name='name' value='{html.escape(n)}'/>"
                "<input type='password' name='secret' placeholder='секрет'/> "
                f"<button class='btn red' type='submit' name='action' value='clear'>Очистить {html.escape(n)}</button>"
                "</form>"
            )
        html_buf.append("</div></div>")
        html_buf.append(HTML_TAIL.format(host=html.escape(host), now=html.escape(ts())))
        self._write_html("".join(html_buf))

    def handle_haproxy(self, qs: Dict[str, List[str]]):
        secret = (qs.get("secret", [""])[0]).strip()
        if not self.toggle_secret or secret != self.toggle_secret:
            self.send_error(403, "Forbidden"); return

        op = (qs.get("op", [""])[0]).strip()
        scope = (qs.get("scope", ["runtime"])[0]).strip()
        backend = (qs.get("backend", [""])[0]).strip()
        server = (qs.get("server", [""])[0]).strip()
        weight = (qs.get("weight", [""])[0]).strip()

        # через очередь haproxy_ops
        if op and backend and server and self.haproxy_ops_queue:
            extra: Dict[str, Any] = {}
            if op == "weight":
                try: extra["weight"] = int(weight or "1")
                except Exception: extra["weight"] = 1
            ok, res = enqueue_haproxy_op(self.haproxy_ops_queue, op, scope or "runtime", backend, server, extra=extra)
            if ok:
                self.send_response(303); self.send_header("Location", "/node?name=" + server); self.end_headers()
            else:
                self.send_error(500, f"enqueue error: {res}")
            return

        # прямая runtime-команда (совместимость)
        node = (qs.get("node", [""])[0]).strip()
        action = (qs.get("action", [""])[0]).strip()  # enable|disable|drain
        if not node or action not in ("enable", "disable", "drain"):
            self.send_error(400, "Bad request"); return

        backend2, server2, _ = get_node_info(node)
        if not backend2 or not server2:
            self.send_error(404, f"Backend/server mapping not found for node: {node}"); return

        def _do():
            ok, _out = haproxy_set_state(backend2, server2, action)
            try: sys.stderr.write(f"[HAPROXY {action}] {backend2}/{server2} -> {'OK' if ok else 'FAIL'}\n")
            except Exception: pass

        submit_async(_do)
        self.send_response(303); self.send_header("Location", "/node?name=" + node); self.end_headers()

    def handle_haproxy_cfg(self, qs: Dict[str, List[str]]):
        secret = (qs.get("secret", [""])[0]).strip()
        if not self.toggle_secret or secret != self.toggle_secret:
            self.send_error(403, "Forbidden"); return

        node = (qs.get("node", [""])[0]).strip()
        action = (qs.get("action", [""])[0]).strip()  # comment|uncomment
        if not node or action not in ("comment", "uncomment"):
            self.send_error(400, "Bad request"); return

        do_comment = (action == "comment")

        def _do():
            ok, note = edit_haproxy_cfg_server(node, do_comment)
            try: sys.stderr.write(f"[HAPROXY_CFG {action}] {node} -> {'OK' if ok else 'FAIL'} ({note})\n")
            except Exception: pass

        submit_async(_do)
        self.send_response(303); self.send_header("Location", "/node?name=" + node); self.end_headers()

# ---- serve & main ----
def serve(flag_dir: str,
          controller_dir: str,
          worker_dirs: List[str],
          error_log_dir: str,
          port: int = 35072,
          refresh: int = 5,
          toggle_secret: Optional[str] = None,
          haproxy_ops_queue: Optional[str] = None,
          haproxy_backends: Optional[str] = None,
          peers: Optional[List[Dict[str, str]]] = None):
    class _H(Handler): pass
    _H.flag_dir = flag_dir
    _H.controller_dir = controller_dir
    _H.worker_dirs = [d for d in (worker_dirs or []) if d]
    _H.error_log_dir = error_log_dir
    _H.refresh = int(refresh)
    _H.toggle_secret = toggle_secret
    _H.haproxy_ops_queue = haproxy_ops_queue
    _H.haproxy_backends = [x.strip() for x in (haproxy_backends or "").split(",") if x.strip()]
    _H.peers = peers or []
    httpd = ThreadingHTTPServer(("0.0.0.0", int(port)), _H)
    sys.stdout.write(f"[{ts()}] Monitor at http://0.0.0.0:{int(port)}\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stdout.write(f"\n[{ts()}] Monitor stopping...\n")

def parse_args(argv: List[str]):
    ap = argparse.ArgumentParser(description="JBoss Monitor (Py3.11 + path_utils)")
    ap.add_argument("--flag-dir", default=DEFAULT_FLAG_DIR, help="папка флагов (restart_*.txt / done_*.txt)")
    ap.add_argument("--controller-report-dir", default=DEFAULT_CONTROLLER_REPORTS, help="корень отчётов контроллера (/report и /report/<NODE>)")
    ap.add_argument("--worker-report-dirs", default=DEFAULT_WORKER_REPORT_DIRS, help="корни отчётов воркеров через ':' (каждый сканируется с подкаталогами)")
    ap.add_argument("--error-log-dir", default=DEFAULT_LOG_DIR, help="корень logs/ (будут просмотрены и подкаталоги)")
    ap.add_argument("--port", type=int, default=35072)
    ap.add_argument("--refresh-sec", type=int, default=5)
    ap.add_argument("--toggle-secret", help="секрет для /rules, /queue и haproxy-* действий")
    ap.add_argument("--haproxy-ops-queue", default=str(SIGNALS_DIR / "haproxy_ops"), help="куда класть заявки HAProxy (runtime/cfg)")

    # HAProxy runtime/config
    ap.add_argument("--haproxy-socket", default=HAPROXY_SOCKET, help="Unix socket HAProxy Runtime API")
    ap.add_argument("--haproxy-tcp", default=HAPROXY_TCP, help="host:port для Runtime API, напр. 127.0.0.1:9999")
    ap.add_argument("--haproxy-cfg", default=HAPROXY_CFG, help="путь к /etc/haproxy/haproxy.cfg")
    ap.add_argument("--haproxy-reload-cmd", default=HAPROXY_RELOAD_CMD, help="команда reload, напр. 'systemctl reload haproxy'")
    ap.add_argument("--haproxy-backends", default="Jboss_client",
                    help="список backend’ов через запятую (например: Jboss_client,Jboss_services_8282)")
    ap.add_argument("--haproxy-parse-interval", type=int, default=HAPROXY_PARSE_INTERVAL_SEC,
                    help="период пересканирования cfg, сек")

    # Воркеры
    ap.add_argument("--workers", type=int, default=32, help="количество асинхронных воркеров")

    # Peers (вкладки)
    ap.add_argument("--peer-tabs", default="",
                    help="Список peers через запятую. Форматы: host | host:port | label@host | label@host:port. Пример: primary@55.51,55.52,55.146:35072,55.147")
    return ap.parse_args(argv)

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    worker_dirs = [p for p in (args.worker_report_dirs or "").split(":") if p]

    # применяем CLI-настройки HAProxy
    HAPROXY_SOCKET = args.haproxy_socket
    HAPROXY_TCP = args.haproxy_tcp
    HAPROXY_CFG = args.haproxy_cfg
    HAPROXY_RELOAD_CMD = args.haproxy_reload_cmd
    HAPROXY_BACKENDS = [x.strip() for x in (args.haproxy_backends or "").split(",") if x.strip()]
    HAPROXY_PARSE_INTERVAL_SEC = int(args.haproxy_parse_interval)

    # воркеры
    start_job_workers(n=args.workers)

    # периодический парсер cfg
    schedule_cfg_parser()

    # peers
    peers = parse_peers(args.peer_tabs or "", args.port)

    serve(flag_dir=args.flag_dir,
          controller_dir=args.controller_report_dir,
          worker_dirs=worker_dirs,
          error_log_dir=args.error_log_dir,
          port=args.port,
          refresh=args.refresh_sec,
          toggle_secret=args.toggle_secret,
          haproxy_ops_queue=args.haproxy_ops_queue,
          haproxy_backends=args.haproxy_backends,
          peers=peers)
