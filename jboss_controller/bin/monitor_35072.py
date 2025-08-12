#!/usr/bin/env python
# -*- coding: utf-8 -*-
# monitor_35072.py — Py2.7
# Дашборд + проблемные + логи + правила + очередь /queue + очистка
# + HAProxy Runtime API (enable/disable/drain) через socket/TCP
# + Редактирование haproxy.cfg (comment/uncomment server <node>) + reload
# + Асинхронная очередь задач (32 воркера)
# + Автопарсер haproxy.cfg: backend-и (по умолчанию Jboss_client), автообновление 60с
# + Вкладки «Peers» для переключения между точками входа (55.51/55.52/55.146/55.147 и т.п.)

import BaseHTTPServer, SocketServer, urlparse
import os, sys, time, json, cgi, traceback, csv, socket as pysock
import datetime as dt
import shutil
import threading
import pipes
import tempfile
import re

try:
    import Queue as queue  # Py2
except:
    import queue

DEFAULT_PORT = 35072
BASE_DIR                   = "/tmp/pattern_controller"
DEFAULT_FLAG_DIR           = BASE_DIR + "/signals"
DEFAULT_CONTROLLER_REPORTS = BASE_DIR + "/report"
DEFAULT_WORKER_REPORT_DIRS = BASE_DIR + "/report"
DEFAULT_LOG_DIR            = BASE_DIR + "/logs"

# rules files
RULES_PROD_PATH            = DEFAULT_CONTROLLER_REPORTS + "/rules.json"
RULES_SAFE_PATH            = DEFAULT_CONTROLLER_REPORTS + "/rules_safe.json"
RULES_ACTIVE_LINK          = DEFAULT_CONTROLLER_REPORTS + "/current_rules.json"

# --- HAProxy runtime/config defaults (переопределяются CLI-флагами) ---
HAPROXY_SOCKET = "/var/lib/haproxy/haproxy.sock"   # Unix socket
HAPROXY_TCP    = None                              # "127.0.0.1:9999" — если используешь TCP socket
HAPROXY_CFG    = "/etc/haproxy/haproxy.cfg"
HAPROXY_RELOAD_CMD = "systemctl reload haproxy"
HAPROXY_BACKENDS = ["Jboss_client"]                # будем парсить эти backend’ы из cfg
HAPROXY_PARSE_INTERVAL_SEC = 60

# Ручные соответствия (если нужно что-то дописать поверх парсера)
MANUAL_NODE_TO_BACKEND = {
    # "node_97":  ("Jboss_client","node_97"),
}

# Автокарта из парсера + lock
_PARSED_MAP = {}
_PARSED_TS  = 0.0
_MAP_LOCK   = threading.Lock()

HTML_HEAD = u"""<!doctype html><html><head>
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
  var q = (document.getElementById('q').value||'').toLowerCase();
  var cards = document.getElementsByClassName('nodecard');
  for(var i=0;i<cards.length;i++){
    var t = cards[i].getAttribute('data-node') + ' ' + cards[i].getAttribute('data-state');
    cards[i].style.display = (t.toLowerCase().indexOf(q)>=0) ? '' : 'none';
  }
}
</script>
</head><body><div class="wrap">
"""

HTML_TAIL = u"""<div class="footer">JBoss Monitor • host: {host} • time: {now}</div></div></body></html>"""

def now(): return dt.datetime.now()
def ts():  return now().strftime("%Y-%m-%d %H:%M:%S")
def human_dt(epoch):
    try: return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))
    except: return "-"

def tail_lines(path, n=200, max_bytes=2*1024*1024):
    try:
        size = os.path.getsize(path)
        with open(path,'rb') as f:
            if size>max_bytes: f.seek(-max_bytes, os.SEEK_END)
            data = f.read()
        try: text = data.decode('utf-8')
        except:
            try: text = data.decode('latin-1','ignore')
            except: text = data.decode('utf-8','ignore')
        lines = text.splitlines()
        return lines[-int(n):] if n>0 else lines
    except Exception as e:
        return ["<unable to read: %s>" % e]

# ---- files enumeration ----
def list_worker_logs(worker_dirs):
    files=[]
    for d in worker_dirs or []:
        if not d or not os.path.isdir(d): continue
        for name in os.listdir(d):
            if name.startswith("worker_") and name.endswith(".log"):
                p=os.path.join(d,name)
                try:
                    st=os.stat(p); files.append({"path":p,"size":st.st_size,"mtime":st.st_mtime})
                except: pass
    files.sort(key=lambda x:x["mtime"], reverse=True)
    return files

def list_controller_reports(controller_dir):
    files=[]
    if controller_dir and os.path.isdir(controller_dir):
        for name in os.listdir(controller_dir):
            if name=="controller_summary.csv" or name.startswith("op_"):
                p=os.path.join(controller_dir,name)
                try:
                    st=os.stat(p); files.append({"path":p,"size":st.st_size,"mtime":st.st_mtime})
                except: pass
    files.sort(key=lambda x:x["mtime"], reverse=True)
    return files

def list_error_logs(log_dir):
    files=[]
    if log_dir and os.path.isdir(log_dir):
        for name in os.listdir(log_dir):
            cond_log = (name.endswith(".log") and (name.startswith("node_") or name.startswith("worker_") or name=="dispatcher.log"))
            cond_disp = (name == "dispatcher.log")
            if cond_log or cond_disp:
                p=os.path.join(log_dir,name)
                try:
                    st=os.stat(p); files.append({"path":p,"size":st.st_size,"mtime":st.st_mtime})
                except: pass
    files.sort(key=lambda x:x["mtime"], reverse=True)
    return files

def detect_nodes(flag_dir, worker_dirs, controller_dir):
    nodes=set()
    if flag_dir and os.path.isdir(flag_dir):
        for name in os.listdir(flag_dir):
            if name.startswith("restart_") and name.endswith(".txt"):
                nodes.add(name[len("restart_"):-4])
            if name.startswith("done_") and name.endswith(".txt"):
                nodes.add(name[len("done_"):-4])
    for d in worker_dirs or []:
        if not d or not os.path.isdir(d): continue
        for name in os.listdir(d):
            if name.startswith("worker_") and name.endswith(".log"):
                parts=name.split("_")
                if len(parts)>=3: nodes.add(parts[1])
    csvp=os.path.join(controller_dir,"controller_summary.csv") if controller_dir else None
    if csvp and os.path.exists(csvp):
        try:
            with open(csvp,"rb") as f:
                r=csv.reader(f)
                headers=None
                for row in r:
                    if headers is None: headers=row; continue
                    if len(row)>=3: nodes.add(row[2])
        except: pass
    return sorted(nodes)

def read_done_flag(flag_dir, node):
    p=os.path.join(flag_dir,"done_%s.txt"%node)
    if not os.path.exists(p): return None
    try:
        with open(p,"rb") as f: data=f.read()
        st=os.stat(p)
        return {"path":p,"mtime":st.st_mtime,"raw":data.strip()}
    except: return None

def restart_flag_present(flag_dir, node):
    p=os.path.join(flag_dir,"restart_%s.txt"%node)
    if os.path.exists(p):
        try: st=os.stat(p); return True, st.st_mtime
        except: return True, None
    return (False, None)

def guess_verify(raw_done):
    if not raw_done: return None
    s = raw_done.lower()
    if "verify=ok" in s or " verify ok" in s: return "OK"
    if "verify=fail" in s or " verify fail" in s: return "FAIL"
    return None

def last_worker_log_for_node(worker_dirs, node):
    candidates=[]
    for d in worker_dirs or []:
        if not d or not os.path.isdir(d): continue
        for name in os.listdir(d):
            if name.startswith("worker_%s_"%node) and name.endswith(".log"):
                p=os.path.join(d,name)
                try: st=os.stat(p); candidates.append((st.st_mtime,p))
                except: pass
    if not candidates: return None
    candidates.sort(reverse=True)
    return candidates[0][1]

def last_controller_op_for_node(controller_dir, node):
    csvp=os.path.join(controller_dir,"controller_summary.csv") if controller_dir else None
    if not csvp or not os.path.exists(csvp): return None
    last=None
    try:
        with open(csvp,"rb") as f:
            r=csv.reader(f)
            headers=None
            for row in r:
                if headers is None: headers=row; continue
                if len(row)<11: continue
                if row[2]==node: last=row
    except: return None
    return last

# ---- rules profile ----
def active_rules_profile():
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
    except:
        return ("unknown", None)

def switch_rules(to_profile):
    target = RULES_PROD_PATH if to_profile=="prod" else RULES_SAFE_PATH
    if not os.path.exists(target):
        return (False, "target rules file not found: %s" % target)
    try:
        temp_link = RULES_ACTIVE_LINK + ".tmp"
        try:
            if os.path.lexists(temp_link): os.unlink(temp_link)
        except: pass
        os.symlink(os.path.relpath(target, os.path.dirname(RULES_ACTIVE_LINK)), temp_link)
        try:
            if os.path.lexists(RULES_ACTIVE_LINK): os.unlink(RULES_ACTIVE_LINK)
        except: pass
        os.rename(temp_link, RULES_ACTIVE_LINK)
        return (True, "switched to %s" % to_profile)
    except Exception as e:
        try:
            if os.path.lexists(temp_link): os.unlink(temp_link)
        except: pass
        return (False, "switch error: %s" % e)

# ---- queue helpers ----
def q_paths(flag_dir):
    q= os.path.join(flag_dir,"queue")
    ip=os.path.join(flag_dir,"inprogress")
    d= os.path.join(flag_dir,"done")
    f= os.path.join(flag_dir,"failed")
    return q,ip,d,f

def list_json(dirpath):
    items=[]
    if dirpath and os.path.isdir(dirpath):
        for name in os.listdir(dirpath):
            if name.endswith(".json"):
                p=os.path.join(dirpath,name)
                try:
                    st=os.stat(p)
                    items.append({"path":p,"name":name,"mtime":st.st_mtime,"size":st.st_size})
                except: pass
    items.sort(key=lambda x:x["name"])
    return items

def ensure_dir(p):
    try:
        if p and not os.path.isdir(p): os.makedirs(p)
    except: pass

def enqueue_haproxy_op(qdir, op, scope, backend, server, extra=None):
    """
    Кладём JSON-файл в очередь haproxy_ops.
    op: enable|disable|drain|weight|comment|uncomment
    scope: runtime|cfg|both
    """
    ensure_dir(qdir)
    import uuid
    rid = str(uuid.uuid4())[:8]
    tsid = now().strftime("%Y%m%d_%H%M%S")
    payload = {"id": rid, "ts": tsid, "op": op, "scope": scope,
               "backend": backend, "server": server}
    if extra:
        try: payload.update(extra)
        except: pass
    name = "rq_%s_%s_%s_%s.json" % (tsid, backend or "be", server or "srv", rid)
    path = os.path.join(qdir, name)
    try:
        with open(path, "wb") as f:
            f.write(json.dumps(payload))
        return True, path
    except Exception as e:
        return False, str(e)

def load_json_safe(path):
    try: return json.load(open(path,"rb"))
    except: return None

# ---- HAProxy Runtime API (socket/TCP) ----
def _haproxy_send(cmd, timeout=2):
    data = (cmd + "\n")
    # AF_UNIX сначала
    if HAPROXY_SOCKET and os.path.exists(HAPROXY_SOCKET):
        try:
            s = pysock.socket(pysock.AF_UNIX, pysock.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect(HAPROXY_SOCKET)
            s.sendall(data)
            out = []
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                out.append(chunk)
            try: s.close()
            except: pass
            try: txt = b"".join(out).decode("utf-8","ignore")
            except: txt = b"".join(out)
            return True, txt
        except Exception as e:
            return False, "unix error: %s" % e

    # AF_INET (TCP) при наличии
    if HAPROXY_TCP:
        try:
            host, port = HAPROXY_TCP.split(":",1)
            port = int(port)
            s = pysock.socket(pysock.AF_INET, pysock.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(data)
            out=[]
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                out.append(chunk)
            try: s.close()
            except: pass
            try: txt = b"".join(out).decode("utf-8","ignore")
            except: txt = b"".join(out)
            return True, txt
        except Exception as e:
            return False, "tcp error: %s" % e

    return False, "no runtime endpoint (socket/tcp) configured"

def haproxy_set_state(backend, server, action):
    if action == "disable":
        return _haproxy_send("disable server %s/%s" % (backend, server))
    elif action == "enable":
        return _haproxy_send("enable server %s/%s" % (backend, server))
    elif action == "drain":
        return _haproxy_send("set server %s/%s state drain" % (backend, server))
    else:
        return (False, "unknown action: %s" % action)

# ---- haproxy.cfg edit helpers ----
def _atomic_write(path, content_bytes):
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".haproxy_cfg.", dir=d)
    try:
        os.write(fd, content_bytes)
        os.close(fd)
        os.rename(tmp, path)
    except:
        try: os.close(fd)
        except: pass
        try:
            if os.path.exists(tmp): os.unlink(tmp)
        except: pass
        raise

def _read_file_bytes(path):
    with open(path, "rb") as f:
        return f.read()

def _reload_haproxy():
    return os.system("/bin/sh -lc %s" % pipes.quote(HAPROXY_RELOAD_CMD))

def edit_haproxy_cfg_server(node_name, do_comment):
    if not os.path.isfile(HAPROXY_CFG):
        return (False, "cfg not found: %s" % HAPROXY_CFG)
    try:
        raw = _read_file_bytes(HAPROXY_CFG)
        try: text = raw.decode("utf-8")
        except:
            try: text = raw.decode("latin-1")
            except: text = raw.decode("utf-8","ignore")

        rx = re.compile(r'^(\s*)(#\s*)?(server\s+%s\b.*)$' % re.escape(node_name), re.M)

        changed = [False]
        def _repl(m):
            indent, hashpart, rest = m.group(1), m.group(2), m.group(3)
            if do_comment:
                if hashpart:
                    return m.group(0)  # уже закомментировано
                changed[0] = True
                return indent + "# " + rest
            else:
                if not hashpart:
                    return m.group(0)  # уже активно
                changed[0] = True
                return indent + rest

        new_text = rx.sub(_repl, text)
        if not changed[0]:
            return (False, "no matching line changed (maybe already desired)")

        # бэкап
        try:
            backup_path = HAPROXY_CFG + ".bak_" + time.strftime("%Y%m%d_%H%M%S")
            with open(backup_path, "wb") as fb:
                fb.write(raw)
        except:
            pass

        _atomic_write(HAPROXY_CFG, new_text.encode("utf-8"))
        rc = _reload_haproxy()
        if rc == 0:
            return (True, "cfg updated & haproxy reloaded")
        else:
            return (True, "cfg updated, but reload rc=%s" % rc)
    except Exception as e:
        return (False, "edit error: %s" % e)

# ---- Парсер haproxy.cfg: backend’ы -> карта серверов ----
def _parse_backends_from_cfg(cfg_path, backend_names):
    """
    Возвращает dict:
      server_name -> {"backend": <name>, "server": <server_name>, "commented": bool}
    Парсит только указанные backend’ы.
    """
    result = {}
    try:
        raw = _read_file_bytes(cfg_path)
        try: text = raw.decode("utf-8")
        except: text = raw.decode("latin-1","ignore")
    except:
        return result

    lines = text.splitlines()
    cur = None
    for s in lines:
        line = s.rstrip("\r\n")
        st = line.strip()
        if st.startswith(("global","defaults","frontend","listen","backend")):
            # смена секции
            if st.startswith("backend "):
                name = st.split(None,1)[1].strip()
                cur = name if (name in backend_names) else None
            else:
                cur = None
            continue
        if not cur:
            continue
        # серверные строки (и закомментированные тоже)
        m = re.match(r'^\s*(#\s*)?server\s+([^\s]+)\s+(\S+)\s*(.*)$', line)
        if m:
            commented = bool(m.group(1))
            srv_name  = m.group(2)
            result[srv_name] = {"backend": cur, "server": srv_name, "commented": commented}
    return result

def get_node_map():
    """Сливает MANUAL + PARSED карту (parsed имеет приоритет)."""
    with _MAP_LOCK:
        m = dict(MANUAL_NODE_TO_BACKEND)
        for k, v in _PARSED_MAP.items():
            m[k] = (v["backend"], v["server"])
        return m

def get_node_info(node):
    """Возвращает (backend, server, commented) для ноды из парсера/ручной карты."""
    with _MAP_LOCK:
        if node in _PARSED_MAP:
            info = _PARSED_MAP[node]
            return (info["backend"], info["server"], bool(info.get("commented")))
        if node in MANUAL_NODE_TO_BACKEND:
            bk, srv = MANUAL_NODE_TO_BACKEND[node]
            return (bk, srv, None)
    return (None, None, None)

def schedule_cfg_parser():
    """Переодический парсинг cfg — раз в HAPROXY_PARSE_INTERVAL_SEC."""
    def _tick():
        global _PARSED_MAP, _PARSED_TS
        try:
            pm = _parse_backends_from_cfg(HAPROXY_CFG, HAPROXY_BACKENDS)
            with _MAP_LOCK:
                _PARSED_MAP = pm
                _PARSED_TS = time.time()
        except:
            try: sys.stderr.write("CFG PARSE ERR:\n%s\n" % traceback.format_exc())
            except: pass
        t = threading.Timer(HAPROXY_PARSE_INTERVAL_SEC, _tick)
        t.daemon = True
        t.start()
    _tick()

# ---- Async job queue (32 workers) ----
try:
    import threading
    _ = threading
except:
    pass

try:
    JOBQ = queue.Queue()
except:
    JOBQ = None

def _job_worker():
    while True:
        try:
            fn, args, kwargs = JOBQ.get()
            try:
                fn(*args, **(kwargs or {}))
            finally:
                JOBQ.task_done()
        except Exception:
            try:
                sys.stderr.write("JOB ERR:\n%s\n" % traceback.format_exc())
            except:
                pass

def start_job_workers(n=32):
    if JOBQ is None: return
    for _ in range(int(n)):
        t = threading.Thread(target=_job_worker)
        t.daemon = True
        t.start()

def submit_async(fn, *args, **kwargs):
    if JOBQ is None:
        return fn(*args, **(kwargs or {}))
    JOBQ.put((fn, args, kwargs))

# ---- Peers (переключение точек входа) ----
def parse_peers(peers_str, default_port):
    """
    Форматы:
      host
      host:port
      label@host
      label@host:port
    Возвращает: [{"label": "...", "url": "http://host:port/"}]
    """
    res=[]
    for tok in (peers_str or "").split(","):
        t = tok.strip()
        if not t: continue
        label = None
        hostport = t
        if "@" in t:
            parts = t.split("@", 1)
            if len(parts)==2:
                label, hostport = parts[0].strip(), parts[1].strip()
        if ":" in hostport:
            host, port = hostport.split(":",1)
            host, port = host.strip(), port.strip()
            if not port: port = str(default_port)
        else:
            host, port = hostport, str(default_port)
        if not label: label = host
        url = "http://%s:%s/" % (host, port)
        res.append({"label": label, "url": url})
    return res

# ---- HTTP server ----
class ThreadingHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
    daemon_threads=True
    allow_reuse_address=True

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    flag_dir=None
    haproxy_ops_queue=None
    haproxy_backends=[]
    controller_dir=None
    worker_dirs=[]
    error_log_dir=None
    refresh=5
    toggle_secret=None
    peers=[]

    def do_GET(self):
        try:
            parsed=urlparse.urlparse(self.path)
            path=parsed.path
            qs=urlparse.parse_qs(parsed.query or "")
            if path in ("/","/index","/index.html"):
                self.handle_index(qs, problems_only=False)
            elif path=="/problems":
                self.handle_index(qs, problems_only=True)
            elif path=="/node":
                self.handle_node(qs)
            elif path=="/logs":
                self.handle_logs(qs)
            elif path=="/view":
                self.handle_view(qs)
            elif path=="/view_json":
                self.handle_view_json(qs)
            elif path=="/status":
                self.handle_status(qs)
            elif path=="/rules":
                self.handle_rules(qs, method="GET")
            elif path=="/queue":
                self.handle_queue(qs, method="GET")
            else:
                self.send_error(404,"Not Found")
        except Exception as e:
            sys.stderr.write("ERR: %s\n%s\n"%(e, traceback.format_exc()))
            try: self.send_error(500,"Internal error")
            except: pass

    def do_POST(self):
        try:
            parsed=urlparse.urlparse(self.path)
            length = int(self.headers.getheader('Content-Length','0') or '0')
            raw = self.rfile.read(length) if length>0 else ''
            qs = urlparse.parse_qs(raw)
            if parsed.path == "/rules":
                self.handle_rules(qs, method="POST")
            elif parsed.path == "/queue":
                self.handle_queue(qs, method="POST")
            elif parsed.path == "/haproxy":
                self.handle_haproxy(qs)
            elif parsed.path == "/haproxy_cfg":
                self.handle_haproxy_cfg(qs)
            else:
                self.send_error(404,"Not Found")
        except Exception as e:
            sys.stderr.write("POST ERR: %s\n%s\n"%(e, traceback.format_exc()))
            try: self.send_error(500,"Internal error")
            except: pass

    def write_html(self, html):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type","text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def peers_bar(self):
        if not self.peers: return u""
        cur_host = pysock.gethostname()
        buf=[u"<div class='peers'><span class='small'>Peers:</span> "]
        for p in self.peers:
            label = p.get("label") or "-"
            url = p.get("url") or "#"
            buf.append(u"<a class='btn' href='%s'>%s</a>" % (cgi.escape(url), cgi.escape(label)))
        buf.append(u"</div>")
        return u"".join(buf)

    # ---- common UI bits ----
    def header_with_rules(self):
        prof, tgt = active_rules_profile()
        label = {"prod":"боевой","safe":"safe","custom":"custom","unknown":"unknown"}.get(prof, prof)
        cls = {"prod":"prod","safe":"safe","custom":"info","unknown":"warn"}.get(prof,"info")
        with _MAP_LOCK:
            pt = _PARSED_TS
        sub = " • cfg-parsed: %s" % (time.strftime("%H:%M:%S", time.localtime(pt)) if pt else "-")
        badge = '<span class="badge %s">rules: %s</span><span class="small">%s</span>' % (cls, cgi.escape(label), cgi.escape(sub))
        return badge

    def _collect_nodes(self):
        nodes = detect_nodes(self.flag_dir, self.worker_dirs, self.controller_dir)
        items=[]
        problems=0
        for n in nodes:
            has_restart, r_mtime = restart_flag_present(self.flag_dir, n)
            done = read_done_flag(self.flag_dir, n)
            ver = guess_verify(done["raw"]) if done else None
            ctrl = last_controller_op_for_node(self.controller_dir, n)
            wlog = last_worker_log_for_node(self.worker_dirs, n)
            is_problem = has_restart or (ver=="FAIL")
            if is_problem: problems+=1
            bk, srv, commented = get_node_info(n)
            items.append({
                "node": n,
                "has_restart": has_restart,
                "restart_mtime": r_mtime,
                "done": done,
                "verify": ver,
                "ctrl": ctrl,
                "wlog": wlog,
                "problem": is_problem,
                "hap_backend": bk,
                "hap_server": srv,
                "cfg_commented": commented,
            })
        items.sort(key=lambda x: (not x["problem"], x["node"]))
        return items, problems

    def handle_index(self, qs, problems_only=False):
        host=pysock.gethostname()
        items, problems = self._collect_nodes()
        html=[HTML_HEAD]
        html.append(u'<h1>JBoss Monitor <small>host: %s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/">Все</a> <a class="btn" href="/problems">Проблемные (%d)</a> <a class="btn" href="/logs">Логи</a> <a class="btn" href="/rules">Правила</a> <a class="btn" href="/queue">Очередь</a></div>' % problems)
        html.append(u'<div class="card"><div class="hd">Ноды</div><div class="bd"><input id="q" type="text" placeholder="фильтр" onkeyup="ffilter()"/><div class="row">')
        for it in items:
            if problems_only and not it["problem"]: continue
            n = it["node"]
            st = ("problem" if it["problem"] else "ok")
            cfg = it["cfg_commented"]
            cfgb = ('<span class="badge off">cfg: OFF</span>' if cfg is True
                    else ('<span class="badge ok">cfg: ON</span>' if cfg is False else ''))
            html.append(u'<div class="card nodecard" data-node="%s" data-state="%s"><div class="hd">%s %s</div><div class="bd">' %
                        (cgi.escape(n), cgi.escape(st), cgi.escape(n), cfgb))
            html.append(u'<a class="btn" href="/node?name=%s">Открыть</a>' % cgi.escape(n))
            if it["has_restart"]:
                html.append(u' <span class="badge warn">restart flag</span>')
            if it["verify"]=="FAIL":
                html.append(u' <span class="badge fail">verify=FAIL</span>')
            elif it["verify"]=="OK":
                html.append(u' <span class="badge ok">verify=OK</span>')
            html.append(u'</div></div>')
        html.append(u'</div></div></div>')
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_node(self, qs):
        node = (qs.get("name",[""])[0]).strip()
        if not node: self.send_error(400,"name required"); return
        has_restart, r_mtime = restart_flag_present(self.flag_dir, node)
        done = read_done_flag(self.flag_dir, node)
        ver = guess_verify(done["raw"]) if done else None
        ctrl = last_controller_op_for_node(self.controller_dir, node)
        wlog = last_worker_log_for_node(self.worker_dirs, node)
        bk, srv, commented = get_node_info(node)

        host = pysock.gethostname()
        html=[HTML_HEAD]
        badge_cfg = ('<span class="badge off">cfg: OFF</span>' if commented is True
                     else ('<span class="badge ok">cfg: ON</span>' if commented is False else ''))
        html.append(u'<h1>Нода: %s %s <span class="right">%s</span></h1>' % (cgi.escape(node), badge_cfg, self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/">← Все</a> <a class="btn" href="/logs?q=%s">Логи</a> <a class="btn" href="/queue">Очередь</a> <a class="btn" href="/rules">Правила</a></div>' % cgi.escape(node))

        html.append(u'<div class="card"><div class="hd">Состояние</div><div class="bd"><table class="kv">')
        html.append(u'<tr><td>restart flag</td><td>%s %s</td></tr>' % ("YES" if has_restart else "NO", cgi.escape(human_dt(r_mtime)) if r_mtime else ""))
        html.append(u'<tr><td>verify</td><td>%s</td></tr>' % (ver or "-"))
        html.append(u'<tr><td>ctrl_summary.csv (последняя)</td><td>%s</td></tr>' % (cgi.escape(" | ".join(ctrl or [])) if ctrl else "-"))
        html.append(u'<tr><td>worker log</td><td>%s</td></tr>' % (cgi.escape(wlog) if wlog else "-"))
        html.append(u'<tr><td>HAProxy backend/server</td><td>%s / %s</td></tr>' % (cgi.escape(bk or "-"), cgi.escape(srv or "-")))
        html.append(u'</table>')
        # --- HAProxy действия (через очередь) ---
        if self.haproxy_ops_queue and self.toggle_secret and self.haproxy_backends:
            be = self.haproxy_backends[0]
            html.append(u'<div class="card"><div class="hd">HAProxy</div><div class="bd">')
            html.append(u"<div class='small' style='color:#666;margin-bottom:6px'>Действия выполняются через очередь <code>haproxy_ops</code> на HAProxy-хосте.</div>")
            for op,label in (("enable","Enable"),("disable","Disable"),("drain","Drain")):
                html.append(u"<form method='post' action='/haproxy' style='display:inline-block;margin-right:6px'>"
                            u"<input type='hidden' name='op' value='%s'/>"
                            u"<input type='hidden' name='scope' value='runtime'/>"
                            u"<input type='hidden' name='backend' value='%s'/>"
                            u"<input type='hidden' name='server' value='%s'/>"
                            u"<input type='password' name='secret' placeholder='секрет'/> "
                            u"<button class='btn' type='submit'>%s (runtime)</button></form>" %
                            (cgi.escape(op), cgi.escape(be), cgi.escape(node), cgi.escape(label)))
            for op,label in (("comment","Comment in cfg"),("uncomment","Uncomment in cfg")):
                html.append(u"<form method='post' action='/haproxy' style='display:inline-block;margin:6px 6px'>"
                            u"<input type='hidden' name='op' value='%s'/>"
                            u"<input type='hidden' name='scope' value='cfg'/>"
                            u"<input type='hidden' name='backend' value='%s'/>"
                            u"<input type='hidden' name='server' value='%s'/>"
                            u"<input type='password' name='secret' placeholder='секрет'/> "
                            u"<button class='btn warn' type='submit'>%s</button></form>" %
                            (cgi.escape(op), cgi.escape(be), cgi.escape(node), cgi.escape(label)))
            html.append(u"<form method='post' action='/haproxy' style='display:inline-block;margin-left:6px'>"
                        u"<input type='hidden' name='op' value='weight'/>"
                        u"<input type='hidden' name='scope' value='runtime'/>"
                        u"<input type='hidden' name='backend' value='%s'/>"
                        u"<input type='hidden' name='server' value='%s'/>"
                        u"weight: <input type='text' name='weight' value='1' size='3'/> "
                        u"<input type='password' name='secret' placeholder='секрет'/> "
                        u"<button class='btn' type='submit'>Set weight</button></form>" %
                        (cgi.escape(be), cgi.escape(node)))
            html.append(u"</div></div>")
        html.append(u"</div></div>")

        # Логи /done / worker
        html.append(u'<div class="card"><div class="hd">Флаги/логи</div><div class="bd"><div class="row">')
        html.append(u'<div class="col"><div class="mono">%s</div></div>' % cgi.escape("\n".join(tail_lines(done["path"], 120)) if done else "-"))
        html.append(u'<div class="col"><div class="mono">%s</div></div>' % cgi.escape("\n".join(tail_lines(wlog, 120)) if wlog else "-"))
        html.append(u'</div></div></div>')

        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_logs(self, qs):
        host = pysock.gethostname()
        q = (qs.get("q",[""])[0]).strip()
        html=[HTML_HEAD]
        html.append(u'<h1>Логи <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/">← Все</a> <a class="btn" href="/queue">Очередь</a> <a class="btn" href="/rules">Правила</a></div>')
        html.append(u'<div class="card"><div class="hd">Ошибки/диспетчер</div><div class="bd">')
        files = list_error_logs(self.error_log_dir)
        for it in files:
            if q and q not in it["path"]: continue
            html.append(u"<h3>%s</h3><div class='mono'>%s</div>" % (cgi.escape(it["path"]), cgi.escape("\n".join(tail_lines(it["path"], 200)))))
        html.append(u'</div></div>')
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_view(self, qs):
        path = (qs.get("path",[""])[0]).strip()
        n = int((qs.get("n",["200"])[0]).strip() or "200")
        host = pysock.gethostname()
        html=[HTML_HEAD]
        html.append(u'<h1>Просмотр файла <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/logs">← Логи</a></div>')
        if not path or not os.path.isfile(path):
            html.append(u"<div class='card'><div class='bd'>Bad path</div></div>")
        else:
            html.append(u"<div class='card'><div class='bd'><h3>%s</h3><div class='mono'>%s</div></div></div>" %
                        (cgi.escape(path), cgi.escape("\n".join(tail_lines(path, n)))))
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_status(self, qs):
        host = pysock.gethostname()
        html=[HTML_HEAD]
        html.append(u'<h1>Status <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u"<div class='card'><div class='bd'><table class='kv'>")
        html.append(u"<tr><td>flags</td><td>%s</td></tr>" % cgi.escape(self.flag_dir or "-"))
        html.append(u"<tr><td>controller report</td><td>%s</td></tr>" % cgi.escape(self.controller_dir or "-"))
        html.append(u"<tr><td>worker reports</td><td>%s</td></tr>" % cgi.escape(":".join(self.worker_dirs or [])))
        html.append(u"<tr><td>error logs</td><td>%s</td></tr>" % cgi.escape(self.error_log_dir or "-"))
        html.append(u"<tr><td>haproxy cfg</td><td>%s</td></tr>" % cgi.escape(HAPROXY_CFG))
        html.append(u"<tr><td>haproxy backends</td><td>%s</td></tr>" % cgi.escape(", ".join(HAPROXY_BACKENDS)))
        with _MAP_LOCK:
            pt=_PARSED_TS; cnt=len(_PARSED_MAP or {})
        html.append(u"<tr><td>cfg parsed</td><td>%s, servers: %d</td></tr>" % (cgi.escape(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pt)) if pt else "-"), cnt))
        html.append(u"</table></div></div>")
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_rules(self, qs, method="GET"):
        host = pysock.gethostname()
        if method=="POST":
            secret = (qs.get("secret",[""])[0]).strip()
            to = (qs.get("profile",[""])[0]).strip()
            if not self.toggle_secret or secret != self.toggle_secret:
                self.send_error(403, "Forbidden"); return
            ok, msg = switch_rules("prod" if to=="prod" else "safe")
            self.send_response(303)
            self.send_header("Location","/rules")
            self.end_headers()
            return
        # GET
        html=[HTML_HEAD]
        html.append(u'<h1>Правила <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/">← Все</a></div>')
        prof, tgt = active_rules_profile()
        html.append(u"<div class='card'><div class='bd'><table class='kv'>")
        html.append(u"<tr><td>active</td><td>%s</td></tr>" % cgi.escape(prof))
        html.append(u"<tr><td>target</td><td>%s</td></tr>" % cgi.escape(tgt or "-"))
        html.append(u"</table>")
        html.append(u"<form method='post' action='/rules'>")
        html.append(u"<input type='password' name='secret' placeholder='секрет'/> ")
        html.append(u"<button class='btn prod' name='profile' value='prod'>prod</button> ")
        html.append(u"<button class='btn safe' name='profile' value='safe'>safe</button>")
        html.append(u"</form>")
        html.append(u"</div></div>")
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_queue(self, qs, method="GET"):
        host = pysock.gethostname()
        q,ip,d,f = q_paths(self.flag_dir)
        if method=="POST":
            secret = (qs.get("secret",[""])[0]).strip()
            act = (qs.get("action",[""])[0]).strip()
            name = (qs.get("name",[""])[0]).strip()
            if not self.toggle_secret or secret != self.toggle_secret:
                self.send_error(403,"Forbidden"); return
            if act=="clear" and name in ("queue","inprogress","done","failed"):
                try:
                    dirp = {"queue": q, "inprogress": ip, "done": d, "failed": f}[name]
                    for x in os.listdir(dirp):
                        if x.endswith(".json"): os.unlink(os.path.join(dirp, x))
                except: pass
            self.send_response(303); self.send_header("Location","/queue"); self.end_headers(); return

        html=[HTML_HEAD]
        html.append(u'<h1>Очередь <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/">← Все</a> <a class="btn" href="/logs">Логи</a></div>')

        def _sec(title, dirp):
            html.append(u"<div class='card'><div class='hd'>%s</div><div class='bd'>" % title)
            items=list_json(dirp)
            if not items:
                html.append(u"<div class='small'>empty</div>")
            else:
                html.append(u"<table><tr><th>file</th><th>mtime</th><th>size</th></tr>")
                for it in items:
                    html.append(u"<tr><td>%s</td><td>%s</td><td>%s</td></tr>" %
                                (cgi.escape(it["name"]), cgi.escape(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(it["mtime"]))), str(it["size"])))
                html.append(u"</table>")
            html.append(u"</div></div>")

        _sec("queue", q); _sec("inprogress", ip); _sec("done", d); _sec("failed", f)

        html.append(u"<div class='card'><div class='hd'>Очистка</div><div class='bd'>")
        for n in ("queue","inprogress","done","failed"):
            html.append(u"<form method='post' action='/queue' style='display:inline-block;margin:4px'>"
                        u"<input type='hidden' name='name' value='%s'/>"
                        u"<input type='password' name='secret' placeholder='секрет'/> "
                        u"<button class='btn red' type='submit' name='action' value='clear'>Очистить %s</button>"
                        u"</form>" % (cgi.escape(n), cgi.escape(n)))
        html.append(u"</div></div>")

        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

    def handle_haproxy(self, qs):
        secret = (qs.get("secret",[""])[0]).strip()
        if not self.toggle_secret or secret != self.toggle_secret:
            self.send_error(403, "Forbidden"); return

        op     = (qs.get("op", [""])[0]).strip()
        scope  = (qs.get("scope", ["runtime"])[0]).strip()
        backend= (qs.get("backend", [""])[0]).strip()
        server = (qs.get("server", [""])[0]).strip()
        weight = (qs.get("weight", [""])[0]).strip()

        if op and backend and server and self.haproxy_ops_queue:
            extra = {}
            if op == "weight":
                try: extra["weight"] = int(weight or "1")
                except: extra["weight"] = 1
            ok, res = enqueue_haproxy_op(self.haproxy_ops_queue, op, scope or "runtime", backend, server, extra=extra)
            if ok:
                self.send_response(303)
                self.send_header("Location", "/node?name=" + cgi.escape(server))
                self.end_headers()
            else:
                self.send_error(500, "enqueue error: %s" % res)
            return

        node   = (qs.get("node",[""])[0]).strip()
        action = (qs.get("action",[""])[0]).strip()  # enable|disable|drain
        if not node or action not in ("enable","disable","drain"):
            self.send_error(400, "Bad request"); return

        backend2, server2, _ = get_node_info(node)
        if not backend2 or not server2:
            self.send_error(404, "Backend/server mapping not found for node: %s" % node); return

        def _do():
            ok, out = haproxy_set_state(backend2, server2, action)
            try:
                sys.stderr.write("[HAPROXY %s] %s/%s -> %s\n" % (action, backend2, server2, "OK" if ok else "FAIL"))
            except: pass

        submit_async(_do)
        self.send_response(303)
        self.send_header("Location", "/node?name=" + cgi.escape(node))
        self.end_headers()

    def handle_haproxy_cfg(self, qs):
        secret = (qs.get("secret",[""])[0]).strip()
        if not self.toggle_secret or secret != self.toggle_secret:
            self.send_error(403, "Forbidden"); return

        node   = (qs.get("node",[""])[0]).strip()
        action = (qs.get("action",[""])[0]).strip()  # comment|uncomment
        if not node or action not in ("comment","uncomment"):
            self.send_error(400, "Bad request"); return

        do_comment = (action == "comment")

        def _do():
            ok, note = edit_haproxy_cfg_server(node, do_comment)
            try:
                sys.stderr.write("[HAPROXY_CFG %s] %s -> %s (%s)\n" %
                                 (action, node, "OK" if ok else "FAIL", note))
            except:
                pass

        submit_async(_do)
        self.send_response(303)
        self.send_header("Location", "/node?name=" + cgi.escape(node))
        self.end_headers()

    def handle_view_json(self, qs):
        path=(qs.get("path",[""])[0]).strip()
        host=pysock.gethostname()
        html=[HTML_HEAD]
        html.append(u'<h1>JSON <small>%s</small> <span class="right">%s</span></h1>' % (cgi.escape(host), self.header_with_rules()))
        html.append(self.peers_bar())
        html.append(u'<div class="nav"><a class="btn" href="/queue">← Очередь</a></div>')
        if not path or not os.path.isfile(path) or not path.endswith(".json"):
            html.append(u"<div class='card'><div class='bd'>Bad path</div></div>")
        else:
            try:
                data = json.dumps(load_json_safe(path), ensure_ascii=False, indent=2)
            except:
                data = ""
            html.append(u"<div class='card'><div class='bd'><h3>%s</h3><div class='mono'>%s</div></div></div>" %
                        (cgi.escape(path), cgi.escape(data)))
        html.append(HTML_TAIL.format(host=cgi.escape(host), now=cgi.escape(ts())))
        self.write_html(u"".join(html))

def serve(flag_dir, controller_dir, worker_dirs, error_log_dir, port=DEFAULT_PORT, refresh=5, toggle_secret=None, haproxy_ops_queue=None, haproxy_backends=None, peers=None):
    class _H(Handler): pass
    _H.flag_dir=flag_dir
    _H.controller_dir=controller_dir
    _H.worker_dirs=[d for d in (worker_dirs or []) if d]
    _H.error_log_dir=error_log_dir
    _H.refresh=int(refresh)
    _H.toggle_secret=toggle_secret
    _H.haproxy_ops_queue=haproxy_ops_queue
    _H.haproxy_backends=[x.strip() for x in (haproxy_backends or "").split(",") if x.strip()]
    _H.peers = peers or []
    httpd=ThreadingHTTPServer(("0.0.0.0", int(port)), _H)
    sys.stdout.write("[%s] Monitor at http://0.0.0.0:%d\n" % (ts(), int(port)))
    try: httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stdout.write("\n[%s] Monitor stopping...\n" % ts())

def parse_args(argv):
    import argparse
    ap=argparse.ArgumentParser(description="JBoss Monitor (Py2.7): проблемные/логи/правила/очередь + HAProxy runtime + cfg parser + peers")
    ap.add_argument("--flag-dir", default=DEFAULT_FLAG_DIR, help="папка флагов (restart_*.txt / done_*.txt)")
    ap.add_argument("--controller-report-dir", default=DEFAULT_CONTROLLER_REPORTS, help="папка отчётов контроллера")
    ap.add_argument("--worker-report-dirs", default=DEFAULT_WORKER_REPORT_DIRS, help="папки логов воркеров через «:»")
    ap.add_argument("--error-log-dir", default=DEFAULT_LOG_DIR, help="папка logs/ (node_*.log, worker_*.log, dispatcher.log)")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--refresh-sec", type=int, default=5)
    ap.add_argument("--toggle-secret", help="секрет для /rules, /queue и haproxy-* действий")
    ap.add_argument("--haproxy-ops-queue", default=os.path.join(DEFAULT_FLAG_DIR, "haproxy_ops"), help="куда класть заявки HAProxy (runtime/cfg)")

    # HAProxy runtime/config
    ap.add_argument("--haproxy-socket", default=HAPROXY_SOCKET, help="Unix socket HAProxy Runtime API")
    ap.add_argument("--haproxy-tcp", default=HAPROXY_TCP, help="host:port для Runtime API, напр. 127.0.0.1:9999")
    ap.add_argument("--haproxy-cfg", default=HAPROXY_CFG, help="путь к /etc/haproxy/haproxy.cfg")
    ap.add_argument("--haproxy-reload-cmd", default=HAPROXY_RELOAD_CMD, help="команда reload, напр. 'systemctl reload haproxy'")
    ap.add_argument("--haproxy-backends", default="Jboss_client",
                    help="список backend-и для парсинга, через запятую (например: Jboss_client,Jboss_services_8282)")
    ap.add_argument("--haproxy-parse-interval", type=int, default=HAPROXY_PARSE_INTERVAL_SEC,
                    help="период пересканирования cfg, сек")

    # Воркеры
    ap.add_argument("--workers", type=int, default=32, help="количество асинхронных воркеров")

    # Peers (вкладки)
    ap.add_argument("--peer-tabs", default="",
                    help="Список peers через запятую. Форматы: host | host:port | label@host | label@host:port. Пример: primary@55.51,55.52,55.146:35072,55.147")
    return ap.parse_args(argv)

if __name__=="__main__":
    args=parse_args(sys.argv[1:])
    worker_dirs=[p for p in (args.worker_report_dirs or "").split(":") if p]

    # применяем CLI-настройки HAProxy
    HAPROXY_SOCKET = args.haproxy_socket
    HAPROXY_TCP    = args.haproxy_tcp
    HAPROXY_CFG    = args.haproxy_cfg
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
