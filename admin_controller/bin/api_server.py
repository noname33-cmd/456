#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mini API над /report + управляющие POST-эндпоинты.

GET  /health
GET  /ops?limit=N
GET  /graphs
GET  /graphs/<name>
GET  /metrics/agg?name=...
GET  /metrics/raw?limit=N

POST /haproxy/toggle       body: {"action":"drain|disable|enable","backend":"...","server":"..."} или {"action":"...","server":"backend/server"}
POST /queue/retry          body: {} | пусто

AUTH:
  - X-Auth-Token: <TOGGLE_SECRET>  (или ?secret=... в query)
  - ИЛИ X-Signature: v=1;ts=...;nonce=...;sig=<hmac>  (см. controller/auth.py)
  - /health открыт без авторизации, если REQUIRE_AUTH_HEALTH!=1
"""

from __future__ import annotations
import argparse, json, os, time, csv, re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote
from pathlib import Path
from io import StringIO
from datetime import datetime
import subprocess

# корень /tmp/pattern_controller (или твой)
from path_utils import BASE  # Path("/tmp/pattern_controller")

# --- auth backend: HMAC (если есть) или простой токен ---
try:
    from controller.auth import verify_request, AuthError  # HMAC+token
    HAVE_HMAC = True
except Exception:
    HAVE_HMAC = False
    class AuthError(Exception): ...
    def verify_request(method, path_qs, body, headers):
        secret_env = os.environ.get("TOGGLE_SECRET", "").strip()
        if not secret_env:
            raise AuthError("missing TOGGLE_SECRET")
        token = headers.get("X-Auth-Token") or headers.get("X-Query-Secret")
        if token != secret_env:
            raise AuthError("forbidden")

DEFAULT_BIND = "0.0.0.0"
DEFAULT_PORT = 35073

REQUIRE_AUTH_HEALTH = os.environ.get("REQUIRE_AUTH_HEALTH", "0") == "1"
REPORT_ROOT_DEFAULT = BASE / "report"
LOG_DIR = Path(os.environ.get("LOG_DIR", str(BASE / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUTH_FAIL_LOG = LOG_DIR / "api_auth_fail.log"

SAFE_NAME = re.compile(r"^[A-Za-z0-9._:-]+$")
SAFE_ACTIONS = {"drain", "disable", "enable"}
MAX_BODY = int(os.environ.get("MAX_BODY_BYTES", "1048576"))  # 1 MiB

def ts_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _nodes_report_dirs(report_root: Path) -> dict[str, Path]:
    if not report_root.exists(): return {}
    return {p.name: p for p in sorted(report_root.iterdir()) if p.is_dir()}

def _tail_csv_rows(path: Path, limit: int | None):
    rows = []; headers = []
    if not path.exists(): return headers, rows
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        rdr = csv.reader(StringIO(text))
        for i, row in enumerate(rdr):
            if i == 0: headers = row
            else: rows.append(row)
        if limit and limit > 0:
            rows = rows[-int(limit):]
    except Exception:
        pass
    return headers, rows

def _tail_raw_metrics(node_dir: Path, limit: int | None):
    today = time.strftime("%Y%m%d", time.localtime())
    raw_dir = node_dir / "metrics" / "raw" / today
    items = []
    if not raw_dir.is_dir():
        return items
    files = sorted(raw_dir.glob("*.json"))
    if limit and limit > 0:
        files = files[-int(limit):]
    for f in files:
        try:
            items.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue
    return items

def _log_auth_fail(remote: str, path: str, reason: str):
    try:
        with AUTH_FAIL_LOG.open("a", encoding="utf-8") as f:
            f.write(f"{ts_now()} remote={remote} path={path} reason={reason}\n")
    except Exception:
        pass

def _call_safe_toggle(action: str, backend: str, server: str) -> dict:
    """Пробуем импортировать safe_haproxy_toggle; если не вышло — subprocess."""
    try:
        from controller.safe_haproxy_toggle import safe_toggle
        safe_toggle(action, backend, server)
        return {"ok": True, "mode": "import"}
    except Exception as e:
        pc_base = os.environ.get("PC_BASE", "/tmp/pattern_controller")
        script = os.path.join(pc_base, "controller", "safe_haproxy_toggle.py")
        cmd = ["/usr/bin/python3", script, "--action", action, "--backend", backend, "--server", server]
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0:
            return {"ok": False, "mode": "subprocess", "stderr": r.stderr.decode("utf-8", "ignore")}
        return {"ok": True, "mode": "subprocess"}

def _call_retry() -> dict:
    try:
        from controller.safe_haproxy_toggle import retry_deferred_once
        retry_deferred_once()
        return {"ok": True, "mode": "import"}
    except Exception:
        pc_base = os.environ.get("PC_BASE", "/tmp/pattern_controller")
        script = os.path.join(pc_base, "controller", "safe_haproxy_toggle.py")
        r = subprocess.run(["/usr/bin/python3", script, "--action", "retry"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0:
            return {"ok": False, "mode": "subprocess", "stderr": r.stderr.decode("utf-8", "ignore")}
        return {"ok": True, "mode": "subprocess"}

class Handler(BaseHTTPRequestHandler):
    report_root: Path = Path(os.environ.get("REPORT_DIR", str(REPORT_ROOT_DEFAULT)))

    # ---- helpers ----
    def _send_json(self, obj, code=200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try: self.wfile.write(data)
        except BrokenPipeError: pass

    def _bad(self, code: int, msg: str):
        self._send_json({"error": msg, "code": code}, code)

    def _read_body(self) -> bytes:
        cl = self.headers.get("Content-Length")
        if cl is None:
            return b""
        try:
            n = int(cl)
        except Exception:
            self._bad(411, "invalid content-length")
            return b""
        if n < 0 or n > MAX_BODY:
            self._bad(413, "body too large")
            return b""
        return self.rfile.read(n) if n else b""

    def _auth(self, method: str, body: bytes) -> bool:
        # /health можно открыть без auth (если не включён REQUIRE_AUTH_HEALTH)
        if self.path.split("?")[0] == "/health" and not REQUIRE_AUTH_HEALTH:
            return True

        hdrs = {k: v for k, v in self.headers.items()}
        qs = parse_qs(urlparse(self.path).query or "")
        if "secret" in qs and qs["secret"]:
            hdrs["X-Query-Secret"] = qs["secret"][0]

        try:
            verify_request(method, self.path, body, hdrs)
            return True
        except AuthError as e:
            _log_auth_fail(self.client_address[0], self.path, str(e))
            self._bad(403, f"forbidden: {e}")
            return False

    def log_message(self, fmt, *args):
        # поменьше шума
        return

    # ---- endpoints (GET) ----
    def do_GET(self):
        try:
            if not self._auth("GET", b""):
                return

            u = urlparse(self.path)
            qs = parse_qs(u.query or "")
            path = u.path or "/"
            nodes = _nodes_report_dirs(self.report_root)

            if path == "/health":
                return self._send_json({"status": "ok", "ts": ts_now(), "nodes": list(nodes.keys())})

            if path == "/ops":
                try:
                    limit = int(qs.get("limit", ["100"])[0])
                except Exception:
                    return self._bad(400, "bad limit")
                by_node = {}
                for name, ndir in nodes.items():
                    hdr, rows = _tail_csv_rows(ndir / "controller_summary.csv", limit)
                    by_node[name] = {"headers": hdr, "rows": rows}
                return self._send_json({"by_node": by_node})

            if path == "/graphs":
                gdir = self.report_root / "graphs"
                graphs = sorted([p.name for p in gdir.glob("*.json")]) if gdir.is_dir() else []
                return self._send_json({"graphs": graphs})

            if path.startswith("/graphs/"):
                name = unquote(path.split("/", 2)[-1]).strip()
                if not SAFE_NAME.match(name):
                    return self._bad(400, "bad graph name")
                p = (self.report_root / "graphs" / name)
                if p.exists():
                    try:
                        return self._send_json(json.loads(p.read_text(encoding="utf-8")))
                    except Exception:
                        return self._bad(500, "bad json")
                return self._bad(404, "not found")

            if path == "/metrics/agg":
                name = qs.get("name", ["agg_5m.json"])[0]
                if not SAFE_NAME.match(name):
                    return self._bad(400, "bad metrics name")
                by_node = {}
                for n, ndir in nodes.items():
                    p = ndir / "metrics" / name
                    if p.exists():
                        try:
                            by_node[n] = json.loads(p.read_text(encoding="utf-8"))
                        except Exception:
                            by_node[n] = {}
                    else:
                        by_node[n] = {}
                return self._send_json({"by_node": by_node})

            if path == "/metrics/raw":
                try:
                    limit = int(qs.get("limit", ["10"])[0])
                except Exception:
                    return self._bad(400, "bad limit")
                by_node = {}
                for n, ndir in nodes.items():
                    items = _tail_raw_metrics(ndir, limit)
                    by_node[n] = {"items": items, "count": len(items)}
                return self._send_json({"by_node": by_node})

            return self._bad(404, "not found")
        except Exception as e:
            self._bad(500, f"internal: {e}")

    # ---- endpoints (POST) ----
    def do_POST(self):
        try:
            body = self._read_body()
            if body is None:
                return  # уже ответили ошибкой
            if not self._auth("POST", body):
                return

            u = urlparse(self.path)
            path = u.path or "/"

            if path == "/queue/retry":
                res = _call_retry()
                code = 200 if res.get("ok") else 500
                return self._send_json({"ok": bool(res.get("ok")), "details": res}, code)

            if path == "/haproxy/toggle":
                ctype = self.headers.get("Content-Type", "")
                if "application/json" not in ctype:
                    return self._bad(415, "expected application/json")
                try:
                    data = json.loads(body.decode("utf-8"))
                except Exception:
                    return self._bad(400, "invalid json")

                action = (data or {}).get("action", "")
                backend = (data or {}).get("backend", "")
                server  = (data or {}).get("server", "")

                # поддерживаем "backend/server" в server
                if server and "/" in server and not backend:
                    backend, server = server.split("/", 1)

                if action not in SAFE_ACTIONS:
                    return self._bad(400, "bad action")
                if not (backend and server):
                    return self._bad(400, "backend/server required")
                if not (SAFE_NAME.match(backend) and SAFE_NAME.match(server)):
                    return self._bad(400, "bad backend/server format")

                res = _call_safe_toggle(action, backend, server)
                code = 200 if res.get("ok") else 500
                return self._send_json({"ok": bool(res.get("ok")), "details": res}, code)

            return self._bad(404, "not found")
        except Exception as e:
            self._bad(500, f"internal: {e}")

def serve(bind: str, port: int):
    srv = ThreadingHTTPServer((bind, int(port)), Handler)
    print(f"[start] api_server bind={bind} port={port} report_root={Handler.report_root}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try: srv.server_close()
        except Exception: pass

def main(argv=None):
    ap = argparse.ArgumentParser(description="Mini API over /report with auth + HAProxy control")
    ap.add_argument("--bind", default=os.environ.get("API_BIND", DEFAULT_BIND))
    ap.add_argument("--port", type=int, default=int(os.environ.get("API_PORT", DEFAULT_PORT)))
    args = ap.parse_args(argv)
    serve(args.bind, args.port)

if __name__ == "__main__":
    main()
