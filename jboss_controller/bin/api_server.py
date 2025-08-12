# FILE: api_server.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Py2.7 — mini API server over /report (ops + metrics tails)

import os, sys, json, time, argparse
import BaseHTTPServer
import SocketServer
import urlparse

DEFAULT_REPORT_DIR = os.environ.get("PC_REPORT_DIR", "/tmp/pattern_controller/report")
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 35073

def ts():
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------- CSV tail safely (2MB window) --------
def tail_csv_rows(path, limit):
    out_rows = []
    headers = []
    if not (path and os.path.isfile(path)):
        return (headers, out_rows)
    try:
        max_bytes = 2 * 1024 * 1024
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
            data = f.read()
        # decode text
        try:
            text = data.decode("utf-8", "ignore")
        except:
            text = data.decode("latin-1", "ignore")

        import csv
        try:
            from cStringIO import StringIO
        except:
            from StringIO import StringIO

        rdr = csv.reader(StringIO(text))
        for i, row in enumerate(rdr):
            if i == 0:
                headers = row
                continue
            out_rows.append(row)
        if limit and limit > 0:
            out_rows = out_rows[-int(limit):]
        return (headers, out_rows)
    except:
        return (headers, out_rows)

# -------- JSONL tail helper (metrics/events) --------
def tail_jsonl(dirpath, prefix=None, limit=200):
    items = []
    if not (dirpath and os.path.isdir(dirpath)):
        return items
    try:
        # берём свежие файлы, подходящие по префиксу
        files = []
        for name in os.listdir(dirpath):
            if not name.endswith(".jsonl"):
                continue
            if prefix and not name.startswith(prefix):
                continue
            p = os.path.join(dirpath, name)
            try:
                st = os.stat(p)
                files.append((st.st_mtime, p))
            except:
                pass
        files.sort(reverse=True)
        # читаем с конца файлов, пока не наберём limit
        left = int(limit)
        for _, p in files:
            # читаем последнее окно 2MB
            try:
                size = os.path.getsize(p)
                with open(p, "rb") as f:
                    if size > 2*1024*1024:
                        f.seek(-2*1024*1024, os.SEEK_END)
                    chunk = f.read()
                try:
                    txt = chunk.decode("utf-8", "ignore")
                except:
                    txt = chunk.decode("latin-1", "ignore")
                lines = [s for s in txt.splitlines() if s.strip()]
                # последние записи
                for line in reversed(lines):
                    try:
                        items.append(json.loads(line))
                    except:
                        continue
                    left -= 1
                    if left <= 0:
                        return list(reversed(items))
            except:
                continue
        return list(reversed(items))
    except:
        return items

class ThreadingHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    report_dir = DEFAULT_REPORT_DIR

    def _write_json(self, obj, code=200):
        try:
            data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        except:
            data = json.dumps({"error": "json-encode-failed"}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except:
            pass

    def log_message(self, fmt, *args):
        # тише в консоль
        sys.stdout.write("[%s] %s\n" % (ts(), fmt % args))

    def do_GET(self):
        try:
            parsed = urlparse.urlparse(self.path)
            path = parsed.path or "/"
            qs = urlparse.parse_qs(parsed.query or "")

            if path in ("/", "/api/ping"):
                return self._write_json({"ok": True, "time": ts()})

            # /api/ops?limit=200
            if path == "/api/ops":
                limit = 200
                try:
                    limit = int((qs.get("limit", ["200"])[0]).strip() or "200")
                except:
                    pass
                csvp = os.path.join(self.report_dir, "controller_summary.csv")
                headers, rows = tail_csv_rows(csvp, limit)
                # упакуем в dict-списки
                out = []
                for r in rows:
                    item = {}
                    for i, h in enumerate(headers or []):
                        try:
                            item[h] = r[i]
                        except:
                            item[h] = ""
                    out.append(item)
                return self._write_json({"headers": headers, "rows": out, "count": len(out)})

            # /api/metrics?prefix=haproxy&limit=300
            if path == "/api/metrics":
                prefix = (qs.get("prefix", [""])[0]).strip() or None
                try:
                    limit = int((qs.get("limit", ["300"])[0]).strip() or "300")
                except:
                    limit = 300
                dirp = os.path.join(self.report_dir, "metrics")
                items = tail_jsonl(dirp, prefix=prefix, limit=limit)
                return self._write_json({"items": items, "count": len(items)})

            # 404
            self.send_error(404, "Not Found")
        except Exception as e:
            try:
                self.send_error(500, "Internal error: %s" % e)
            except:
                pass

def serve(bind, port, report_dir):
    Handler.report_dir = report_dir
    srv = ThreadingHTTPServer((bind, int(port)), Handler)
    sys.stdout.write("[start] api_server bind=%s port=%s report=%s\n" % (bind, str(port), report_dir))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass

def main():
    ap = argparse.ArgumentParser(description="Mini API over /report (Py2.7)")
    ap.add_argument("--bind", default=DEFAULT_BIND)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--report", default=DEFAULT_REPORT_DIR)
    args = ap.parse_args()
    if not os.path.isdir(args.report):
        sys.stderr.write("WARN: report dir not found: %s\n" % args.report)
    serve(args.bind, args.port, args.report)

if __name__ == "__main__":
    main()
