#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# stats_collector_haproxy.py — Py2.7
#
# Сбор HAProxy stats (CSV через runtime socket или HTTP) -> /report/metrics/
#  - metrics/haproxy_stats.jsonl  (сырые срезы)
#  - metrics/agg_1m.json, agg_5m.json, agg_1h.json  (агрегаты)
#
# Запуск (пример):
#   python2 stats_collector_haproxy.py \
#     --socket /var/lib/haproxy/haproxy.sock \
#     --metrics-dir /minjust-74-250/123/pattern_controller/report/metrics \
#     --poll-sec 10
#
# или через HTTP:
#   python2 stats_collector_haproxy.py \
#     --stats-url http://127.0.0.1:8081/haproxy?stats;csv \
#     --metrics-dir /.../report/metrics

import os, sys, time, json, csv, argparse, fcntl, threading, subprocess
try:
    import urllib2
except:
    import urllib.request as urllib2  # на всякий

BASE = os.environ.get("PC_BASE", "/tmp/pattern_controller")
DEFAULT_METRICS_DIR = os.path.join(BASE, "report", "metrics")
DEFAULT_LOGS_DIR    = os.path.join(BASE, "logs")

def ts():
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def log_line(log_path, line):
    ensure_dir(os.path.dirname(log_path))
    data = ("[%s] %s\n" % (ts(), line)).encode("utf-8")
    try:
        f = open(log_path, "ab")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(data); f.flush(); os.fsync(f.fileno())
        finally:
            try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except: pass
            f.close()
    except: pass
    try:
        sys.stdout.write(line + "\n")
    except: pass

# ---- fetchers ----
def fetch_stats_via_socket(sock_path, timeout=3.0):
    # Мини-клиент runtime socket (show stat)
    import socket
    if not (sock_path and os.path.exists(sock_path)):
        return False, "socket not found", ""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(float(timeout))
    try:
        s.connect(sock_path)
        s.sendall(b"show stat\n")
        chunks=[]
        while True:
            try:
                data = s.recv(65536)
            except socket.timeout:
                break
            if not data: break
            chunks.append(data)
        out = b"".join(chunks)
        try:
            txt = out.decode("utf-8", "ignore")
        except:
            txt = out.decode("latin-1", "ignore")
        return True, "ok", txt
    except Exception as e:
        return False, "socket error: %s" % e, ""
    finally:
        try: s.close()
        except: pass

def fetch_stats_via_http(url, timeout=4.0):
    try:
        r = urllib2.urlopen(urllib2.Request(url), timeout=float(timeout))
        data = r.read()
        try:
            txt = data.decode("utf-8", "ignore")
        except:
            txt = data.decode("latin-1", "ignore")
        return True, "ok", txt
    except Exception as e:
        return False, "http error: %s" % e, ""

# ---- parsing ----
def parse_csv_stats(text):
    """
    Возвращает список dict по строкам stats:
      {
        "px": <pxname>,
        "sv": <svname>,
        "type": <FRONTEND|BACKEND|SERVER>,
        "status": <UP|DOWN|OPEN|...>,
        "scur": int,
        "rate": int,
        "hrsp_4xx": int,
        "hrsp_5xx": int
      }
    """
    out = []
    if not text:
        return out

    lines = []
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("#"):
            # пропускаем комментарии-описания
            if line.startswith("# "):
                # иногда заголовок идёт как "# pxname,svname,..."
                lines.append(line[2:])
            continue
        lines.append(line)

    if not lines:
        return out

    # распознаём заголовок
    reader = csv.reader(lines)
    header = None
    for row in reader:
        if not row:
            continue
        if header is None:
            header = row
            # индексы нужных колонок
            idx = {}
            for name in ("pxname","svname","status","scur","rate","hrsp_4xx","hrsp_5xx"):
                try:
                    idx[name] = header.index(name)
                except:
                    idx[name] = None
            continue

        # строки данных
        try:
            px = row[idx["pxname"]] if idx["pxname"] is not None else ""
            sv = row[idx["svname"]] if idx["svname"] is not None else ""
            status = (row[idx["status"]] if idx["status"] is not None else "") or ""
            def _int(v):
                try: return int(v)
                except: return 0
            scur = _int(row[idx["scur"]]) if idx["scur"] is not None else 0
            rate = _int(row[idx["rate"]]) if idx["rate"] is not None else 0
            h4xx = _int(row[idx["hrsp_4xx"]]) if idx["hrsp_4xx"] is not None else 0
            h5xx = _int(row[idx["hrsp_5xx"]]) if idx["hrsp_5xx"] is not None else 0

            if sv in ("FRONTEND","BACKEND"):
                typ = sv
            else:
                typ = "SERVER"

            out.append({
                "px": px, "sv": sv, "type": typ, "status": status,
                "scur": scur, "rate": rate, "hrsp_4xx": h4xx, "hrsp_5xx": h5xx
            })
        except:
            continue

    return out

# ---- IO helpers ----
def append_jsonl(path, obj):
    ensure_dir(os.path.dirname(path))
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    try:
        f = open(path, "ab")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(line); f.flush(); os.fsync(f.fileno())
        finally:
            try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except: pass
            f.close()
    except: pass

def tail_jsonl_since(path, since_ts, max_bytes=4*1024*1024):
    """Читает jsonl c конца (до max_bytes) и отдаёт все записи с ts >= since_ts."""
    items = []
    if not os.path.isfile(path):
        return items
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
            data = f.read()
        try:
            text = data.decode("utf-8", "ignore")
        except:
            text = data.decode("latin-1", "ignore")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except:
                continue
            # obj: {"ts": <epoch>, "items": [...]}
            t = obj.get("ts")
            try:
                t = float(t)
            except:
                t = 0.0
            if t >= since_ts:
                items.append(obj)
    except:
        pass
    return items

def aggregate_window(jsonl_path, win_sec):
    """
    Суммирует за окно времени (win_sec) по ключу (px, sv).
    Возвращает список словарей:
      {"px":..., "sv":..., "sum_5xx":..., "sum_4xx":..., "last_status":..., "last_rate":..., "last_scur":...}
    """
    cutoff = time.time() - float(win_sec)
    slices = tail_jsonl_since(jsonl_path, cutoff)
    sums = {}  # (px, sv) -> dict

    for obj in slices:
        items = obj.get("items") or []
        for it in items:
            px = it.get("px") or ""
            sv = it.get("sv") or ""
            key = (px, sv)
            acc = sums.get(key)
            if not acc:
                acc = {"px": px, "sv": sv, "sum_5xx": 0, "sum_4xx": 0, "last_status": "", "last_rate": 0, "last_scur": 0}
                sums[key] = acc
            try: acc["sum_5xx"] += int(it.get("hrsp_5xx", 0) or 0)
            except: pass
            try: acc["sum_4xx"] += int(it.get("hrsp_4xx", 0) or 0)
            except: pass
            # последние наблюдения перетираем
            st = it.get("status") or ""
            if st: acc["last_status"] = st
            try:
                acc["last_rate"] = int(it.get("rate", 0) or 0)
            except:
                pass
            try:
                acc["last_scur"] = int(it.get("scur", 0) or 0)
            except:
                pass

    # список
    res = []
    for _, v in sums.items():
        res.append(v)
    return res

def write_json(path, obj):
    ensure_dir(os.path.dirname(path))
    data = json.dumps(obj, ensure_ascii=False)
    try:
        f = open(path, "wb")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(data)
            f.flush(); os.fsync(f.fileno())
        finally:
            try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except: pass
            f.close()
    except:
        pass

def main():
    ap = argparse.ArgumentParser(description="HAProxy stats collector (Py2.7)")
    ap.add_argument("--socket", default="/var/lib/haproxy/haproxy.sock")
    ap.add_argument("--stats-url", help="HTTP ;csv URL (fallback/alt)")
    ap.add_argument("--metrics-dir", default=DEFAULT_METRICS_DIR)
    ap.add_argument("--logs-dir", default=DEFAULT_LOGS_DIR)
    ap.add_argument("--poll-sec", type=int, default=10)
    ap.add_argument("--timeout", type=float, default=3.0)
    args = ap.parse_args()

    ensure_dir(args.metrics_dir); ensure_dir(args.logs_dir)
    jsonl_path = os.path.join(args.metrics_dir, "haproxy_stats.jsonl")
    log_path   = os.path.join(args.logs_dir, "haproxy_stats_collector.log")

    host = os.uname()[1]
    log_line(log_path, "START stats_collector on %s" % host)

    while True:
        # 1) fetch
        ok, why, txt = (False, "no source", "")
        if args.socket and os.path.exists(args.socket):
            ok, why, txt = fetch_stats_via_socket(args.socket, timeout=args.timeout)
        if (not ok) and args.stats_url:
            ok, why, txt = fetch_stats_via_http(args.stats_url, timeout=args.timeout)

        if not ok:
            log_line(log_path, "FETCH FAIL: %s" % why)
            time.sleep(max(1, int(args.poll_sec))); continue

        # 2) parse
        items = parse_csv_stats(txt)
        if not items:
            log_line(log_path, "PARSE WARN: empty items")
        # 3) append raw jsonl
        now_epoch = time.time()
        append_jsonl(jsonl_path, {"ts": now_epoch, "items": items})

        # 4) aggregates
        for win, name in ((60, "agg_1m.json"), (300, "agg_5m.json"), (3600, "agg_1h.json")):
            agg = aggregate_window(jsonl_path, win)
            write_json(os.path.join(args.metrics_dir, name), agg)

        time.sleep(max(1, int(args.poll_sec)))

if __name__ == "__main__":
    main()
