# FILE: metrics_rebuilder.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
metrics_rebuilder.py — переагрегация исторических jsonl → agg_1m/5m/1h (Py2.7)

Читает все файлы из report/metrics/YYYYMMDD/*.jsonl (формат строк — как у
stats_collector_haproxy.py), пересчитывает скользящие окна и сохраняет
итоговые снапшоты в:
  - report/metrics/agg_1m.json
  - report/metrics/agg_5m.json
  - report/metrics/agg_1h.json

Опционально: фильтр по backend-ам (--backends).
"""

import os, sys, json, time, argparse
from collections import defaultdict, deque

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def parse_iso(ts):
    # 'YYYY-mm-ddTHH:MM:SS'
    try:
        return int(time.mktime(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")))
    except:
        return None

def iter_jsonl(metrics_dir, backends_allow):
    """
    Итерирует все записи jsonl по возрастанию: сначала по дням, затем по имени файла,
    строки читает последовательно (ожидается, что внутри примерно по времени).
    """
    if not os.path.isdir(metrics_dir):
        return
    days = []
    for name in os.listdir(metrics_dir):
        if len(name) == 8 and name.isdigit() and os.path.isdir(os.path.join(metrics_dir, name)):
            days.append(name)
    days.sort()
    for day in days:
        ddir = os.path.join(metrics_dir, day)
        files = [x for x in os.listdir(ddir) if x.endswith(".jsonl")]
        files.sort()
        for fn in files:
            path = os.path.join(ddir, fn)
            try:
                f = open(path, "rb")
            except:
                continue
            try:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except:
                        continue
                    be = obj.get("backend") or obj.get("pxname") or ""
                    if backends_allow and (be not in backends_allow):
                        continue
                    ts = obj.get("ts") or ""
                    epoch = parse_iso(ts)
                    if epoch is None:
                        continue
                    sv = obj.get("server") or obj.get("svname") or ""
                    rps = int(obj.get("rps", 0) or 0)
                    s4  = int(obj.get("hrsp_4xx", 0) or 0)
                    s5  = int(obj.get("hrsp_5xx", 0) or 0)
                    yield epoch, be, sv, rps, s4, s5
            finally:
                try: f.close()
                except: pass

def snapshot_from_deques(store, wnd_sec):
    """
    store: key -> deque[(t, rps, 4xx, 5xx)]
    Возвращает агрегаты по ключам.
    """
    snap = {}
    for key, dq in store.items():
        if not dq:
            continue
        # чистка старья относительно последнего времени в очереди
        t_now = dq[-1][0]
        t0 = t_now - wnd_sec
        while dq and dq[0][0] < t0:
            dq.popleft()
        if not dq:
            continue
        cnt = len(dq)
        sum_rps = 0; sum4 = 0; sum5 = 0
        rps_vals = []
        for t, r, a4, a5 in dq:
            sum_rps += r; sum4 += a4; sum5 += a5
            rps_vals.append(r)
        avg_rps = float(sum_rps)/cnt if cnt else 0.0
        rps_vals.sort()
        p95 = rps_vals[int(0.95*len(rps_vals))-1] if rps_vals else 0
        snap[key] = {
            "avg_rps": round(avg_rps, 2),
            "sum_4xx": int(sum4),
            "sum_5xx": int(sum5),
            "p95_rps": int(p95),
        }
    return snap

def write_json(path, obj):
    tmp = path + ".tmp"
    try:
        open(tmp, "wb").write(json.dumps(obj, ensure_ascii=False, sort_keys=True))
        os.rename(tmp, path)
    except Exception as e:
        sys.stderr.write("write %s error: %s\n" % (path, e))

def rebuild(metrics_dir, backends_allow):
    # окна
    W1M, W5M, W1H = 60, 300, 3600
    # deques по ключу "backend/server"
    d1m = defaultdict(lambda: deque())
    d5m = defaultdict(lambda: deque())
    d1h = defaultdict(lambda: deque())

    count = 0
    for epoch, be, sv, rps, s4, s5 in iter_jsonl(metrics_dir, backends_allow):
        key = be + "/" + sv
        for store, wnd in ((d1m, W1M), (d5m, W5M), (d1h, W1H)):
            dq = store[key]
            dq.append((epoch, rps, s4, s5))
            t0 = epoch - wnd
            while dq and dq[0][0] < t0:
                dq.popleft()
        count += 1
        if (count % 200000) == 0:
            sys.stdout.write(".. processed %d records\n" % count)

    snap_1m = snapshot_from_deques(d1m, W1M)
    snap_5m = snapshot_from_deques(d5m, W5M)
    snap_1h = snapshot_from_deques(d1h, W1H)
    return snap_1m, snap_5m, snap_1h, count

def main():
    ap = argparse.ArgumentParser(description="Rebuild HAProxy metrics aggregates (Py2.7)")
    ap.add_argument("--metrics-dir", default="/tmp/pattern_controller/report/metrics",
                    help="корень с YYYYMMDD/*.jsonl и agg_*.json")
    ap.add_argument("--backends", default="", help="фильтр по backend-ам, через запятую (пусто = все)")
    args = ap.parse_args()

    if not os.path.isdir(args.metrics_dir):
        sys.stderr.write("no metrics dir: %s\n" % args.metrics_dir)
        sys.exit(2)

    allow = set([x.strip() for x in (args.backends or "").split(",") if x.strip()])

    sys.stdout.write("Rebuilding aggregates from %s...\n" % args.metrics_dir)
    s1, s5, sH, cnt = rebuild(args.metrics_dir, allow)

    write_json(os.path.join(args.metrics_dir, "agg_1m.json"), s1)
    write_json(os.path.join(args.metrics_dir, "agg_5m.json"), s5)
    write_json(os.path.join(args.metrics_dir, "agg_1h.json"), sH)

    sys.stdout.write("Done. records=%d, keys: 1m=%d, 5m=%d, 1h=%d\n" %
                     (cnt, len(s1), len(s5), len(sH)))

if __name__ == "__main__":
    main()
