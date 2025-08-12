# FILE: graph_builder.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
graph_builder.py — ночной агрегатор графиков для UI (Py2.7).

Берёт:
  - controller_summary.csv (операции, verify)
  - metrics/agg_1h.json (ошибки 5xx; поддержка двух форматов: list и dict)
И строит:
  - graphs/ops_per_day.json
  - graphs/verify_status.json
  - graphs/5xx_per_day.json
"""

import os, sys, csv, json, time
import argparse
from collections import defaultdict

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def parse_csv_ops(path):
    """
    Возвращает:
      per_day:     { 'YYYY-mm-dd': total_ops_count }
      verify_ok:   { 'YYYY-mm-dd': count }
      verify_fail: { 'YYYY-mm-dd': count }
    """
    per_day = defaultdict(int)
    verify_ok = defaultdict(int)
    verify_fail = defaultdict(int)
    if not os.path.isfile(path):
        return {}, {}, {}
    try:
        with open(path, "rb") as f:
            rdr = csv.reader(f)
            headers = None
            for row in rdr:
                if headers is None:
                    headers = row
                    continue
                if len(row) < 8:
                    continue
                ts   = row[0]
                note = row[7] if len(row) > 7 else ""
                day = (ts.split(" ") or [""])[0]
                if not day:
                    continue
                per_day[day] += 1
                s = (note or "").lower()
                if "verify=ok"   in s: verify_ok[day]   += 1
                if "verify=fail" in s: verify_fail[day] += 1
    except Exception as e:
        try: sys.stderr.write("parse_csv_ops error: %s\n" % e)
        except: pass
    return dict(per_day), dict(verify_ok), dict(verify_fail)

def parse_metrics_5xx(path):
    """
    Читает metrics/agg_1h.json и отдаёт { 'YYYY-mm-dd': sum_5xx_for_that_day }.
    Поддерживает 2 формата:
      1) Список словарей [{'px':..., 'sv':..., 'hrsp_5xx':...}, ...]
      2) Словарь {'backend/server': {'sum_5xx': ...}, ...}
    """
    per_day = defaultdict(int)
    if not os.path.isfile(path):
        return {}
    try:
        data = json.load(open(path, "rb"))
        today = time.strftime("%Y-%m-%d", time.localtime(time.time()))

        if isinstance(data, list):
            total_5xx = 0
            for it in data:
                try:
                    total_5xx += int(it.get("hrsp_5xx", 0) or 0)
                except:
                    pass
            per_day[today] += total_5xx

        elif isinstance(data, dict):
            total_5xx = 0
            for _, val in data.items():
                try:
                    total_5xx += int(val.get("sum_5xx", 0) or 0)
                except:
                    pass
            per_day[today] += total_5xx
    except Exception as e:
        try: sys.stderr.write("parse_metrics_5xx error: %s\n" % e)
        except: pass
    return dict(per_day)

def main():
    ap = argparse.ArgumentParser(description="Build UI graphs (Py2.7)")
    ap.add_argument("--report-dir", default="/tmp/pattern_controller/report")
    args = ap.parse_args()

    summary_csv = os.path.join(args.report_dir, "controller_summary.csv")
    agg_1h      = os.path.join(args.report_dir, "metrics", "agg_1h.json")
    graphs_dir  = os.path.join(args.report_dir, "graphs")
    ensure_dir(graphs_dir)

    per_day, verify_ok, verify_fail = parse_csv_ops(summary_csv)
    s5_day = parse_metrics_5xx(agg_1h)

    try:
        open(os.path.join(graphs_dir, "ops_per_day.json"), "wb").write(
            json.dumps(per_day, ensure_ascii=False, sort_keys=True)
        )
        open(os.path.join(graphs_dir, "verify_status.json"), "wb").write(
            json.dumps({"ok": verify_ok, "fail": verify_fail}, ensure_ascii=False, sort_keys=True)
        )
        open(os.path.join(graphs_dir, "5xx_per_day.json"), "wb").write(
            json.dumps(s5_day, ensure_ascii=False, sort_keys=True)
        )
    except Exception as e:
        try: sys.stderr.write("write graphs error: %s\n" % e)
        except: pass

    try: sys.stdout.write("graph_builder done\n")
    except: pass

if __name__ == "__main__":
    main()
