#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
rules_tester.py — оффлайн-прогон правил по архиву логов (Python 2.7)
Интеграция: пишет подробный JSON-отчёт в report/rules_test/, краткий CSV-резюме,
опционально публикует событие в signals/events/ и возвращает код ошибки,
если превышены пороги (см. --fail-on-*).

Пример:
  python2 rules_tester.py \
    --logs-dir /var/log/jboss/archive \
    --rules /tmp/pattern_controller/report/rules.json \
    --report-dir /tmp/pattern_controller/report \
    --signals-dir /tmp/pattern_controller/signals \
    --grep node_150 \
    --from "2025-08-10 00:00:00" \
    --to   "2025-08-12 23:59:59" \
    --sample 200000 \
    --examples-per-rule 20 \
    --fail-on-critical 10 \
    --fail-on-restart  5 \
    --emit-event
"""

import os, sys, re, csv, json, time, datetime as dt
import argparse

# ---------- utils ----------

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def parse_dt(s):
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(time.mktime(time.strptime(s, fmt)))
        except:
            pass
    return None

def safe_decode(b):
    if b is None: return u""
    if isinstance(b, unicode): return b
    try: return b.decode("utf-8","ignore")
    except: 
        try: return b.decode("latin-1","ignore")
        except: return u""

def safe_slice(s, n=400):
    try:
        if isinstance(s, unicode): u = s
        else: u = safe_decode(s)
        return u[:n]
    except: return u""

def list_files_recursive(root):
    files=[]
    for d,_,names in os.walk(root):
        for n in names:
            p=os.path.join(d,n)
            try:
                if os.path.isfile(p): files.append(p)
            except: pass
    files.sort()
    return files

def load_rules(path):
    try:
        data = json.load(open(path,"rb"))
    except Exception as e:
        sys.stderr.write("rules read error: %s\n" % e)
        return []
    rules=[]
    for i,r in enumerate(data):
        pat = r.get("pattern") or ""
        sev = (r.get("severity","info") or "info").lower()
        act = (r.get("action","notify") or "notify").lower()
        try:
            rx = re.compile(pat, re.I|re.U)
        except Exception as e:
            sys.stderr.write("bad regex #%d: %s (%s)\n" % (i, pat, e))
            rx = None
        rules.append({"idx": i, "pattern": pat, "severity": sev, "action": act, "rx": rx})
    return rules

def emit_event(signals_dir, text, severity="info", source="rules_tester"):
    if not signals_dir: return
    evdir = os.path.join(signals_dir, "events")
    ensure_dir(evdir)
    tsid = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    path = os.path.join(evdir, "ev_%s_rules_test.json" % tsid)
    obj = {"ts": tsid, "severity": severity, "source": source, "text": text}
    try:
        open(path,"wb").write(json.dumps(obj))
    except: pass

# ---------- core ----------

def test_rules_on_logs(logs_dir, rules, sample_limit, grep, ts_from, ts_to, examples_per_rule):
    results = {}
    for r in rules:
        results[r["idx"]] = {
            "pattern": r["pattern"],
            "severity": r["severity"],
            "action": r["action"],
            "count": 0,
            "examples": []
        }

    files = list_files_recursive(logs_dir)
    total_lines = 0

    for p in files:
        try:
            data = open(p,"rb").read()
        except Exception as e:
            sys.stderr.write("read error %s: %s\n" % (p, e))
            continue

        text = safe_decode(data)
        for line in text.splitlines():
            if sample_limit and total_lines >= sample_limit:
                break
            total_lines += 1

            if grep and (grep.lower() not in line.lower()):
                continue

            if ts_from or ts_to:
                # попытка разобрать дату в начале строки: YYYY-mm-dd HH:MM:SS
                epoch = None
                s = line[:19]
                try:
                    epoch = int(time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S")))
                except:
                    epoch = None
                if epoch is not None:
                    if ts_from and epoch < ts_from: continue
                    if ts_to   and epoch > ts_to:   continue

            for r in rules:
                if not r["rx"]: continue
                if r["rx"].search(line):
                    x = results[r["idx"]]
                    x["count"] += 1
                    if len(x["examples"]) < examples_per_rule:
                        x["examples"].append(safe_slice(line))

    return results, total_lines

def write_report(report_dir, rules_path, logs_dir, results, total_lines):
    ensure_dir(report_dir)
    tsid = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out_json = os.path.join(report_dir, "rules_test_%s.json" % tsid)
    out_csv  = os.path.join(report_dir, "rules_test_summary.csv")

    payload = {
        "timestamp": tsid,
        "rules_file": rules_path,
        "logs_dir": logs_dir,
        "total_lines_scanned": total_lines,
        "rules": results
    }
    # JSON (полный отчёт)
    tmp = out_json + ".tmp"
    try:
        open(tmp,"wb").write(json.dumps(payload, ensure_ascii=False, indent=2))
        os.rename(tmp, out_json)
    except Exception as e:
        sys.stderr.write("write json error: %s\n" % e)

    # CSV (накопительный краткий отчёт)
    try:
        new = not os.path.exists(out_csv)
        f = open(out_csv, "ab")
        w = csv.writer(f)
        if new:
            w.writerow(["timestamp","rules_file","logs_dir","rule_idx","severity","action","pattern","count"])
        for idx, r in results.items():
            w.writerow([tsid, rules_path, logs_dir, idx, r["severity"], r["action"], r["pattern"], r["count"]])
        f.close()
    except Exception as e:
        sys.stderr.write("write csv error: %s\n" % e)

    return out_json, out_csv

def main():
    ap = argparse.ArgumentParser(description="Offline rules tester with reporting (Py2.7)")
    ap.add_argument("--logs-dir", required=True, help="директория с логами (рекурсивно)")
    ap.add_argument("--rules", required=True, help="rules.json / rules_safe.json")
    ap.add_argument("--report-dir", default="/tmp/pattern_controller/report/rules_test",
                    help="куда писать отчёты (JSON+CSV)")
    ap.add_argument("--signals-dir", default="/tmp/pattern_controller/signals",
                    help="куда публиковать события (events/)")

    ap.add_argument("--grep", help="фильтр по подстроке (например, node_150)")
    ap.add_argument("--from", dest="from_ts", help="начало окна: 'YYYY-mm-dd HH:MM:SS' или 'YYYY-mm-dd'")
    ap.add_argument("--to",   dest="to_ts",   help="конец окна: 'YYYY-mm-dd HH:MM:SS' или 'YYYY-mm-dd'")
    ap.add_argument("--sample", type=int, default=0, help="ограничить число строк (0=без лимита)")
    ap.add_argument("--examples-per-rule", type=int, default=20, help="сколько примеров сохранять на правило")

    # пороги отказа (non-zero exit) для CI/операторов
    ap.add_argument("--fail-on-critical", type=int, default=0, help="если суммарно critical > N → exit 1")
    ap.add_argument("--fail-on-restart",  type=int, default=0, help="если суммарно action==restart > N → exit 1")

    ap.add_argument("--emit-event", action="store_true", help="положить краткое событие в signals/events/")
    args = ap.parse_args()

    rules = load_rules(args.rules)
    if not rules:
        sys.stderr.write("no rules loaded\n")
        sys.exit(2)

    ts_from = parse_dt(args.from_ts)
    ts_to   = parse_dt(args.to_ts)

    results, total_lines = test_rules_on_logs(
        args.logs_dir, rules, int(args.sample), args.grep, ts_from, ts_to, int(args.examples_per_rule)
    )

    out_json, out_csv = write_report(args.report_dir, args.rules, args.logs_dir, results, total_lines)

    # агрегаты по порогам
    sum_critical = 0
    sum_restart  = 0
    for _, r in results.items():
        if r["severity"] == "critical":
            sum_critical += r["count"]
        if r["action"] == "restart":
            sum_restart += r["count"]

    msg = ("rules_tester: scanned=%d, critical=%d, restart=%d, report=%s"
           % (total_lines, sum_critical, sum_restart, out_json))

    if args.emit_event:
        sev = ("critical" if (args.fail_on_critical and sum_critical > args.fail_on_critical) or
                           (args.fail_on_restart  and sum_restart  > args.fail_on_restart) else "info")
        emit_event(args.signals_dir, msg, sev)

    # exit code по порогам
    if args.fail_on_critical and sum_critical > args.fail_on_critical:
        sys.stderr.write("FAIL: critical=%d > %d\n" % (sum_critical, args.fail_on_critical))
        sys.exit(1)
    if args.fail_on_restart and sum_restart > args.fail_on_restart:
        sys.stderr.write("FAIL: restart=%d > %d\n" % (sum_restart, args.fail_on_restart))
        sys.exit(1)

    sys.stdout.write("OK %s\n" % msg)

if __name__ == "__main__":
    main()
