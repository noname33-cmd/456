#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
watchdog_stuck_jobs.py — мониторинг зависших задач и флагов (Py2.7)

Логика:
  - Если файл в inprogress/ старше --threshold-min → репаблиш обратно в queue/ (один раз),
    либо переместить в failed/ и сгенерировать событие.
  - Если restart_<node>.txt висит дольше --flag-threshold-min и нет done_<node>.txt — событие.

События публикуются в /signals/events/*.json для notifier_telegram.py.
"""

import os, sys, time, json, argparse, shutil
from lock_utils import with_flock

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def list_json(dirp):
    out=[]
    if not dirp or not os.path.isdir(dirp): return out
    for n in os.listdir(dirp):
        if n.endswith(".json"):
            p=os.path.join(dirp,n)
            try: st=os.stat(p); out.append((st.st_mtime,p))
            except: pass
    out.sort()
    return out

def event_emit(events_dir, text, severity="warn", source="watchdog", node=None):
    ensure_dir(events_dir)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    base = "ev_%s_%s.json" % (ts, (node or "na"))
    path = os.path.join(events_dir, base)
    obj = {"ts": ts, "severity": severity, "source": source, "node": node, "text": text}
    try:
        open(path,"wb").write(json.dumps(obj))
    except: pass

def main():
    ap = argparse.ArgumentParser(description="Watchdog for stuck jobs (Py2.7)")
    ap.add_argument("--ops-inprogress", required=True)
    ap.add_argument("--ops-queue", required=True)
    ap.add_argument("--ops-failed", required=True)
    ap.add_argument("--flags", required=True, help="директория с restart_*/done_*")
    ap.add_argument("--events-dir", default="/tmp/pattern_controller/signals/events")
    ap.add_argument("--threshold-min", type=int, default=10)
    ap.add_argument("--flag-threshold-min", type=int, default=15)
    ap.add_argument("--lock", default="/tmp/pattern_controller/locks/watchdog.lock")
    args = ap.parse_args()

    now = time.time()
    thr = args.threshold_min * 60
    fthr = args.flag_threshold_min * 60

    with with_flock(args.lock):
        # 1) inprogress → слишком старые
        for mtime, path in list_json(args.ops_inprogress):
            age = now - mtime
            if age < thr: continue
            base = os.path.basename(path)
            # попробуем republish (перекинуть в queue)
            try:
                shutil.move(path, os.path.join(args.ops_queue, base))
                event_emit(args.events_dir, "republish stale job %s (age %ds)" % (base, int(age)))
                continue
            except:
                pass
            # если не удалось — в failed
            try:
                shutil.move(path, os.path.join(args.ops_failed, base))
                event_emit(args.events_dir, "move stale job to failed %s (age %ds)" % (base, int(age)), severity="critical")
            except:
                event_emit(args.events_dir, "cannot move stale job %s" % base, severity="critical")

        # 2) зависшие restart_* без done_*
        try:
            for n in os.listdir(args.flags):
                if not n.startswith("restart_") or not n.endswith(".txt"): continue
                p = os.path.join(args.flags, n)
                try: st = os.stat(p)
                except: continue
                age = now - st.st_mtime
                if age < fthr: continue
                node = n[len("restart_"):-4]
                donep = os.path.join(args.flags, "done_%s.txt" % node)
                if not os.path.exists(donep):
                    event_emit(args.events_dir, "stuck restart flag for node %s (age %ds)" % (node, int(age)), severity="critical", node=node)
        except:
            pass

    sys.stdout.write("watchdog done\n")

if __name__ == "__main__":
    main()
