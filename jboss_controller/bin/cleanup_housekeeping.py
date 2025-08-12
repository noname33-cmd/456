# FILE: cleanup_housekeeping.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
cleanup_housekeeping.py — ротация логов и очистка старых артефактов (Py2.7)
Пример:
  python2 cleanup_housekeeping.py \
    --logs /tmp/pattern_controller/logs --keep-days 30 \
    --ops-done /tmp/pattern_controller/signals/haproxy_ops_done --ops-keep-days 7 \
    --report /tmp/pattern_controller/report --max-oplogs 2000
"""

import os, sys, time, argparse
from lock_utils import with_flock

def older_than(path, days):
    try:
        age = time.time() - os.path.getmtime(path)
        return age > (days * 86400)
    except:
        return False

def rm_old_in_dir(dirp, days, exts=None, name_prefix=None):
    if not dirp or not os.path.isdir(dirp): return (0,0)
    removed = 0; total = 0
    for name in os.listdir(dirp):
        p = os.path.join(dirp, name)
        if not os.path.isfile(p): continue
        if exts and not any(name.endswith(e) for e in exts): continue
        if name_prefix and not name.startswith(name_prefix): continue
        total += 1
        if older_than(p, days):
            try: os.unlink(p); removed += 1
            except: pass
    return (removed, total)

def cap_total_files(dirp, pattern_prefix, max_keep):
    if not dirp or not os.path.isdir(dirp): return (0,0)
    files = []
    for n in os.listdir(dirp):
        if not n.startswith(pattern_prefix): continue
        p = os.path.join(dirp, n)
        try: st = os.stat(p)
        except: continue
        if not os.path.isfile(p): continue
        files.append((st.st_mtime, p))
    files.sort(reverse=True)
    removed = 0
    for _, p in files[max_keep:]:
        try: os.unlink(p); removed += 1
        except: pass
    return (removed, len(files))

def main():
    ap = argparse.ArgumentParser(description="Housekeeping / cleanup (Py2.7)")
    ap.add_argument("--logs", help="каталог логов (node_*.log, worker_*.log, dispatcher.log)")
    ap.add_argument("--keep-days", type=int, default=30)
    ap.add_argument("--ops-done", help="каталог .../haproxy_ops_done")
    ap.add_argument("--ops-keep-days", type=int, default=7)
    ap.add_argument("--report", help="корень report/ (metrics jsonl, graphs, op_*.log)")
    ap.add_argument("--max-oplogs", type=int, default=2000, help="лимит количества op_*.log")
    ap.add_argument("--lock", default="/tmp/pattern_controller/locks/cleanup.lock")
    args = ap.parse_args()

    with with_flock(args.lock):
        # logs/
        if args.logs:
            rm_old_in_dir(args.logs, args.keep_days, exts=[".log"])
        # signals/haproxy_ops_done
        if args.ops_done:
            rm_old_in_dir(args.ops_done, args.ops_keep_days, exts=[".json"])
        # report/metrics/YYYYMMDD
        if args.report:
            metrics = os.path.join(args.report, "metrics")
            if os.path.isdir(metrics):
                for day in os.listdir(metrics):
                    ddir = os.path.join(metrics, day)
                    if not os.path.isdir(ddir): continue
                    # чистим jsonl, старше keep-days
                    for name in os.listdir(ddir):
                        if not name.endswith(".jsonl"): continue
                        p = os.path.join(ddir, name)
                        if older_than(p, args.keep_days):
                            try: os.unlink(p)
                            except: pass
            # cap op_*.log
            removed,_ = cap_total_files(args.report, "op_", args.max_oplogs)
            _ = removed  # не используем

    sys.stdout.write("cleanup done\n")

if __name__ == "__main__":
    main()
