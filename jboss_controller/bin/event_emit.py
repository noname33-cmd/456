#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
event_emit.py — маленький helper для публикации событий в очередь notifier-а (Py2.7).
Пример:
  python2 event_emit.py --dir /tmp/pattern_controller/signals/events \
    --severity warn --source manual --text "node_150 drained"
"""
import os, sys, time, json, argparse

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="/tmp/pattern_controller/signals/events")
    ap.add_argument("--severity", default="info")
    ap.add_argument("--source", default="manual")
    ap.add_argument("--text", required=True)
    ap.add_argument("--node")
    args = ap.parse_args()

    ensure_dir(args.dir)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    base = "ev_%s_%s.json" % (ts, (args.node or "na"))
    path = os.path.join(args.dir, base)
    obj = {"ts": ts, "severity": args.severity, "source": args.source, "node": args.node, "text": args.text}
    open(path, "wb").write(json.dumps(obj))
    sys.stdout.write("event -> %s\n" % path)

if __name__ == "__main__":
    main()
