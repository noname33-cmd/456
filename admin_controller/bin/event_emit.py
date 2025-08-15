#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
event_emit.py — helper для публикации событий в очередь notifier-а.
Пример:
  ./event_emit.py --dir /tmp/pattern_controller/signals/events \
    --severity warn --source manual --text "node_150 drained"
"""
from __future__ import annotations
import argparse, json, time
from pathlib import Path
from path_utils import SIGNALS_DIR

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(SIGNALS_DIR / "events"))
    ap.add_argument("--severity", default="info")
    ap.add_argument("--source", default="manual")
    ap.add_argument("--text", required=True)
    ap.add_argument("--node")
    args = ap.parse_args(argv)

    events_dir = Path(args.dir)
    ensure_dir(events_dir)

    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    base = f"ev_{ts}_{(args.node or 'na')}.json"
    path = events_dir / base
    obj = {"ts": ts, "severity": args.severity, "source": args.source,
           "node": args.node, "text": args.text}
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    print(f"event -> {path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
