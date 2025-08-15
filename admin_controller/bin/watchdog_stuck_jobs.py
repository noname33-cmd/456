#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
watchdog_stuck_jobs.py — мониторинг зависших задач и флагов.
Логика:
  - Старые файлы в inprogress/ репаблишим в queue/ или переносим в failed/
  - Долгие restart_<node>.txt без done_<node>.txt — событие в signals/events/
"""
from __future__ import annotations
import argparse, json, shutil, time
from pathlib import Path
from path_utils import SIGNALS_DIR
from lock_utils import with_flock

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def list_json(dirp: Path) -> list[Path]:
    if not dirp.is_dir(): return []
    return sorted([p for p in dirp.iterdir() if p.suffix == ".json"])

def emit_event(events_dir: Path, text: str, severity="warn", source="watchdog", node: str | None = None):
    ensure_dir(events_dir)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    base = f"ev_{ts}_{(node or 'na')}.json"
    (events_dir / base).write_text(json.dumps({"ts": ts, "severity": severity, "source": source, "node": node, "text": text}, ensure_ascii=False), encoding="utf-8")

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue",      default=str(SIGNALS_DIR / "queue"))
    ap.add_argument("--inprogress", default=str(SIGNALS_DIR / "inprogress"))
    ap.add_argument("--failed",     default=str(SIGNALS_DIR / "failed"))
    ap.add_argument("--events",     default=str(SIGNALS_DIR / "events"))
    ap.add_argument("--threshold-min", type=int, default=10)
    ap.add_argument("--flag-dir",   default=str(SIGNALS_DIR))
    ap.add_argument("--flag-threshold-min", type=int, default=15)
    args = ap.parse_args(argv)

    q  = Path(args.queue); ip = Path(args.inprogress); fl = Path(args.failed)
    ev = Path(args.events); fd = Path(args.flag_dir)
    ensure_dir(q); ensure_dir(ip); ensure_dir(fl); ensure_dir(ev)

    now = time.time()
    # 1) зависшие inprogress → queue или failed
    for p in list_json(ip):
        age_min = (now - p.stat().st_mtime) / 60.0
        if age_min < args.threshold_min: continue
        with with_flock(p.with_suffix(".lock"), timeout_sec=1.0):
            try:
                # Один раз репаблишим (если нет маркера)
                marker = p.with_suffix(".reb")
                if not marker.exists():
                    shutil.move(str(p), str(q / p.name))
                    marker.write_text("republished", encoding="utf-8")
                else:
                    shutil.move(str(p), str(fl / p.name))
                    emit_event(ev, f"stuck job moved to failed: {p.name}")
            except Exception:
                continue

    # 2) долгие restart-флаги без done_
    for rp in fd.glob("restart_*.txt"):
        node = rp.stem[len("restart_"):]
        done = fd / f"done_{node}.txt"
        age_min = (now - rp.stat().st_mtime) / 60.0
        if age_min >= args.flag_threshold_min and not done.exists():
            emit_event(ev, f"restart flag is too old without done: {node}", node=node)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
