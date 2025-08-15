#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Агрегатор графиков (Py3.11).
Собирает controller_summary.csv и metrics/agg_1h.json из всех /report/<NODE>/...
Пишет суммарные графики в /tmp/pattern_controller/report/graphs/*.json
"""
from __future__ import annotations
import csv, json, datetime as dt
from pathlib import Path
from collections import defaultdict
from typing import Dict, Any, List
from path_utils import BASE  # общий корень /tmp/pattern_controller

def _nodes_report_dirs() -> list[Path]:
    root = BASE / "report"
    if not root.exists(): return []
    return sorted([p for p in root.iterdir() if p.is_dir()])

def _load_csv(csv_path: Path, per_day, verify_ok, verify_fail):
    if not csv_path.exists(): return
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.reader(f)
            headers = None
            for row in rdr:
                if headers is None:
                    headers = row; continue
                if len(row) < 8: continue
                ts_s = row[0]; note = row[7] if len(row) > 7 else ""
                day = (ts_s.split(" ") or [""])[0]
                if not day: continue
                per_day[day] += 1
                s = (note or "").lower()
                if "verify=ok" in s:   verify_ok[day]   += 1
                if "verify=fail" in s: verify_fail[day] += 1
    except Exception:
        pass

def _load_agg_1h(path: Path) -> List[dict]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict) and x.get("data")]
    if isinstance(obj, dict):
        ts = dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        return [{"ts": ts, "data": obj}]
    return []

def _sum_5xx_per_day(snaps: List[dict]) -> Dict[str, int]:
    out = defaultdict(int)
    for snap in snaps:
        day = (str(snap.get("ts","")).split(" ") or [""])[0]
        data = snap.get("data") or {}
        total = 0
        for _, v in data.items():
            try: total += int((v or {}).get("sum_5xx", 0))
            except Exception: pass
        if day: out[day] = max(out[day], total)
    return out

def main(argv=None) -> int:
    graphs_dir = BASE / "report" / "graphs"
    graphs_dir.mkdir(parents=True, exist_ok=True)

    per_day = defaultdict(int); verify_ok = defaultdict(int); verify_fail = defaultdict(int)
    s5_day = defaultdict(int)

    for rep in _nodes_report_dirs():
        _load_csv(rep / "controller_summary.csv", per_day, verify_ok, verify_fail)
        for p in [rep / "metrics" / "agg_1h.json"]:
            for day, val in _sum_5xx_per_day(_load_agg_1h(p)).items():
                s5_day[day] = max(s5_day[day], val)

    (graphs_dir / "ops_per_day.json").write_text(json.dumps(dict(per_day), ensure_ascii=False, sort_keys=True), encoding="utf-8")
    (graphs_dir / "verify_status.json").write_text(json.dumps({"ok": dict(verify_ok), "fail": dict(verify_fail)}, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    (graphs_dir / "5xx_per_day.json").write_text(json.dumps(dict(s5_day), ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print("graph_builder done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
