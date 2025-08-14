#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics_rebuilder.py — переагрегация исторических jsonl → agg_1m/5m/1h (Py3.11)

Читает все файлы из report/metrics/YYYYMMDD/*.jsonl (если есть) ИЛИ
использует snapshots из report/metrics/raw/YYYYMMDD/*.json (как fallback),
пересчитывает окна и сохраняет снапшоты в:
  - report/metrics/agg_1m.json
  - report/metrics/agg_5m.json
  - report/metrics/agg_1h.json

Опционально: фильтр по backend-ам (--backends).
"""
from __future__ import annotations

import argparse, os, json, datetime as dt, sys
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, List

def parse_line(line: str) -> Dict[str, Any]:
    try:
        return json.loads(line)
    except Exception:
        return {}

def iter_jsonl_files(metrics_dir: Path) -> Iterable[Path]:
    for day_dir in sorted([p for p in metrics_dir.iterdir() if p.is_dir() and p.name.isdigit() and len(p.name)==8]):
        for fn in sorted([x for x in day_dir.iterdir() if x.is_file() and x.name.endswith(".jsonl")]):
            yield fn

def iter_raw_snapshots(metrics_dir: Path) -> Iterable[Path]:
    raw = metrics_dir / "raw"
    for day_dir in sorted([p for p in raw.iterdir() if p.is_dir() and p.name.isdigit() and len(p.name)==8]):
        for fn in sorted([x for x in day_dir.iterdir() if x.is_file() and x.name.endswith(".json")]):
            yield fn

def merge_snapshot(dst: Dict[str, Dict[str, Any]], snap_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    for r in snap_rows:
        px = str(r.get("pxname","")); sv = str(r.get("svname",""))
        if not px or not sv or sv in ("FRONTEND","BACKEND"):
            continue
        key = f"{px}/{sv}"
        cur = dst.get(key) or {}
        cur.update({
            "sum_2xx": int((r.get("hrsp_2xx") or 0)),
            "sum_3xx": int((r.get("hrsp_3xx") or 0)),
            "sum_4xx": int((r.get("hrsp_4xx") or 0)),
            "sum_5xx": int((r.get("hrsp_5xx") or 0)),
            "scur": int((r.get("scur") or 0)),
            "smax": int((r.get("smax") or 0)),
            "qcur": int((r.get("qcur") or 0)),
            "qmax": int((r.get("qmax") or 0)),
        })
        dst[key] = cur
    return dst

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def rebuild(metrics_root: Path, allow_backends: set[str]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], int]:
    agg_1m: Dict[str, Dict[str, Any]] = {}
    agg_5m: Dict[str, Dict[str, Any]] = {}
    agg_1h: Dict[str, Dict[str, Any]] = {}
    count = 0

    used_any = False
    for fp in iter_jsonl_files(metrics_root):
        try:
            for ln in fp.read_text(encoding="utf-8").splitlines():
                obj = parse_line(ln)
                rows = obj.get("rows") or []
                if not isinstance(rows, list):
                    continue
                count += 1
                agg_1m = merge_snapshot(agg_1m, rows)
                agg_5m = merge_snapshot(agg_5m, rows)
                agg_1h = merge_snapshot(agg_1h, rows)
                used_any = True
        except Exception:
            continue

    if not used_any:
        for fp in iter_raw_snapshots(metrics_root):
            try:
                obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue
            rows = obj.get("rows") or []
            if not isinstance(rows, list):
                continue
            count += 1
            agg_1m = merge_snapshot(agg_1m, rows)
            agg_5m = merge_snapshot(agg_5m, rows)
            agg_1h = merge_snapshot(agg_1h, rows)

    if allow_backends:
        def allowed(k: str) -> bool:
            be = (k.split("/",1)+[""])[0]
            return be in allow_backends
        agg_1m = {k:v for k,v in agg_1m.items() if allowed(k)}
        agg_5m = {k:v for k,v in agg_5m.items() if allowed(k)}
        agg_1h = {k:v for k,v in agg_1h.items() if allowed(k)}

    return agg_1m, agg_5m, agg_1h, count

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="metrics rebuilder (Py3.11)")
    ap.add_argument("--metrics-dir", default="/tmp/pattern_controller/report/metrics",
                    help="корень с YYYYMMDD/*.jsonl и raw/")
    ap.add_argument("--backends", default="", help="фильтр по backend-ам, через запятую (пусто = все)")
    args = ap.parse_args(argv)

    mdir = Path(args.metrics_dir)
    if not mdir.is_dir():
        print(f"no metrics dir: {mdir}", file=sys.stderr)
        return 2

    allow = set([x.strip() for x in (args.backends or "").split(",") if x.strip()])
    a1, a5, aH, cnt = rebuild(mdir, allow)

    write_json(mdir / "agg_1m.json", a1)
    write_json(mdir / "agg_5m.json", a5)
    write_json(mdir / "agg_1h.json", aH)
    print(f"Done. records={cnt}, keys: 1m={len(a1)}, 5m={len(a5)}, 1h={len(aH)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
