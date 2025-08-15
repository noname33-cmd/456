#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сбор метрик HAProxy из runtime socket (Py3.11).
Пишет снапшоты в /tmp/pattern_controller/report/<HOSTNAME>/metrics/...
"""
from __future__ import annotations
import argparse, datetime as dt, json, socket
from pathlib import Path
from typing import Dict, Any, List
from path_utils import REPORT_DIR, metrics_root, metrics_raw_dir  # единые пути

def ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def read_runtime_csv(sock_path: str) -> List[Dict[str, str]]:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    try:
        s.sendall(b"show stat\n")
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    finally:
        s.close()
    text = data.decode("utf-8", "replace")
    header = None; rows: List[Dict[str, str]] = []
    for ln in text.splitlines():
        if ln.startswith("# "):
            header = ln[2:].split(","); continue
        if not ln or ln.startswith("#") or not header:
            continue
        parts = ln.split(",")
        row = {header[i]: (parts[i] if i < len(parts) else "") for i in range(len(header))}
        px, sv = row.get("pxname",""), row.get("svname","")
        if not px or not sv or sv in ("FRONTEND","BACKEND"):
            continue
        rows.append(row)
    return rows

def to_int(v: str, default: int = 0) -> int:
    try: return int(v)
    except Exception: return default

def aggregate(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    now_s = ts(); out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = f'{r.get("pxname","")}/{r.get("svname","")}'
        out[key] = {
            "sum_2xx": to_int(r.get("hrsp_2xx","0")),
            "sum_3xx": to_int(r.get("hrsp_3xx","0")),
            "sum_4xx": to_int(r.get("hrsp_4xx","0")),
            "sum_5xx": to_int(r.get("hrsp_5xx","0")),
            "scur": to_int(r.get("scur","0")),
            "smax": to_int(r.get("smax","0")),
            "qcur": to_int(r.get("qcur","0")),
            "qmax": to_int(r.get("qmax","0")),
            "last": now_s,
        }
    return out

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="HAProxy stats collector (Py3.11)")
    ap.add_argument("--sock", default="/var/lib/haproxy/haproxy.sock")
    ap.add_argument("--report", default=str(REPORT_DIR))  # уже /<HOSTNAME>
    ap.add_argument("--raw-keep-days", type=int, default=7)
    args = ap.parse_args(argv)

    # каталоги метрик
    mroot = metrics_root()
    today = dt.datetime.now().strftime("%Y%m%d")
    raw_dir = metrics_raw_dir(today)

    rows = read_runtime_csv(args.sock)
    snap_obj = {"ts": ts(), "rows": rows}
    write_json(mroot / "last.json", snap_obj)
    write_json(raw_dir / f'{dt.datetime.now().strftime("%H%M%S")}.json', snap_obj)

    agg = aggregate(rows)
    write_json(mroot / "agg_1m.json", agg)
    write_json(mroot / "agg_5m.json", agg)
    write_json(mroot / "agg_15m.json", agg)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
