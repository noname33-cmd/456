#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обработчик очереди операций для HAProxy (Py3.11).
Очередь общая: /tmp/pattern_controller/signals/haproxy_ops/*.json
Логи и отчёты: /tmp/pattern_controller/(logs|report)/<HOSTNAME>/...
"""
from __future__ import annotations
import argparse, json, os, time, socket, csv
from pathlib import Path
from typing import Any, Dict, Tuple
from haproxy_runtime import HAProxyRuntime
from haproxy_cfg_parser import HAProxyCfg
from path_utils import REPORT_DIR, LOGS_DIR, haproxy_ops_dirs, HOSTNAME as HOSTNAME_SAFE

DEFAULT_QDIR, DEFAULT_IPDIR, DEFAULT_DDIR, DEFAULT_FDIR = haproxy_ops_dirs()

HEAD = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]

def ts() -> str:
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_csv_row(csv_path: Path, row: list[str]) -> None:
    ensure_dir(csv_path.parent)
    new = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEAD)
        w.writerow(row)

def log_line(path: Path, line: str) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    try: print(line)
    except Exception: pass

def handle_runtime(rt: HAProxyRuntime, backend: str, server: str, op: str, data: Dict[str, Any]) -> Tuple[bool, str]:
    if op == "drain":  return True, rt.set_state(backend, server, "drain")
    if op == "enable": return True, rt.set_state(backend, server, "ready")
    if op == "maint":  return True, rt.set_state(backend, server, "maint")
    if op == "weight":
        w = int(data.get("weight", 0)); return True, rt.set_weight(backend, server, w)
    return False, f"unsupported runtime op: {op}"

def handle_cfg(cfg: HAProxyCfg, backend: str, server: str, op: str) -> Tuple[bool, str]:
    if op in ("comment","cfg_disable"):   return cfg.comment_server(backend, server)
    if op in ("uncomment","cfg_enable"):  return cfg.uncomment_server(backend, server)
    return False, f"unsupported cfg op: {op}"

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="HAProxy ops worker (Py3.11)")
    ap.add_argument("--runtime-sock", default="/var/lib/haproxy/haproxy.sock")
    ap.add_argument("--cfg-path", default="/etc/haproxy/haproxy.cfg")
    ap.add_argument("--backends-allow", default="")
    ap.add_argument("--queue", default=str(DEFAULT_QDIR))
    ap.add_argument("--inprogress", default=str(DEFAULT_IPDIR))
    ap.add_argument("--done", default=str(DEFAULT_DDIR))
    ap.add_argument("--failed", default=str(DEFAULT_FDIR))
    ap.add_argument("--report", default=str(REPORT_DIR))
    ap.add_argument("--logs", default=str(LOGS_DIR))
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--interval", type=float, default=0.2)
    args = ap.parse_args(argv)

    qdir = Path(args.queue); ipdir = Path(args.inprogress); ddir = Path(args.done); fdir = Path(args.failed)
    for d in (qdir, ipdir, ddir, fdir): ensure_dir(d)
    report_dir = Path(args.report); logs_dir = Path(args.logs); ensure_dir(report_dir); ensure_dir(logs_dir)

    csv_path = report_dir / "controller_summary.csv"
    host = HOSTNAME_SAFE

    rt = HAProxyRuntime(args.runtime_sock)
    allow = set([x.strip() for x in (args.backends_allow or "").split(",") if x.strip()])
    cfg = HAProxyCfg(args.cfg_path, allowed_backends=allow if allow else None)

    def process_one(path: Path) -> None:
        base = path.name
        ip = ipdir / base
        try:
            path.replace(ip)
        except Exception:
            return
        try:
            obj = json.loads(ip.read_text(encoding="utf-8"))
        except Exception as e:
            log_line(logs_dir / "haproxy_ops_worker.log", f"read error {base}: {e}")
            try: ip.replace((fdir / base))
            except Exception: pass
            return

        backend = obj.get("backend","")
        server  = obj.get("server","")
        scope   = obj.get("scope","runtime")
        op      = obj.get("op","")
        note    = obj.get("note","")
        op_log  = report_dir / f"op_{obj.get('ts','')}_{backend}_{server}.log"

        try:
            if scope == "runtime":  ok, msg = handle_runtime(rt, backend, server, op, obj)
            elif scope == "cfg":    ok, msg = handle_cfg(cfg, backend, server, op)
            else:                   ok, msg = False, f"unsupported scope: {scope}"
        except Exception as e:
            ok, msg = False, str(e)

        log_line(op_log, f"{ts()} {scope}/{op} {backend}/{server} -> {'OK' if ok else 'FAIL'} :: {msg[:1000]}")
        write_csv_row(csv_path, [
            ts(), host, server or "-", "haproxy_op", "info",
            f"{scope}/{op}", ("OK" if ok else "FAIL"), (note or msg)[:3000], str(op_log), "-", "-"
        ])

        dst = ddir if ok else fdir
        try: ip.replace(dst / base)
        except Exception: pass

    if args.loop:
        while True:
            items = sorted([p for p in qdir.glob("*.json") if p.is_file()])
            if not items:
                time.sleep(args.interval); continue
            for p in items: process_one(p)
    else:
        for p in sorted([p for p in qdir.glob("*.json") if p.is_file()]): process_one(p)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
