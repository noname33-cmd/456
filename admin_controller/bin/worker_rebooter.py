#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
worker_rebooter.py — локальный воркер перезапуска узла (Py3.11)

Логика:
  - Если есть signals/restart_<node>.txt:
      1) выполнить --pre-cmd (опционально)
      2) выполнить --restart-cmd (обязательно)
      3) ждать health (tcp-порт и/или HTTP-URL)
      4) выполнить --post-cmd (опционально)
      5) записать signals/done_<node>.txt с verify=OK|FAIL
      6) убрать restart_*.txt (идемпотентно)
  - Пишет логи в /tmp/pattern_controller/logs/<HOST>/worker_<node>.log
  - Добавляет строку в /tmp/pattern_controller/report/<HOST>/controller_summary.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import http.client
import os
import shlex
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Единая точка путей/идентичности
from path_utils import SIGNALS_DIR, REPORT_DIR, LOGS_DIR, HOSTNAME

# ---------- util ----------
def now() -> dt.datetime:
    return dt.datetime.now()

def ts() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _log_open(node: str):
    p = LOGS_DIR / f"worker_{node}.log"
    ensure_dir(p.parent)
    return p.open("a", encoding="utf-8")

def _log_write(fh, line: str) -> None:
    try:
        fh.write(line + "\n"); fh.flush()
    except Exception:
        try:
            sys.stdout.write(line + "\n")
        except Exception:
            pass

def sh(cmd: Optional[str], timeout: int = 120, log_fh=None) -> int:
    """Запуск shell-команды с логированием stdout в лог воркера."""
    if not cmd:
        return 0
    _log_write(log_fh, f"[{ts()}] RUN: {cmd}")
    try:
        proc = subprocess.Popen(
            ["/bin/sh", "-lc", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        start = time.time()
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            _log_write(log_fh, line.rstrip("\n"))
            if timeout > 0 and (time.time() - start) > timeout:
                try:
                    proc.kill()
                except Exception:
                    pass
                _log_write(log_fh, f"[{ts()}] TIMEOUT after {timeout}s")
                return 124
        rc = proc.wait()
        _log_write(log_fh, f"[{ts()}] RC={rc}")
        return int(rc)
    except subprocess.TimeoutExpired:
        _log_write(log_fh, f"[{ts()}] TIMEOUT after {timeout}s (spawn)")
        return 124
    except Exception as e:
        _log_write(log_fh, f"[{ts()}] EXC: {e}")
        return 1

# ---------- health ----------
def tcp_health(addr: str, timeout: float, log_fh=None) -> bool:
    """addr: 'host:port'"""
    try:
        host, port_s = addr.rsplit(":", 1)
        port = int(port_s)
    except Exception:
        _log_write(log_fh, f"[{ts()}] tcp_health: bad addr '{addr}'")
        return False

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                _log_write(log_fh, f"[{ts()}] tcp_health OK: {addr}")
                return True
        except Exception:
            time.sleep(1.0)
    _log_write(log_fh, f"[{ts()}] tcp_health FAIL: {addr} after {timeout}s")
    return False

def http_health(url: str, timeout: float, log_fh=None) -> bool:
    """Поддерживается только http://host:port/path (без TLS)."""
    try:
        assert url.startswith("http://")
        rest = url[7:]
        hostport, _, path = rest.partition("/")
        host, _, port_s = hostport.partition(":")
        port = int(port_s or "80")
        path = "/" + (path or "")
    except Exception:
        _log_write(log_fh, f"[{ts()}] http_health: bad url '{url}'")
        return False

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=3.0)
            conn.request("GET", path)
            resp = conn.getresponse()
            ok = (200 <= resp.status < 300)
            conn.close()
            if ok:
                _log_write(log_fh, f"[{ts()}] http_health OK: {url} ({resp.status})")
                return True
        except Exception:
            time.sleep(1.0)
    _log_write(log_fh, f"[{ts()}] http_health FAIL: {url} after {timeout}s")
    return False

# ---------- CSV summary ----------
HEAD = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]

def append_summary(report_dir: Path, node: str, result_ok: bool, note: str, log_file: Optional[Path]) -> None:
    ensure_dir(report_dir)
    csv_path = report_dir / "controller_summary.csv"
    new = not csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEAD)
        w.writerow([
            ts(), HOSTNAME, node, "worker_rebooter", "info",
            "restart", ("OK" if result_ok else "FAIL"), note[:3000],
            "-", str(log_file) if log_file else "-", "-"
        ])

# ---------- main ----------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Worker rebooter (Py3.11)")
    ap.add_argument("--signals", default=str(SIGNALS_DIR), help="директория с флагами/очередями")
    ap.add_argument("--report",  default=str(REPORT_DIR),   help="директория отчётов этой ноды")
    ap.add_argument("--log-dir", default=str(LOGS_DIR),     help="директория логов этой ноды")
    ap.add_argument("--node", required=True, help="имя ноды (используется в restart_/done_ файлах)")
    ap.add_argument("--pre-cmd")
    ap.add_argument("--restart-cmd", required=True)
    ap.add_argument("--post-cmd")
    ap.add_argument("--health-tcp", help="формат host:port")
    ap.add_argument("--health-http", help="формат http://host:port/path")
    ap.add_argument("--health-timeout", type=int, default=300)
    args = ap.parse_args(argv)

    sig_dir = Path(args.signals); sig_dir.mkdir(parents=True, exist_ok=True)
    rep_dir = Path(args.report);  rep_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(args.log_dir); log_dir.mkdir(parents=True, exist_ok=True)

    f_restart = sig_dir / f"restart_{args.node}.txt"
    f_done    = sig_dir / f"done_{args.node}.txt"
    worker_log = log_dir / f"worker_{args.node}.log"

    # Если нет флага — нечего делать (идемпотентно)
    if not f_restart.exists():
        return 0

    with _log_open(args.node) as lf:
        _log_write(lf, f"[{ts()}] === worker_rebooter start node={args.node}")

        # 1) pre
        rc_pre = sh(args.pre_cmd, timeout=120, log_fh=lf) if args.pre_cmd else 0

        # 2) restart (обязательный)
        restart_timeout = max(60, args.health_timeout // 2)
        rc_restart = sh(args.restart_cmd, timeout=restart_timeout, log_fh=lf)

        # 3) health
        ok = True
        if args.health_tcp:
            ok = ok and tcp_health(args.health_tcp, float(args.health_timeout), log_fh=lf)
        if args.health_http:
            ok = ok and http_health(args.health_http, float(args.health_timeout), log_fh=lf)

        # 4) post
        rc_post = sh(args.post_cmd, timeout=120, log_fh=lf) if args.post_cmd else 0

        # 5) done + summary
        verify = "OK" if (ok and rc_restart == 0) else "FAIL"
        note = f"verify={verify}; rc_pre={rc_pre}; rc_restart={rc_restart}; rc_post={rc_post}"
        f_done.write_text(
            f"ts={ts()}\nnode={args.node}\nverify={verify}\n",
            encoding="utf-8",
        )
        append_summary(rep_dir, args.node, (verify == "OK"), note, worker_log)

        # 6) убрать флаг перезапуска (идемпотентно)
        try:
            f_restart.unlink()
        except Exception:
            pass

        _log_write(lf, f"[{ts()}] === worker_rebooter end node={args.node} result={verify}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
