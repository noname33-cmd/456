#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Queue dispatcher for controlled JBoss restarts (Py3.11)

from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from __future__ import annotations
import argparse, csv, datetime as dt, json, os, pipes, socket, subprocess, threading, time, traceback
from pathlib import Path
from typing import Optional
from path_utils import REPORT_DIR, LOGS_DIR, SIGNALS_DIR, HOSTNAME

import httpx

BASE_DIR = Path("/tmp/pattern_controller")
DEFAULT_FLAG_DIR   = BASE_DIR / "signals"
DEFAULT_REPORT_DIR = BASE_DIR / "report"
DEFAULT_LOG_DIR    = BASE_DIR / "logs"


def now(): return dt.datetime.now()
def ts():  return now().strftime("%Y-%m-%d %H:%M:%S")
def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def sh(cmd: str, timeout: int = 90, log=None) -> int:
    # Выполняем через /bin/sh -lc "<cmd>"
    if log:
        log(f"[RUN] {cmd}")
    try:
        res = subprocess.run(
            ["/bin/sh", "-lc", cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if log:
            if res.stdout:
                log("[OUT] " + res.stdout.strip())
            if res.stderr:
                log("[ERR] " + res.stderr.strip())
        return res.returncode
    except subprocess.TimeoutExpired:
        log and log("[ERROR] timeout")
        return 124
    except Exception as e:
        log and log(f"[ERROR] {e}")
        return 1


#def append_controller_log(report_dir: Path, line: str):
#   ensure_dir(report_dir)
#  path = report_dir / "controller_summary.csv"
# new = not path.exists()
#with path.open("a", encoding="utf-8", newline="") as f:
#   w = csv.writer(f)
#  if new:
#     w.writerow(["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"])
# w.writerow([ts(), HOSTNAME, "-", "dispatcher", "info", "-", "OK", line, "-", "-", "-"])

def send_telegram(token: str | None, chat_id: str | None, text: str) -> None:
    if not token or not chat_id: return
    try:
        import urllib.parse, urllib.request
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data), timeout=10)
    except Exception:
        pass

#def tg_send(token: str | None, chat_id: str | None, text: str) -> None:
#   if not token or not chat_id:
#      return
#    try:
#       url = f"https://api.telegram.org/bot{token}/sendMessage"
#      with httpx.Client(timeout=10.0) as c:
#         c.post(url, json={"chat_id": chat_id, "text": text})
#except Exception:
#   pass


@dataclass
class Limits:
    max_concurrent: int
    stagger_sec: int
    per_node_cooldown: int
    burst_window: int
    burst_limit: int
    per_group_max: int
    worker_wait_sec: int


class Dispatcher:
    def __init__(self, flag_dir: Path, report_dir: Path, log_dir: Path,
                 max_concurrent: int, stagger_sec: float,
                 per_node_cooldown: int, burst_window: int, burst_limit: int,
                 per_group_max: int, groups_file: Optional[Path], worker_wait_sec: int):
        self.flag_dir, self.report_dir, self.log_dir = flag_dir, report_dir, log_dir
        for d in (flag_dir, report_dir, log_dir): ensure_dir(d)
        self.max_concurrent = int(max_concurrent)
        self.stagger_sec = float(stagger_sec)
        self.per_node_cooldown = int(per_node_cooldown)
        self.burst_window, self.burst_limit = int(burst_window), int(burst_limit)
        self.per_group_max = int(per_group_max)
        self.worker_wait_sec = int(worker_wait_sec)

    # ---- helpers ----
    def log_path(self) -> Path:
        return self.log_dir / "dispatcher.log"

    def log(self, line: str) -> None:
        try:
            with self.log_path().open("a", encoding="utf-8") as f:
                f.write(f"[{ts()}] {line}\n")
        except Exception:
            pass
        try:
            sys.stdout.write(line + "\n")
        except Exception:
            pass

    def write_csv(self, row: List[Any]) -> None:
        csv_path = self.report_dir / "controller_summary.csv"
        headers = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]
        new = not csv_path.exists()
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(headers)
            w.writerow(row)

    def list_queue(self) -> List[Path]:
        try:
            items = [self.qdir / x for x in os.listdir(self.qdir) if x.endswith(".json")]
            items.sort()  # по ts в имени
            return items
        except Exception:
            return []

    def read_json(self, path: Path) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def move(self, src: Path, dst_dir: Path) -> Optional[Path]:
        try:
            dst = dst_dir / src.name
            src.replace(dst)
            return dst
        except Exception as e:
            self.log(f"move error: {src} -> {dst_dir}: {e}")
            return None

    def cleanup_flags(self, node: str) -> None:
        for name in (f"restart_{node}.txt", f"done_{node}.txt"):
            p = self.flag_dir / name
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass

    def can_start(self, node: str) -> Tuple[bool, str]:
        # global limit
        if len(self.active) >= self.limits.max_concurrent:
            return (False, "global_limit")
        # per-node cooldown
        last = self.node_last.get(node, 0.0)
        if (time.time() - last) < self.limits.per_node_cooldown:
            return (False, "node_cooldown")
        # burst window
        t0 = time.time() - self.limits.burst_window
        recent = [1 for (t, _, _) in self.history if t >= t0]
        if len(recent) >= self.limits.burst_limit:
            return (False, "burst_limit")
        # per-group
        grp = self.groups.get(node)
        if grp:
            if self.group_active.get(grp, 0) >= self.limits.per_group_max:
                return (False, "group_limit")
        return (True, "ok")

    def mark_active(self, node: str) -> None:
        self.active[node] = time.time()
        grp = self.groups.get(node)
        if grp:
            self.group_active[grp] = self.group_active.get(grp, 0) + 1

    def unmark_active(self, node: str) -> None:
        if node in self.active:
            del self.active[node]
        grp = self.groups.get(node)
        if grp and self.group_active.get(grp, 0) > 0:
            self.group_active[grp] -= 1

    # ---- core ----
    def run_one(self, rq_path: Path) -> None:
        rq = self.read_json(rq_path)
        if not rq:
            self.move(rq_path, self.fdir)
            return

        node = rq.get("node") or "na"
        comment_cmd   = rq.get("comment_cmd", "true")
        uncomment_cmd = rq.get("uncomment_cmd", "true")
        tg_token = rq.get("tg_token", "")
        tg_chat  = rq.get("tg_chat", "")
        reason   = rq.get("reason", "pattern")

        def _log(m: str) -> None:
            self.log(f"{node}: {m}")

        # можно ли стартовать
        ok, why = self.can_start(node)
        if not ok:
            _log(f"defer ({why})")
            # оставим в очереди как есть
            time.sleep(1)
            return

        # перемещаем в inprogress
        moved = self.move(rq_path, self.ipdir)
        if not moved:
            return

        self.mark_active(node)
        try:
            # выполняем comment_cmd
            rc1 = sh(comment_cmd, timeout=self.limits.worker_wait_sec, log=_log)
            self.write_csv([ts(), self.hostname, node, "comment", "info", "comment_node",
                            "OK" if rc1 == 0 else "FAIL", reason, "", "", ""])

            # ждём немного между задачами (stagger)
            time.sleep(max(0, self.limits.stagger_sec))

            # Проверка наличия done-флага
            done = (self.flag_dir / f"done_{node}.txt").exists()

            # если done нет — опционально откатить
            if not done:
                sh(uncomment_cmd, timeout=self.limits.worker_wait_sec, log=_log)
                self.write_csv([ts(), self.hostname, node, "uncomment", "warn", "rollback",
                                "OK", "no done flag", "", "", ""])
                self.move(moved, self.fdir)
                tg_send(tg_token, tg_chat, f"[{ts()}] {node}: rollback (no done flag)")
                return

            # всё ок
            self.cleanup_flags(node)
            self.move(moved, self.ddir)
            self.node_last[node] = time.time()
            self.history.append((time.time(), node, True))
            tg_send(tg_token, tg_chat, f"[{ts()}] {node}: done")
        except Exception as e:
            self.history.append((time.time(), node, False))
            self.move(moved, self.fdir)
            _log(f"error: {e}")
        finally:
            self.unmark_active(node)

    def loop(self) -> None:
        self.log("dispatcher started")
        while True:
            items = self.list_queue()
            if not items:
                time.sleep(0.5)
                continue
            for p in items:
                self.run_one(p)


def build_cli() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Queue dispatcher (Py3.11)")
    ap.add_argument("--flag-dir", default=str(DEFAULT_FLAG_DIR))
    ap.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    ap.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    ap.add_argument("--max-concurrent", type=int, default=2)
    ap.add_argument("--stagger-sec", type=int, default=3)
    ap.add_argument("--per-node-cooldown", type=int, default=300)
    ap.add_argument("--burst-window", type=int, default=60)
    ap.add_argument("--burst-limit", type=int, default=5)
    ap.add_argument("--per-group-max", type=int, default=1)
    ap.add_argument("--worker-wait-sec", type=int, default=60)
    ap.add_argument("--groups-file")
    return ap


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--flag-dir",   default=str(SIGNALS_DIR))
    ap.add_argument("--report-dir", default=str(REPORT_DIR))
    ap.add_argument("--log-dir",    default=str(LOGS_DIR))
    ap.add_argument("--max-concurrent", type=int, default=2)
    ap.add_argument("--stagger-sec", type=float, default=10.0)
    ap.add_argument("--per-node-cooldown", type=int, default=300)
    ap.add_argument("--burst-window", type=int, default=60)
    ap.add_argument("--burst-limit", type=int, default=4)
    ap.add_argument("--per-group-max", type=int, default=1)
    ap.add_argument("--groups-file")
    ap.add_argument("--worker-wait-sec", type=int, default=15)
    args = ap.parse_args(argv)

    disp = Dispatcher(Path(args.flag_dir), Path(args.report_dir), Path(args.log_dir),
                      args.max_concurrent, args.stagger_sec, args.per_node_cooldown,
                      args.burst_window, args.burst_limit, args.per_group_max,
                      Path(args.groups_file) if args.groups_file else None, args.worker_wait_sec)
    append_controller_log(Path(args.report_dir), "dispatcher started")
    # … запуск основного цикла (оставлен как в оригинале) …
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
