#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cleanup_housekeeping.py — ротация логов и очистка старых артефактов (Py3.11)

Примеры:
  python3 cleanup_housekeeping.py \
    --logs /tmp/pattern_controller/logs --keep-days 30 \
    --ops-done /tmp/pattern_controller/signals/haproxy_ops_done --ops-keep-days 7 \
    --report /tmp/pattern_controller/report --max-oplogs 2000
"""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Iterable, Tuple, List

class FileLock:
    def __init__(self, path: Path, timeout_sec: int = 0):
        self.path = Path(path); self.timeout_sec = max(0, int(timeout_sec)); self._fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+")
        try:
            import fcntl
            start = time.time()
            while True:
                try:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if self.timeout_sec and (time.time() - start) > self.timeout_sec:
                        raise TimeoutError(f"Lock timeout on {self.path}")
                    time.sleep(0.1)
        except ImportError:
            pass
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            import fcntl
            try: fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            except Exception: pass
        except ImportError:
            pass
        try:
            self._fh.flush(); os.fsync(self._fh.fileno())
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        return False

SECONDS_IN_DAY = 24 * 60 * 60

def older_than(path: Path, days: int) -> bool:
    try:
        st = path.stat()
    except FileNotFoundError:
        return False
    cutoff = time.time() - days * SECONDS_IN_DAY
    return st.st_mtime < cutoff

def safe_unlink(path: Path) -> bool:
    try:
        path.unlink(); return True
    except FileNotFoundError:
        return False
    except Exception:
        return False

def list_files(dirpath: Path, patterns: Iterable[str] = ("*",)) -> List[Path]:
    out: List[Path] = []
    for pat in patterns:
        out.extend([p for p in dirpath.glob(pat) if p.is_file()])
    return out

def cleanup_logs(logs_dir: Path, keep_days: int) -> Tuple[int, int]:
    if not logs_dir or not logs_dir.exists():
        return (0, 0)
    patterns = ["*.log", "*.log.*", "node_*.log*", "worker_*.log*", "dispatcher.log*"]
    files = list_files(logs_dir, patterns)
    before = len(files); removed = 0
    for p in files:
        if older_than(p, keep_days) and safe_unlink(p):
            removed += 1
    return (removed, before)

def cleanup_ops_done(signals_dir: Path, ops_keep_days: int) -> Tuple[int, int]:
    if not signals_dir or not signals_dir.exists():
        return (0, 0)
    files = [p for p in signals_dir.iterdir() if p.is_file()]
    before = len(files); removed = 0
    for p in files:
        if older_than(p, ops_keep_days) and safe_unlink(p):
            removed += 1
    return (removed, before)

def cleanup_report_jsonl(report_dir: Path, keep_days: int) -> Tuple[int, int]:
    if not report_dir or not report_dir.exists():
        return (0, 0)
    files = [p for p in report_dir.rglob("*.jsonl") if p.is_file()]
    before = len(files); removed = 0
    for p in files:
        if older_than(p, keep_days) and safe_unlink(p):
            removed += 1
    return (removed, before)

def cap_total_files(report_dir: Path, prefix: str, max_keep: int) -> Tuple[int, int]:
    if not report_dir or not report_dir.exists():
        return (0, 0)
    files = [p for p in report_dir.iterdir() if p.is_file() and p.name.startswith(prefix)]
    files_sorted = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    before = len(files_sorted)
    if before <= max_keep:
        return (0, before)
    to_delete = files_sorted[max_keep:]; removed = 0
    for p in to_delete:
        if safe_unlink(p):
            removed += 1
    return (removed, before)

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Housekeeping / cleanup (Py3.11)")
    ap.add_argument("--logs", type=Path, help="Каталог логов")
    ap.add_argument("--keep-days", type=int, default=30)
    ap.add_argument("--ops-done", type=Path, help="Каталог сигналов завершённых операций")
    ap.add_argument("--ops-keep-days", type=int, default=7)
    ap.add_argument("--report", type=Path, help="Каталог отчётов")
    ap.add_argument("--max-oplogs", type=int, default=2000)
    ap.add_argument("--lock", type=Path, default=Path("/tmp/pattern_controller/locks/cleanup_housekeeping.lock"))
    ap.add_argument("--lock-timeout", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    return ap

def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    with FileLock(args.lock, timeout_sec=args.lock_timeout):
        total_removed = 0
        if args.logs:
            removed, total = cleanup_logs(args.logs, args.keep_days) if not args.dry_run else (0, len(list_files(args.logs, ["*.log", "*.log.*", "node_*.log*", "worker_*.log*", "dispatcher.log*"])))
            print(f"[logs] dir={args.logs} total={total} removed={removed}")
            total_removed += removed
        if args.ops_done:
            removed, total = cleanup_ops_done(args.ops_done, args.ops_keep_days) if not args.dry_run else (0, len([p for p in (args.ops_done.iterdir() if args.ops_done.exists() else []) if p.is_file()]))
            print(f"[ops-done] dir={args.ops_done} total={total} removed={removed}")
            total_removed += removed
        if args.report:
            removed, total = cleanup_report_jsonl(args.report, args.keep_days) if not args.dry_run else (0, len(list(args.report.rglob("*.jsonl")) if args.report.exists() else []))
            print(f"[report-jsonl] dir={args.report} total={total} removed={removed}")
            total_removed += removed
            removed, total = cap_total_files(args.report, "op_", args.max_oplogs) if not args.dry_run else (0, len([p for p in (args.report.iterdir() if args.report.exists() else []) if p.is_file() and p.name.startswith('op_')]))
            print(f"[report-oplogs-cap] dir={args.report} total={total} removed={removed}")
            total_removed += removed
        print(f"cleanup done, removed={total_removed}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
