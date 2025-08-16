#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pattern_controller.py — контроллер паттернов по логам (Py3.11).

Функции:
- tail логов и матчинг по правилам (regex)
- защита от "дребезга" (debounce) и "перегрева" (cooldown)
- постановка задач в очередь (signals/queue/*.json)
- CSV-резюме в report/controller_summary.csv
- Telegram-уведомления (опционально)
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import json
import os
import queue
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import httpx

# === Defaults ===
BASE_DIR = Path(os.environ.get("PC_BASE", "/tmp/pattern_controller"))
DEFAULT_FLAG_DIR   = BASE_DIR / "signals"
DEFAULT_REPORT_DIR = BASE_DIR / "report"
DEFAULT_LOG_DIR    = BASE_DIR / "logs"

DEFAULT_RULES = [
    {"pattern": r"\b(ERROR|Exception|CRITICAL|FATAL)\b", "severity": "critical", "action": "restart"},
    {"pattern": r"\bWARN(ING)?\b",                        "severity": "warn",     "action": "notify"},
    {"pattern": r"connection reset by peer",              "severity": "critical", "action": "restart"},
    {"pattern": r"connection reset",                      "severity": "critical", "action": "restart"},
    {"pattern": r"\bRST_STREAM\b",                        "severity": "critical", "action": "restart"},
    {"pattern": r"\bECONNRESET\b",                        "severity": "critical", "action": "restart"},
]

HEADERS = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]


def now() -> dt.datetime:
    return dt.datetime.now()


def ts() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def append_node_log(log_dir: Path, node: str, line: str) -> None:
    ensure_dir(log_dir)
    path = log_dir / f"node_{node}.log"
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def append_controller_error(log_dir: Path, line: str) -> None:
    ensure_dir(log_dir)
    path = log_dir / "pattern_errors.log"
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def run_cmd(cmd_list: List[str], timeout: int = 60, log_cb=None) -> int:
    line = " ".join(shlex.quote(s) for s in cmd_list)
    if log_cb:
        log_cb(f"[{ts()}] RUN: {line}")
    try:
        res = subprocess.run(cmd_list, capture_output=True, text=True, timeout=timeout)
        if res.stdout:
            log_cb and log_cb(f"[{ts()}] STDOUT: {res.stdout.strip()}")
        if res.stderr:
            log_cb and log_cb(f"[{ts()}] STDERR: {res.stderr.strip()}")
        return res.returncode
    except subprocess.TimeoutExpired:
        log_cb and log_cb(f"[{ts()}] ERROR: timeout")
        return 124
    except Exception as e:
        log_cb and (log_cb(f"[{ts()}] ERROR: {e}"))
        return 1


def write_csv_row(csv_path: Path, headers: List[str], row: List[Any]) -> None:
    ensure_dir(csv_path.parent)
    new = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(headers)
        w.writerow(row)


def tg_send(token: str | None, chat_id: str | None, text: str) -> None:
    if not token or not chat_id or not text:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        with httpx.Client(timeout=10.0) as c:
            c.post(url, json={"chat_id": chat_id, "text": text})
    except Exception as e:
        append_controller_error(DEFAULT_LOG_DIR, f"[{ts()}] TG ERROR: {e}")


def tg_send_long(token: str | None, chat_id: str | None, text: str, chunk: int = 3500) -> None:
    if not token or not chat_id or not text:
        return
    for i in range(0, len(text), chunk):
        tg_send(token, chat_id, text[i : i + chunk])


class LogFollower:
    def __init__(self, path: Path, seek_end: bool = True, poll_interval: float = 0.5):
        self.path = path
        self.poll = float(poll_interval)
        self.seek_end = bool(seek_end)
        self._fh: Optional[Any] = None
        self._ino: Optional[int] = None
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._fh:
                self._fh.close()
        except Exception:
            pass

    def _open(self) -> None:
        while not self._stop.is_set():
            try:
                fh = self.path.open("rb")
                st = os.fstat(fh.fileno())
                self._fh, self._ino = fh, st.st_ino
                if self.seek_end:
                    fh.seek(0, os.SEEK_END)
                return
            except OSError:
                time.sleep(self.poll)

    def lines(self) -> Iterator[str]:
        self._open()
        assert self._fh is not None
        while not self._stop.is_set():
            line = self._fh.readline()
            if line:
                try:
                    yield line.decode("utf-8", "ignore")
                except Exception:
                    yield ""
                continue
            try:
                cur_ino = os.stat(self.path).st_ino
                if cur_ino != self._ino:
                    try:
                        self._fh.close()
                    except Exception:
                        pass
                    self._open()
            except OSError:
                time.sleep(self.poll)
                continue
            time.sleep(self.poll)


def enqueue_request(
    flag_dir: Path,
    node: str,
    host: str,
    comment_cmd: str,
    uncomment_cmd: str,
    reason_pattern: str,
    report_dir: Path,
    tg_token: Optional[str],
    tg_chat: Optional[str],
) -> Optional[Path]:
    qdir = flag_dir / "queue"
    ensure_dir(qdir)
    rid = f"{int(time.time()*1000):08d}"[-8:]
    tsid = now().strftime("%Y%m%d_%H%M%S")
    rq = {
        "id": rid,
        "ts": tsid,
        "node": node,
        "host": host,
        "comment_cmd": comment_cmd,
        "uncomment_cmd": uncomment_cmd,
        "reason": reason_pattern,
        "report_dir": str(report_dir),
        "tg_token": tg_token or "",
        "tg_chat": tg_chat or "",
    }
    path = qdir / f"rq_{tsid}_{node}_{rid}.json"
    try:
        path.write_text(json.dumps(rq, ensure_ascii=False), encoding="utf-8")
        return path
    except Exception as e:
        append_controller_error(DEFAULT_LOG_DIR, f"[{ts()}] QUEUE WRITE ERROR: {e}")
        return None


@dataclasses.dataclass
class ControllerCfg:
    logs: List[Path]
    node: str
    flag_dir: Path
    report_dir: Path
    log_dir: Path
    comment_cmd: str
    uncomment_cmd: str
    ack_timeout_sec: int
    cmd_timeout_sec: int
    cooldown_sec: int
    debounce_sec: int
    tg_token: Optional[str]
    tg_chat: Optional[str]
    queue_mode: bool
    uncomment_on_fail: bool


class PatternController:
    def __init__(self, cfg: ControllerCfg, rules: List[Dict[str, Any]]) -> None:
        self.cfg = cfg
        self.host = socket.gethostname()
        self.rules = [
            {
                "rx": re.compile(r["pattern"], re.I | re.U),
                "severity": r.get("severity", "info"),
                "action": r.get("action", "notify"),
            }
            for r in rules
        ]
        self._followers: List[LogFollower] = []
        self._threads: List[threading.Thread] = []
        self._stopping = threading.Event()
        self._last_action_ts = 0.0
        self._last_match_ts = 0.0

        ensure_dir(self.cfg.flag_dir)
        ensure_dir(self.cfg.report_dir)
        ensure_dir(self.cfg.log_dir)
        self.csv_path = self.cfg.report_dir / "controller_summary.csv"

    def _cooldowns_ok(self) -> Tuple[bool, str]:
        t = time.time()
        if (t - self._last_match_ts) < self.cfg.debounce_sec:
            return False, "debounced"
        if (t - self._last_action_ts) < self.cfg.cooldown_sec:
            return False, "cooldown"
        return True, "ok"

    def _cleanup_flags(self) -> None:
        for name in (f"restart_{self.cfg.node}.txt", f"done_{self.cfg.node}.txt"):
            p = self.cfg.flag_dir / name
            try:
                if p.exists():
                    p.unlink()
            except Exception as e:
                append_controller_error(self.cfg.log_dir, f"[{ts()}] CLEANUP WARN node={self.cfg.node} path={p} err={e}")

    def _signal_restart_and_wait(
        self, matched_log: str, matched_line: str, matched_pattern: str, log_cb
    ) -> None:
        op_id = now().strftime("%Y%m%d_%H%M%S") + "_" + self.cfg.node
        op_log_path = self.cfg.report_dir / f"op_{op_id}.log"
        restart_flag = self.cfg.flag_dir / f"restart_{self.cfg.node}.txt"
        done_flag = self.cfg.flag_dir / f"done_{self.cfg.node}.txt"

        # Комментирование
        rc = run_cmd(["/bin/sh", "-lc", self.cfg.comment_cmd], timeout=self.cfg.cmd_timeout_sec, log_cb=log_cb)
        write_csv_row(
            self.csv_path,
            HEADERS,
            [
                ts(),
                self.host,
                self.cfg.node,
                "comment",
                "info",
                "comment_node",
                ("OK" if rc == 0 else "FAIL"),
                "",
                str(op_log_path),
                matched_log,
                matched_line.strip()[:400],
            ],
        )
        tg_send_long(
            self.cfg.tg_token,
            self.cfg.tg_chat,
            f"[{ts()}] {self.host}: COMMENT '{self.cfg.node}' → {('OK' if rc==0 else 'FAIL')}",
        )

        # Флаг рестарта
        try:
            restart_flag.write_text(f"ts={ts()}\npattern={matched_pattern}\n", encoding="utf-8")
        except Exception:
            pass

        # Ожидание done-флага
        deadline = time.time() + max(5, self.cfg.ack_timeout_sec)
        while time.time() < deadline:
            if done_flag.exists():
                break
            time.sleep(0.5)

        # Разкоммент, если не дошло до done и выставлен uncomment_on_fail
        if not done_flag.exists() and self.cfg.uncomment_on_fail:
            run_cmd(["/bin/sh", "-lc", self.cfg.uncomment_cmd], timeout=self.cfg.cmd_timeout_sec, log_cb=log_cb)

        # Уборка флагов (идемпотентно)
        self._cleanup_flags()

    def _handle_match(self, matched_log: str, matched_line: str, matched_pattern: str, severity: str, action: str) -> None:
        ok, reason = self._cooldowns_ok()
        if not ok:
            append_node_log(self.cfg.log_dir, self.cfg.node, f"[{ts()}] SKIP ({reason}) {matched_pattern}")
            return
        self._last_match_ts = time.time()

        def log_cb(s: str) -> None:
            append_node_log(self.cfg.log_dir, self.cfg.node, s)

        if self.cfg.queue_mode:
            # Кладём заявку в очередь
            path = enqueue_request(
                self.cfg.flag_dir,
                self.cfg.node,
                self.host,
                self.cfg.comment_cmd,
                self.cfg.uncomment_cmd,
                matched_pattern,
                self.cfg.report_dir,
                self.cfg.tg_token,
                self.cfg.tg_chat,
            )
            write_csv_row(
                self.csv_path,
                HEADERS,
                [ts(), self.host, self.cfg.node, "enqueue", severity, action, ("OK" if path else "FAIL"),
                 "", "", matched_log, matched_line.strip()[:400]],
            )
            if path:
                tg_send(self.cfg.tg_token, self.cfg.tg_chat, f"[{ts()}] queued {self.cfg.node}: {matched_pattern}")
            self._last_action_ts = time.time()
            return

        # Немедленный сценарий: коммент → флаг → ждём done → (опционально) раскоммент
        self._signal_restart_and_wait(matched_log, matched_line, matched_pattern, log_cb=log_cb)
        self._last_action_ts = time.time()

    def start(self) -> None:
        # Тейлим все указанные логи
        for p in self.cfg.logs:
            f = LogFollower(p, seek_end=True, poll_interval=0.5)
            self._followers.append(f)

            def worker(fp: Path = p, fol: LogFollower = f):
                for line in fol.lines():
                    for r in self.rules:
                        rx: re.Pattern[str] = r["rx"]
                        if rx.search(line or ""):
                            self._handle_match(
                                matched_log=str(fp),
                                matched_line=line,
                                matched_pattern=rx.pattern,
                                severity=r["severity"],
                                action=r["action"],
                            )
                            break

            th = threading.Thread(target=worker, name=f"tail-{p.name}", daemon=True)
            self._threads.append(th)
            th.start()

    def stop(self) -> None:
        self._stopping.set()
        for f in self._followers:
            f.stop()
        for t in self._threads:
            t.join(timeout=1.0)


def build_cli() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Pattern controller (Py3.11)")
    ap.add_argument("--logs", nargs="+", required=True, help="пути до логов для tail")
    ap.add_argument("--node", required=True)
    ap.add_argument("--flag-dir", default=str(DEFAULT_FLAG_DIR))
    ap.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    ap.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    ap.add_argument("--comment-cmd", required=True)
    ap.add_argument("--uncomment-cmd", required=True)
    ap.add_argument("--ack-timeout-sec", type=int, default=120)
    ap.add_argument("--cmd-timeout-sec", type=int, default=90)
    ap.add_argument("--cooldown-sec", type=int, default=180)
    ap.add_argument("--debounce-sec", type=int, default=15)
    ap.add_argument("--rules-file", help="JSON с правилами [{'pattern','severity','action'}]")
    ap.add_argument("--tg-token")
    ap.add_argument("--tg-chat")
    ap.add_argument("--queue-mode", action="store_true", help="вместо немедленного рестарта класть заявку в очередь")
    ap.add_argument("--uncomment-on-fail", action="store_true", help="если нет done_* — попытаться раскомментировать")
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    args = build_cli().parse_args(argv)
    logs = [Path(x) for x in args.logs]
    cfg = ControllerCfg(
        logs=logs,
        node=args.node,
        flag_dir=Path(args.flag_dir),
        report_dir=Path(args.report_dir),
        log_dir=Path(args.log_dir),
        comment_cmd=args.comment_cmd,
        uncomment_cmd=args.uncomment_cmd,
        ack_timeout_sec=args.ack_timeout_sec,
        cmd_timeout_sec=args.cmd_timeout_sec,
        cooldown_sec=args.cooldown_sec,
        debounce_sec=args.debounce_sec,
        tg_token=args.tg_token,
        tg_chat=args.tg_chat,
        queue_mode=bool(args.queue_mode),
        uncomment_on_fail=bool(args.uncomment_on_fail),
    )
    # Загрузить правила
    rules = DEFAULT_RULES
    if args.rules_file:
        try:
            rules = json.loads(Path(args.rules_file).read_text(encoding="utf-8"))
        except Exception as e:
            append_controller_error(cfg.log_dir, f"[{ts()}] rules load error: {e}")

    ctrl = PatternController(cfg, rules)
    stop_evt = threading.Event()

    def _sig(*_):
        stop_evt.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    ctrl.start()
    while not stop_evt.is_set():
        time.sleep(0.5)
    ctrl.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
