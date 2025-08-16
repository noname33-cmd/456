#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Безопасный drain/disable/enable для HAProxy с предохранителем ">= min_enabled".

- Читает правила из RULES_FILE (env) или $PC_BASE/report/rules.json
- Общается с HAProxy Runtime через UNIX-сокет (без shell/socat)
- Если после drain/disable останется < min_enabled — кладёт задачу в очередь deferred.csv
- CLI:
    --action {drain,disable,enable,retry}
    --backend <name> --server <name>   (для drain/disable/enable)
ENV:
    PC_BASE=/tmp/pattern_controller
    HAPROXY_SOCKET=/var/lib/haproxy/haproxy.sock
    RULES_FILE=$PC_BASE/report/rules.json
"""

from __future__ import annotations
import csv, json, os, sys, fcntl, time, tempfile, contextlib, re, socket
from pathlib import Path
from typing import List, Dict, Tuple

# --- базовые пути
PC_BASE = Path(os.environ.get("PC_BASE", "/tmp/pattern_controller"))
BASE_DIR = PC_BASE
SIGNALS  = BASE_DIR / "signals"
REPORT   = BASE_DIR / "report"
LOGS     = BASE_DIR / "logs"

LOCKS_DIR  = SIGNALS / "locks"
QUEUE_DIR  = SIGNALS / "queue"
QUEUE_FILE = QUEUE_DIR / "deferred.csv"

# ENV с возможностью переопределить файл правил и сокет
HAPROXY_SOCKET = os.environ.get("HAPROXY_SOCKET", "/var/lib/haproxy/haproxy.sock")
RULES_FILE = Path(os.environ.get("RULES_FILE", str(REPORT / "rules.json")))

# валидация имён
SAFE_NAME = re.compile(r"^[A-Za-z0-9._:-]+$")

def ensure_dirs():
    for p in (LOCKS_DIR, QUEUE_DIR, LOGS, REPORT, SIGNALS):
        p.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_dirs()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with (LOGS / "safe_toggle.log").open("a", encoding="utf-8") as f:
            f.write(f"{ts} {msg}\n")
    except Exception:
        pass
    print(msg, flush=True)

def load_rules() -> Dict:
    """
    rules.json:
    {
      "global":  { "min_enabled": 4 },
      "backends": { "Jboss_client": { "min_enabled": 4 } }
    }
    """
    try:
        with RULES_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log(f"[WARN] RULES_FILE not found: {RULES_FILE}, using defaults")
    except Exception as e:
        log(f"[ERROR] load_rules: {e}; using defaults")
    return {"global": {"min_enabled": 4}, "backends": {}}

def get_min_enabled(backend: str, rules: Dict) -> int:
    be = rules.get("backends", {}).get(backend, {})
    try:
        return int(be.get("min_enabled", rules.get("global", {}).get("min_enabled", 4)))
    except Exception:
        return 4

# --- работа с HAProxy runtime сокетом (без socat)
def _send_runtime(cmd: str, timeout: float = 3.0) -> str:
    """
    Отправляет строку в UNIX-сокет HAProxy, возвращает stdout.
    """
    data = (cmd.strip() + "\n").encode("utf-8")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(HAPROXY_SOCKET)
        s.sendall(data)
        chunks = []
        while True:
            try:
                part = s.recv(65536)
            except socket.timeout:
                break
            if not part:
                break
            chunks.append(part)
        return b"".join(chunks).decode("utf-8", "ignore")
    finally:
        try: s.close()
        except Exception: pass

def get_stats() -> List[Dict[str, str]]:
    out = _send_runtime("show stat -1 2 -1")
    lines = [ln for ln in out.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        return []
    reader = csv.reader(lines)
    rows = list(reader)
    headers = rows[0]
    data = []
    for r in rows[1:]:
        row = {headers[i]: (r[i] if i < len(r) and i < len(headers) else "") for i in range(len(headers))}
        data.append(row)
    return data

def list_backend_servers(backend: str) -> List[Dict[str, str]]:
    stats = get_stats()
    return [r for r in stats if r.get("pxname") == backend and r.get("svname") not in ("BACKEND", "")]

def server_is_enabled(row: Dict[str, str]) -> bool:
    status = (row.get("status") or "").upper()
    admin  = (row.get("admin")  or "").upper()
    return (status in ("UP", "OPEN")) and ("MAINT" not in admin)

def count_enabled(backend: str) -> Tuple[int, int]:
    servers = list_backend_servers(backend)
    total = len(servers)
    enabled = sum(1 for s in servers if server_is_enabled(s))
    return enabled, total

def get_server_row(backend: str, server: str) -> Dict[str, str] | None:
    for s in list_backend_servers(backend):
        if s.get("svname") == server:
            return s
    return None

@contextlib.contextmanager
def with_lock(name: str):
    ensure_dirs()
    lock_path = LOCKS_DIR / f"{name}.lock"
    f = open(lock_path, "w")
    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()

def enable_server(backend: str, server: str):
    _send_runtime(f"enable server {backend}/{server}")
    log(f"[ENABLE] {backend}/{server}")

def drain_server(backend: str, server: str):
    _send_runtime(f"set server {backend}/{server} state drain")
    log(f"[DRAIN]  {backend}/{server}")

def disable_server(backend: str, server: str):
    _send_runtime(f"disable server {backend}/{server}")
    log(f"[DISABLE] {backend}/{server}")

def enqueue_deferred(action: str, backend: str, server: str, reason: str):
    ensure_dirs()
    new = not QUEUE_FILE.exists()
    with QUEUE_FILE.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=';')
        if new:
            w.writerow(["ts","action","backend","server","reason"])
        w.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), action, backend, server, reason])
    log(f"[DEFER]  {action} {backend}/{server} — {reason}")

def _validate_names(backend: str, server: str):
    if not (SAFE_NAME.match(backend or "") and SAFE_NAME.match(server or "")):
        raise ValueError("bad backend/server format")

def safe_toggle(action: str, backend: str, server: str):
    """
    action ∈ {'drain','disable','enable'}
    Гарантия: перед drain/disable оставим >= min_enabled (если снимаемый сервер действительно активный).
    """
    _validate_names(backend, server)
    rules = load_rules()
    min_enabled = get_min_enabled(backend, rules)

    if action == "enable":
        with with_lock(backend):
            enable_server(backend, server)
        return

    with with_lock(backend):
        row = get_server_row(backend, server)
        # если сервер не найден — оставим в очереди на разбор (или логируем ошибку)
        if row is None:
            enqueue_deferred(action, backend, server, reason="server not found in stats")
            return

        # считаем активных сейчас
        enabled_now, _ = count_enabled(backend)

        # вычтем текущий сервер из активных только если он активен — именно это влияет на порог
        if server_is_enabled(row):
            would_left = enabled_now - 1
        else:
            would_left = enabled_now  # он уже не в трафике — порог не нарушается

        if would_left < min_enabled:
            enqueue_deferred(
                action, backend, server,
                reason=f"would_left={would_left} < min={min_enabled}"
            )
            return

        if action == "drain":
            drain_server(backend, server)
        elif action == "disable":
            disable_server(backend, server)
        else:
            raise ValueError("unknown action")

def retry_deferred_once():
    """Один проход по очереди: если активных нод стало > min_enabled — выполняем отложенные действия."""
    if not QUEUE_FILE.exists():
        log("[RETRY] deferred queue is empty")
        return

    rules = load_rules()
    with QUEUE_FILE.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter=';')
        rows = list(rdr)

    remaining = []
    for r in rows:
        action  = r.get("action","")
        backend = r.get("backend","")
        server  = r.get("server","")

        try:
            _validate_names(backend, server)
            with with_lock(backend):
                min_enabled = get_min_enabled(backend, rules)
                row = get_server_row(backend, server)
                enabled_now, _ = count_enabled(backend)
                if action in ("drain","disable"):
                    would_left = enabled_now - (1 if (row and server_is_enabled(row)) else 0)
                    if would_left < min_enabled:
                        r["reason"] = f"would_left={would_left} < min={min_enabled}"
                        remaining.append(r)
                        continue
                    if action == "drain":
                        drain_server(backend, server)
                    else:
                        disable_server(backend, server)
                else:
                    enable_server(backend, server)
        except Exception as e:
            r["reason"] = f"error: {e}"
            remaining.append(r)

    if remaining:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="") as tf:
            w = csv.DictWriter(tf, fieldnames=["ts","action","backend","server","reason"], delimiter=';')
            w.writeheader()
            for r in remaining:
                if "reason" not in r:
                    r["reason"] = "still not enough enabled"
                w.writerow(r)
        os.replace(tf.name, QUEUE_FILE)
        log(f"[RETRY] remaining deferred: {len(remaining)}")
    else:
        try: QUEUE_FILE.unlink()
        except FileNotFoundError: pass
        log("[RETRY] deferred queue cleared")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="HAProxy safe toggle with min_enabled guard")
    p.add_argument("--backend")
    p.add_argument("--server")
    p.add_argument("--action", required=True, choices=["drain","disable","enable","retry"])
    args = p.parse_args()

    ensure_dirs()
    if args.action == "retry":
        retry_deferred_once()
        sys.exit(0)

    if not (args.backend and args.server):
        print("backend/server required for drain/disable/enable", file=sys.stderr)
        sys.exit(2)

    try:
        safe_toggle(args.action, args.backend, args.server)
    except Exception as e:
        log(f"[ERROR] {e}")
        sys.exit(1)
