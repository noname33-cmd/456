#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Безопасный drain/disable/enable для HAProxy c предохранителем ">= min_enabled".
- Читает правила из RULES_FILE (env) или /tmp/pattern_controller/report/rules.json
- Работает через HAProxy Runtime API (UNIX сокет)
- Если после drain/disable останется < min_enabled — кладёт задачу в очередь deferred.csv
- CLI:
    --action {drain,disable,enable,retry}
    --backend <name> --server <name>   (для drain/disable/enable)
ENV:
    HAPROXY_SOCKET=/run/haproxy/admin.sock (или ваш путь)
    RULES_FILE=/tmp/pattern_controller/report/rules.json
"""

import csv, json, os, sys, fcntl, time, subprocess, tempfile, contextlib
from pathlib import Path
from typing import List, Dict, Tuple

BASE_DIR   = Path("/tmp/pattern_controller")
SIGNALS    = BASE_DIR / "signals"
REPORT     = BASE_DIR / "report"
LOGS       = BASE_DIR / "logs"

LOCKS_DIR  = SIGNALS / "locks"
QUEUE_DIR  = SIGNALS / "queue"
QUEUE_FILE = QUEUE_DIR / "deferred.csv"

# ENV с возможностью переопределить файл правил и сокет
HAPROXY_SOCKET = os.environ.get("HAPROXY_SOCKET", "/run/haproxy/admin.sock")
RULES_FILE = Path(os.environ.get("RULES_FILE", str(REPORT / "rules.json")))

def ensure_dirs():
    for p in (LOCKS_DIR, QUEUE_DIR, LOGS, REPORT, SIGNALS):
        p.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_dirs()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with (LOGS / "safe_toggle.log").open("a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")
    # дублируем в stdout для journalctl
    print(msg, flush=True)

def load_rules() -> Dict:
    """rules.json:
    {
      "global": { "min_enabled": 4 },
      "backends": { "Jboss_client": { "min_enabled": 4 } }
    }
    """
    try:
        with RULES_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log(f"[WARN] RULES_FILE not found: {RULES_FILE}, using defaults")
        return {"global": {"min_enabled": 4}, "backends": {}}
    except Exception as e:
        log(f"[ERROR] load_rules: {e}; using defaults")
        return {"global": {"min_enabled": 4}, "backends": {}}

def get_min_enabled(backend: str, rules: Dict) -> int:
    be = rules.get("backends", {}).get(backend, {})
    try:
        return int(be.get("min_enabled", rules.get("global", {}).get("min_enabled", 4)))
    except Exception:
        return 4

def run_haproxy_cmd(cmd: str) -> str:
    # echo "show stat" | socat - UNIX-CONNECT:/run/haproxy/admin.sock
    full = f'echo "{cmd}" | socat - UNIX-CONNECT:{HAPROXY_SOCKET}'
    res = subprocess.run(full, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode != 0:
        raise RuntimeError(f"HAProxy runtime error: {res.stderr.decode('utf-8','ignore').strip()}")
    return res.stdout.decode("utf-8","ignore")

def get_stats() -> List[Dict[str, str]]:
    out = run_haproxy_cmd("show stat -1 2 -1")
    lines = [ln for ln in out.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        return []
    reader = csv.reader(lines)
    rows = list(reader)
    headers = rows[0]
    data = []
    for r in rows[1:]:
        row = {headers[i]: (r[i] if i < len(headers) and i < len(r) else "") for i in range(len(headers))}
        data.append(row)
    return data

def list_backend_servers(backend: str) -> List[Dict[str, str]]:
    stats = get_stats()
    # pxname=backend, svname=server; исключаем агрегат BACKEND
    return [r for r in stats if r.get("pxname") == backend and r.get("svname") not in ("BACKEND", "")]

def count_enabled(backend: str) -> Tuple[int, int]:
    """enabled: status in {UP, OPEN} и admin (admin_state) не содержит MAINT."""
    servers = list_backend_servers(backend)
    total = len(servers)
    enabled = 0
    for s in servers:
        status = (s.get("status") or "").upper()
        admin  = (s.get("admin")  or "").upper()
        if status in ("UP", "OPEN") and "MAINT" not in admin:
            enabled += 1
    return enabled, total

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
    run_haproxy_cmd(f"enable server {backend}/{server}")
    log(f"[ENABLE] {backend}/{server}")

def drain_server(backend: str, server: str):
    run_haproxy_cmd(f"set server {backend}/{server} state drain")
    log(f"[DRAIN]  {backend}/{server}")

def disable_server(backend: str, server: str):
    run_haproxy_cmd(f"disable server {backend}/{server}")
    log(f"[DISABLE]{backend}/{server}")

def enqueue_deferred(action: str, backend: str, server: str, reason: str):
    ensure_dirs()
    new = not QUEUE_FILE.exists()
    with QUEUE_FILE.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=';')
        if new:
            w.writerow(["ts","action","backend","server","reason"])
        w.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), action, backend, server, reason])
    log(f"[DEFER]  {action} {backend}/{server} — {reason}")

def safe_toggle(action: str, backend: str, server: str):
    """
    action ∈ {'drain','disable','enable'}
    Гарантия: перед drain/disable оставим >= min_enabled.
    """
    rules = load_rules()
    min_enabled = get_min_enabled(backend, rules)

    if action == "enable":
        with with_lock(f"{backend}"):
            enable_server(backend, server)
        return

    with with_lock(f"{backend}"):
        enabled, _ = count_enabled(backend)
        if enabled <= min_enabled:
            enqueue_deferred(action, backend, server,
                             reason=f"enabled={enabled}, min={min_enabled} — недостаточно активных нод")
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
        action  = r["action"]
        backend = r["backend"]
        server  = r["server"]

        try:
            with with_lock(f"{backend}"):
                min_enabled = get_min_enabled(backend, rules)
                enabled, _   = count_enabled(backend)

                if action in ("drain","disable"):
                    if enabled <= min_enabled:
                        r["reason"] = f"enabled={enabled}<=min={min_enabled}"
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
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
            w = csv.DictWriter(tf, fieldnames=["ts","action","backend","server","reason"], delimiter=';')
            w.writeheader()
            for r in remaining:
                if "reason" not in r:
                    r["reason"] = "still not enough enabled"
                w.writerow(r)
        os.replace(tf.name, QUEUE_FILE)
        log(f"[RETRY] remaining deferred: {len(remaining)}")
    else:
        try:
            QUEUE_FILE.unlink()
        except FileNotFoundError:
            pass
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
