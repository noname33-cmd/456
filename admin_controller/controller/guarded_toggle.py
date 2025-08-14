#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Универсальный предохранитель для пулов сервисов: HAProxy, Nginx, systemd-группы, JBoss.
- Конфиг кластеров: /tmp/pattern_controller/report/clusters.json
- Очередь отложенных операций: /tmp/pattern_controller/signals/queue/deferred_all.csv
- Ежеминутный retry: guarded_toggle.py --action retry
CLI:
  --action {disable,drain,enable,retry}
  --cluster <name> --server <name>
"""

import os, sys, json, csv, time, fcntl, tempfile, subprocess, importlib, contextlib
from pathlib import Path
from typing import Dict, Any, List, Tuple

BASE = Path("/tmp/pattern_controller")
REPORT = BASE / "report"
SIGNALS = BASE / "signals"
LOGS = BASE / "logs"
LOCKS = SIGNALS / "locks"
QUEUE = SIGNALS / "queue"
QUEUE_FILE = QUEUE / "deferred_all.csv"

CLUSTERS_FILE = REPORT / "clusters.json"  # описание пулов/бэкендов
RULES_FILE    = REPORT / "rules.json"     # min_enabled глобально/по пулам (можно заменить через env RULES_FILE)

RULES_FILE = Path(os.environ.get("RULES_FILE", str(RULES_FILE)))

def ensure_dirs():
    for p in (REPORT, SIGNALS, LOGS, LOCKS, QUEUE):
        p.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_dirs()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    (LOGS / "guarded_toggle.log").open("a", encoding="utf-8").write(f"{ts} {msg}\n")
    print(msg, flush=True)

def load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default

def load_clusters() -> Dict[str, Any]:
    """
    clusters.json формат:
    {
      "Jboss_client": {
        "provider": "haproxy",
        "provider_conf": { "socket": "/run/haproxy/admin.sock" },
        "min_enabled": 4
      },
      "nginx_api": {
        "provider": "nginx",
        "provider_conf": {
          "upstream_conf": "/etc/nginx/upstreams/api_upstream.conf",
          "reload_cmd": "nginx -s reload"
        },
        "min_enabled": 3
      },
      "workers_group": {
        "provider": "systemd_group",
        "provider_conf": { "units": ["svc-a.service","svc-b.service","svc-c.service","svc-d.service","svc-e.service"] },
        "min_enabled": 4
      },
      "jboss_pool": {
        "provider": "jboss",
        "provider_conf": { "cli": "/opt/jboss/bin/jboss-cli.sh", "hosts": ["node1","node2","node3","node4","node5"] },
        "min_enabled": 4
      }
    }
    """
    return load_json(CLUSTERS_FILE, {})

def load_rules() -> Dict[str, Any]:
    # совместимость с вашим rules.json: берем global.min_enabled как дефолт
    return load_json(RULES_FILE, {"global": {"min_enabled": 4}, "backends": {}})

def get_min_enabled(cluster: str, clusters: Dict[str, Any], rules: Dict[str, Any]) -> int:
    if cluster in clusters and "min_enabled" in clusters[cluster]:
        return int(clusters[cluster]["min_enabled"])
    be = rules.get("backends", {}).get(cluster, {})
    return int(be.get("min_enabled", rules.get("global", {}).get("min_enabled", 4)))

@contextlib.contextmanager
def with_lock(name: str):
    ensure_dirs()
    f = open(LOCKS / (name.replace("/", "_") + ".lock"), "w")
    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()

def enqueue(action: str, cluster: str, server: str, reason: str):
    ensure_dirs()
    new = not QUEUE_FILE.exists()
    with QUEUE_FILE.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=';')
        if new:
            w.writerow(["ts","action","cluster","server","reason"])
        w.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), action, cluster, server, reason])
    log(f"[DEFER] {action} {cluster}/{server} — {reason}")

# ===== динамическая загрузка провайдеров =====
def get_provider(provider_name: str):
    """
    providers/haproxy.py: must define:
      count_enabled(conf) -> (enabled:int, total:int)
      set_state(conf, server:str, action:str)  # action in {"enable","disable","drain"}
    """
    mod = importlib.import_module(f"controller.providers.{provider_name}")
    return mod

# ===== основные операции =====
def safe_toggle(action: str, cluster: str, server: str):
    """
    action ∈ {'enable','disable','drain'}
    """
    clusters = load_clusters()
    if cluster not in clusters:
        raise RuntimeError(f"Unknown cluster: {cluster}")
    conf = clusters[cluster]
    provider = get_provider(conf["provider"])
    provider_conf = conf.get("provider_conf", {})
    rules = load_rules()
    min_enabled = get_min_enabled(cluster, clusters, rules)

    if action == "enable":
        with with_lock(cluster):
            provider.set_state(provider_conf, server, "enable")
            log(f"[ENABLE] {cluster}/{server}")
        return

    with with_lock(cluster):
        enabled, total = provider.count_enabled(provider_conf)
        if enabled <= min_enabled:
            enqueue(action, cluster, server, f"enabled={enabled}, min={min_enabled}")
            return
        provider.set_state(provider_conf, server, action)
        log(f"[{action.upper()}] {cluster}/{server}")

def retry_once():
    if not QUEUE_FILE.exists():
        log("[RETRY] queue empty")
        return

    clusters = load_clusters()
    rules = load_rules()
    with QUEUE_FILE.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=';'))

    remaining = []
    for r in rows:
        action  = r["action"]
        cluster = r["cluster"]
        server  = r["server"]
        try:
            if cluster not in clusters:
                r["reason"] = "unknown cluster"
                remaining.append(r); continue

            conf = clusters[cluster]
            provider = get_provider(conf["provider"])
            provider_conf = conf.get("provider_conf", {})
            min_enabled = get_min_enabled(cluster, clusters, rules)

            with with_lock(cluster):
                enabled, _ = provider.count_enabled(provider_conf)
                if action in ("disable","drain"):
                    if enabled <= min_enabled:
                        r["reason"] = f"enabled={enabled}<=min={min_enabled}"
                        remaining.append(r); continue
                    provider.set_state(provider_conf, server, action)
                    log(f"[RETRY {action}] {cluster}/{server}")
                else:
                    provider.set_state(provider_conf, server, "enable")
                    log(f"[RETRY enable] {cluster}/{server}")
        except Exception as e:
            r["reason"] = f"error: {e}"
            remaining.append(r)

    if remaining:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
            w = csv.DictWriter(tf, fieldnames=["ts","action","cluster","server","reason"], delimiter=';')
            w.writeheader()
            for r in remaining:
                if "reason" not in r:
                    r["reason"] = "unknown"
                w.writerow(r)
        os.replace(tf.name, QUEUE_FILE)
        log(f"[RETRY] remaining: {len(remaining)}")
    else:
        try: QUEUE_FILE.unlink()
        except FileNotFoundError: pass
        log("[RETRY] queue cleared")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Universal guarded toggle")
    p.add_argument("--action", required=True, choices=["enable","disable","drain","retry"])
    p.add_argument("--cluster")
    p.add_argument("--server")
    args = p.parse_args()

    ensure_dirs()
    if args.action == "retry":
        retry_once(); sys.exit(0)

    if not (args.cluster and args.server):
        print("cluster/server required for enable/disable/drain", file=sys.stderr)
        sys.exit(2)
    safe_toggle(args.action, args.cluster, args.server)
