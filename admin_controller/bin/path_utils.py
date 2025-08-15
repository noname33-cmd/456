#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
path_utils.py — единая точка путей /tmp/pattern_controller (Py3.11).

- База: /tmp/pattern_controller (можно переопределить env PC_BASE)
- Идентичность ноды: HOSTNAME (безопасная), NODE_ID (стабильная из MAC+hostname)
- Каталоги: logs/<HOST>, report/<HOST>, signals/ (общая шина)
"""

from __future__ import annotations
import os, re, uuid, hashlib, socket
from pathlib import Path
from typing import Tuple

# --- база
_DEF_BASE = "/tmp/pattern_controller"
PC_BASE = os.environ.get("PC_BASE", _DEF_BASE)
BASE = Path(PC_BASE)

# --- hostname / node_id
def _safe_hostname() -> str:
    raw = socket.gethostname().strip().lower()
    return re.sub(r"[^a-z0-9._-]+", "-", raw)[:63] or "unknown"

def _load_or_make_node_id(base: Path) -> str:
    p = base / "node_id"
    try:
        if p.exists():
            return p.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    seed = f"{uuid.getnode():012x}-{_safe_hostname()}".encode("utf-8")
    nid = hashlib.sha1(seed).hexdigest()[:12]
    try:
        base.mkdir(parents=True, exist_ok=True)
        p.write_text(nid, encoding="utf-8")
    except Exception:
        pass
    return nid

HOSTNAME = _safe_hostname()
NODE_ID  = _load_or_make_node_id(BASE)

# --- ключевые каталоги
LOGS_DIR    = BASE / "logs"   / HOSTNAME
REPORT_DIR  = BASE / "report" / HOSTNAME
SIGNALS_DIR = BASE / "signals"

for d in (LOGS_DIR, REPORT_DIR, SIGNALS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# --- метрики
def metrics_root() -> Path:
    p = REPORT_DIR / "metrics"
    p.mkdir(parents=True, exist_ok=True)
    return p

def metrics_raw_dir(day_yyyymmdd: str) -> Path:
    p = metrics_root() / "raw" / day_yyyymmdd
    p.mkdir(parents=True, exist_ok=True)
    return p

# --- очереди HAProxy
def haproxy_ops_dirs() -> Tuple[Path, Path, Path, Path]:
    q  = SIGNALS_DIR / "haproxy_ops"
    ip = SIGNALS_DIR / "haproxy_ops_inprogress"
    dn = SIGNALS_DIR / "haproxy_ops_done"
    fl = SIGNALS_DIR / "haproxy_ops_failed"
    for d in (q, ip, dn, fl): d.mkdir(parents=True, exist_ok=True)
    return q, ip, dn, fl
