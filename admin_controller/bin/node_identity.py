#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
node_identity.py — единая точка для идентификации ноды и базовых путей.

- NODE_ID: стабильный короткий идентификатор (по MAC+hostname), хранится в /tmp/pattern_controller/node_id
- HOSTNAME_SAFE: hostname в безопасном виде (a-z0-9._-)
- BASE: корень контура (PC_BASE или /tmp/pattern_controller)
- LOGS_DIR / REPORT_DIR / SIGNALS_DIR: директории по умолчанию (с автонеймспейсом)
"""
from __future__ import annotations
import os, re, uuid, hashlib, socket
from pathlib import Path

def _base() -> Path:
    return Path(os.environ.get("PC_BASE", "/tmp/pattern_controller"))

def _safe_hostname() -> str:
    raw = socket.gethostname()
    s = re.sub(r"[^a-z0-9._-]+", "-", raw.strip().lower())
    return (s or "unknown")[:63]

def _node_id(base: Path) -> str:
    nid_path = base / "node_id"
    if nid_path.exists():
        return nid_path.read_text(encoding="utf-8").strip()
    seed = f"{uuid.getnode():012x}-{_safe_hostname()}".encode("utf-8")
    nid = hashlib.sha1(seed).hexdigest()[:12]
    base.mkdir(parents=True, exist_ok=True)
    nid_path.write_text(nid, encoding="utf-8")
    return nid

BASE = _base()
HOSTNAME_SAFE = _safe_hostname()
NODE_ID = _node_id(BASE)

LOGS_DIR    = BASE / "logs" / HOSTNAME_SAFE
REPORT_DIR  = BASE / "report" / HOSTNAME_SAFE
SIGNALS_DIR = BASE / "signals"

for p in (LOGS_DIR, REPORT_DIR, SIGNALS_DIR):
    p.mkdir(parents=True, exist_ok=True)
