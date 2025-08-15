#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
policy_auto_drain.py — авто-действия по метрикам/флагам (Py3.11).

Логика:
  1) Если для server verify=FAIL (done_<node>.txt) И рост 5xx за 5 минут > --thr-5xx,
     кладём заявки в haproxy_ops: drain + weight=0.
  2) Если затем verify=OK держится --heal-min минут и 5xx за 5 минут < --heal-5xx,
     возвращаем weight=1 и enable.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

BASE = Path(os.environ.get("PC_BASE", "/tmp/pattern_controller"))
DEFAULT_SIGNALS = BASE / "signals"
DEFAULT_REPORT  = BASE / "report"
DEFAULT_OPS_Q   = DEFAULT_SIGNALS / "haproxy_ops"
EVENTS_DIR      = DEFAULT_SIGNALS / "events"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def emit_event(text: str, severity: str = "info", node: Optional[str] = None) -> None:
    ensure_dir(EVENTS_DIR)
    tsid = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    obj = {"ts": tsid, "severity": severity, "source": "auto_drain", "node": node, "text": text}
    (EVENTS_DIR / f"ev_{tsid}_auto_drain_{node or 'na'}.json").write_text(json.dumps(obj), encoding="utf-8")


def enqueue_op(qdir: Path, op: str, scope: str, backend: str, server: str, extra: Optional[Dict]=None) -> Tuple[bool, str]:
    ensure_dir(qdir)
    rid = f"{int(time.time()*1000):08d}"[-8:]
    tsid = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    payload = {"id": rid, "ts": tsid, "op": op, "scope": scope, "backend": backend, "server": server}
    if extra:
        payload.update(extra)
    name = f"rq_{tsid}_{backend or 'be'}_{server or 'srv'}_{rid}.json"
    path = qdir / name
    try:
        path.write_text(json.dumps(payload), encoding="utf-8")
        return True, str(path)
    except Exception as e:
        return False, str(e)


def read_agg_5m(agg_path: Path) -> Dict[str, Dict[str, int]]:
    try:
        return json.loads(agg_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def list_nodes_from_verify(flag_dir: Path) -> set[str]:
    nodes: set[str] = set()
    if not flag_dir.is_dir():
        return nodes
    for n in os.listdir(flag_dir):
        if n.startswith("done_") and n.endswith(".txt"):
            nodes.add(n[5:-4])
        if n.startswith("restart_") and n.endswith(".txt"):
            nodes.add(n[8:-4])
    return nodes


def read_verify(flag_dir: Path, node: str) -> Tuple[Optional[str], Optional[float]]:
    okp = flag_dir / f"done_{node}.txt"
    if not okp.exists():
        return None, None
    try:
        data = okp.read_bytes()
        try:
            txt = data.decode("utf-8", "ignore")
        except Exception:
            txt = data.decode("latin-1", "ignore")
        ts = okp.stat().st_mtime
        s = txt.lower()
        if "verify=ok" in s:
            return "OK", ts
        if "verify=fail" in s:
            return "FAIL", ts
        return None, ts
    except Exception:
        return None, None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Auto drain policy (Py3.11)")
    ap.add_argument("--signals-dir", default=str(DEFAULT_SIGNALS))
    ap.add_argument("--report-dir", default=str(DEFAULT_REPORT))
    ap.add_argument("--ops-queue",  default=str(DEFAULT_OPS_Q))
    ap.add_argument("--backend", default="Jboss_client")
    ap.add_argument("--thr-5xx", type=int, default=20)
    ap.add_argument("--heal-5xx", type=int, default=2)
    ap.add_argument("--heal-min", type=int, default=10)
    args = ap.parse_args(argv)

    agg5_path = Path(args.report_dir) / "metrics" / "agg_5m.json"
    agg = read_agg_5m(agg5_path)  # key "backend/server": {sum_5xx, ...}

    flag_dir = Path(args.signals_dir)
    nodes = list_nodes_from_verify(flag_dir)

    # простая state-БД
    state_path = Path(args.report_dir) / "auto_drain_state.json"
    try:
        state: Dict[str, Dict[str, Any]] = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        state = {}

    backend = args.backend
    qdir = Path(args.ops_queue)

    now_ts = time.time()
    changed = False

    for node in sorted(nodes):
        # Определяем backend/server: предполагаем server == node
        be, srv = backend, node

        key = f"{be}/{srv}"
        m = agg.get(key, {})
        sum_5xx = int(m.get("sum_5xx", 0))

        verify, vts = read_verify(flag_dir, node)
        vts = vts or 0.0

        st = state.get(node) or {"phase": "idle", "last": 0}
        phase = st.get("phase", "idle")

        if phase in ("idle", "healed"):
            if verify == "FAIL" and sum_5xx > args.thr_5xx:
                ok1, _ = enqueue_op(qdir, "drain", "runtime", be, srv, {})
                ok2, _ = enqueue_op(qdir, "weight", "runtime", be, srv, {"weight": 0})
                if ok1 or ok2:
                    emit_event(f"auto_drain: drain+weight0 {key}", severity="warn", node=node)
                    state[node] = {"phase": "drained", "last": now_ts}
                    changed = True

        elif phase == "drained":
            if verify == "OK" and (now_ts - vts) >= (args.heal_min * 60) and sum_5xx < args.heal_5xx:
                ok1, _ = enqueue_op(qdir, "weight", "runtime", be, srv, {"weight": 1})
                ok2, _ = enqueue_op(qdir, "enable", "runtime", be, srv, {})
                if ok1 or ok2:
                    emit_event(f"auto_drain: enable+weight1 {key}", severity="info", node=node)
                    state[node] = {"phase": "healed", "last": now_ts}
                    changed = True

    if changed:
        try:
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
