#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
discovery_from_haproxy.py — извлекает список server из backend в haproxy.cfg
и (опционально) включает systemd-инстансы jboss-worker@<node>.service.
"""
from __future__ import annotations

import argparse, os, re, subprocess, sys
from pathlib import Path
from typing import List

def parse_servers(cfg_path: str | os.PathLike, backend_name: str) -> List[str]:
    if not os.path.isfile(cfg_path):
        return []
    names: List[str] = []
    in_bk = False
    bk_re = re.compile(r"^\s*backend\s+" + re.escape(backend_name) + r"\b")
    srv_re = re.compile(r"^\s*(?:#\s*)?server\s+(\S+)\s+", re.I)
    with open(cfg_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if bk_re.match(line):
                in_bk = True
                continue
            if in_bk and re.match(r"^\s*(frontend|backend|listen|global|defaults)\b", line):
                in_bk = False
            if in_bk:
                m = srv_re.match(line)
                if m:
                    names.append(m.group(1))
    return sorted(set(names))

def ensure_enabled(unit: str, dry_run: bool = False) -> None:
    if dry_run:
        sys.stdout.write(f"would ensure enabled: {unit}\n")
        return
    rc = subprocess.call(["systemctl", "is-enabled", "--quiet", unit])
    if rc != 0:
        subprocess.call(["systemctl", "enable", "--now", unit])
        sys.stdout.write(f"enabled {unit}\n")

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="HAProxy discovery (Py3.11)")
    ap.add_argument("--cfg", required=True)
    ap.add_argument("--backend", required=True)
    ap.add_argument("--systemd-dir", default="/etc/systemd/system")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--enable-missing", action="store_true")
    args = ap.parse_args(argv)

    servers = parse_servers(args.cfg, args.backend)
    sys.stdout.write("servers: %s\n" % ", ".join(servers))

    if args.enable_missing:
        for s in servers:
            unit = f"jboss-worker@{s}.service"
            ensure_enabled(unit, dry_run=args.dry_run)

    sys.stdout.write("discovery OK, servers=%d\n" % len(servers))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
