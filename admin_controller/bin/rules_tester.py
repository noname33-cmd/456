#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rules_tester.py — тест правил по логам (Py3.11)

Пример:
  python3 rules_tester.py \
    --rules-file rules.json \
    --logs /var/log/app/app.log /var/log/app/app2.log \
    --node srv_55_51_1 \
    --limit 50 \
    --out /tmp/pattern_controller/report/rules_tester.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_rules(path: Path) -> List[Dict[str, str]]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        out = []
        for r in obj:
            out.append({
                "pattern": r["pattern"],
                "severity": r.get("severity", "info"),
                "action": r.get("action", "notify"),
            })
        return out
    except Exception as e:
        print(f"ERR: failed to load rules: {e}", file=sys.stderr)
        return []


def find_matches(rules: List[Dict[str, str]], logs: List[Path], limit: int | None) -> List[Dict[str, Any]]:
    compiled = [{"rx": re.compile(r["pattern"], re.I | re.U), **r} for r in rules]
    found: List[Dict[str, Any]] = []
    for lp in logs:
        if not lp.exists():
            continue
        try:
            with lp.open("r", encoding="utf-8", errors="ignore") as f:
                for ln in f:
                    for r in compiled:
                        if r["rx"].search(ln):
                            found.append({
                                "log": str(lp),
                                "pattern": r["rx"].pattern,
                                "severity": r["severity"],
                                "action": r["action"],
                                "line": ln.rstrip()[:800],
                            })
                            break
                    if limit and len(found) >= limit:
                        return found
        except Exception:
            continue
    return found


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Rules tester (Py3.11)")
    ap.add_argument("--rules-file", required=True)
    ap.add_argument("--logs", nargs="+", required=True)
    ap.add_argument("--node", default="unknown")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--out", help="jsonl-файл для результатов")
    args = ap.parse_args(argv)

    rules = load_rules(Path(args.rules_file))
    logs = [Path(x) for x in args.logs]
    matches = find_matches(rules, logs, args.limit)

    # вывод в консоль
    for m in matches:
        print(f"[{m['severity']}] {m['action']} :: {m['pattern']} :: {m['log']} :: {m['line']}")

    # jsonl
    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", encoding="utf-8") as f:
            for m in matches:
                f.write(json.dumps({"node": args.node, **m}, ensure_ascii=False) + "\n")

    print(f"total matches: {len(matches)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
