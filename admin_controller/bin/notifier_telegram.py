#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
notifier_telegram.py — читает /signals/events/*.json, отправляет в TG, делает rate-limit.
"""
from __future__ import annotations
import argparse, json, time, urllib.parse, urllib.request
from pathlib import Path
from path_utils import BASE, SIGNALS_DIR, REPORT_DIR

DEFAULT_EVENTS = SIGNALS_DIR / "events"
DEFAULT_SENT   = BASE / "report" / "sent_events.json"
RATE_LIMIT_SEC = 10  # не чаще одного запроса в N секунд

def send_tg(bot_token: str, chat_id: str, text: str, timeout: int = 10) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.getcode() == 200

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", default=str(DEFAULT_EVENTS))
    ap.add_argument("--sent-json",   default=str(DEFAULT_SENT))
    ap.add_argument("--bot-token",   required=True)
    ap.add_argument("--chat-id",     required=True)
    ap.add_argument("--rate-sec",    type=int, default=RATE_LIMIT_SEC)
    args = ap.parse_args(argv)

    events_dir = Path(args.events_dir); events_dir.mkdir(parents=True, exist_ok=True)
    sent_path  = Path(args.sent_json);  sent_path.parent.mkdir(parents=True, exist_ok=True)

    # грузим кэш уже отправленных
    try:
        sent = json.loads(sent_path.read_text(encoding="utf-8"))
        if not isinstance(sent, dict): sent = {}
    except Exception:
        sent = {}

    last_ts = 0.0
    changed = False
    for fn in sorted(p.name for p in events_dir.glob("*.json")):
        if fn in sent: continue
        path = events_dir / fn
        try:
            ev = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        text = f"[{ev.get('severity','info')}] {ev.get('text','')}"
        # rate limit
        now = time.time()
        if now - last_ts < args.rate_sec:
            time.sleep(args.rate_sec - (now - last_ts))
        if send_tg(args.bot_token, args.chat_id, text):
            sent[fn] = int(time.time()); changed = True
            last_ts = time.time()

    if changed:
        sent_path.write_text(json.dumps(sent, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
