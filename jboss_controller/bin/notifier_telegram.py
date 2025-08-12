#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
notifier_telegram.py — единая точка отправки событий в TG (Py2.7).
Читает /signals/events/*.json, отправляет через Bot API, делает rate-limit.
"""

import os, sys, time, json, argparse
import urllib2

BASE = os.environ.get("PC_BASE", "/tmp/pattern_controller")
DEFAULT_EVENTS = os.path.join(BASE, "signals", "events")
DEFAULT_SENT   = os.path.join(BASE, "report", "sent_events.json")
BOT_TOKEN      = "XXX:YYYY"
CHAT_ID        = "-1001234567890"
RATE_LIMIT_SEC = 10

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

def send_tg(text):
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % BOT_TOKEN
        data = json.dumps({"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True})
        req = urllib2.Request(url, data, {"Content-Type": "application/json"})
        urllib2.urlopen(req, timeout=5).read()
        return True
    except Exception as e:
        sys.stderr.write("TG send error: %s\n" % e)
        return False

def main():
    ap = argparse.ArgumentParser(description="Send queued events to TG (Py2.7)")
    ap.add_argument("--events-dir", default=DEFAULT_EVENTS)
    ap.add_argument("--sent-file",  default=DEFAULT_SENT)
    args = ap.parse_args()

    ensure_dir(args.events_dir)
    try:
        sent = json.load(open(args.sent_file, "rb"))
    except:
        sent = {}

    now = time.time()
    for fn in sorted(os.listdir(args.events_dir)):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(args.events_dir, fn)
        if fn in sent:
            continue
        try:
            ev = json.load(open(path, "rb"))
            text = "[%s] %s" % (ev.get("severity", "info"), ev.get("text", ""))
            if send_tg(text):
                sent[fn] = now
                time.sleep(RATE_LIMIT_SEC)
        except Exception as e:
            sys.stderr.write("event %s error: %s\n" % (fn, e))

    try:
        open(args.sent_file, "wb").write(json.dumps(sent, ensure_ascii=False))
    except:
        pass

    sys.stdout.write("notifier_telegram done\n")

if __name__ == "__main__":
    main()
