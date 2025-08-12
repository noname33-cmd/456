# FILE: discovery_from_haproxy.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
discovery_from_haproxy.py — извлекает список server из backend в haproxy.cfg
и (опционально) генерирует/включает systemd-инстансы jboss-worker@<node>.service.

Пример:
  python2 discovery_from_haproxy.py --cfg /etc/haproxy/haproxy.cfg \
    --backend Jboss_client --systemd-dir /etc/systemd/system --dry-run
"""

import os, sys, re, argparse, subprocess

def parse_servers(cfg_path, backend_name):
    if not os.path.isfile(cfg_path): return []
    try:
        txt = open(cfg_path,"rb").read()
        try: txt = txt.decode("utf-8")
        except: txt = txt.decode("latin-1","ignore")
    except:
        return []
    servers = []
    cur = None
    for line in txt.splitlines():
        st = line.strip()
        if st.startswith("backend "):
            cur = st.split(None,1)[1].strip()
            continue
        if cur != backend_name:
            continue
        m = re.match(r'^\s*(#\s*)?server\s+([^\s]+)\s+(\S+)', line)
        if m:
            name = m.group(2)
            servers.append(name)
    return sorted(list(set(servers)))

def main():
    ap = argparse.ArgumentParser(description="HAProxy discovery → jboss-worker@<node>")
    ap.add_argument("--cfg", default="/etc/haproxy/haproxy.cfg")
    ap.add_argument("--backend", default="Jboss_client")
    ap.add_argument("--systemd-dir", default="/etc/systemd/system")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--enable-missing", action="store_true", help="systemctl enable --now для новых")
    args = ap.parse_args()

    servers = parse_servers(args.cfg, args.backend)
    for s in servers:
        unit = "jboss-worker@%s.service" % s
        unit_path = os.path.join(args.systemd_dir, unit)
        if args.dry_run:
            sys.stdout.write("would ensure enabled: %s\n" % unit)
            continue
        if args.enable_missing:
            rc = subprocess.call(["systemctl","is-enabled","--quiet", unit])
            if rc != 0:
                subprocess.call(["systemctl","enable","--now", unit])
                sys.stdout.write("enabled %s\n" % unit)

    sys.stdout.write("discovery OK, servers=%d\n" % len(servers))

if __name__ == "__main__":
    main()
