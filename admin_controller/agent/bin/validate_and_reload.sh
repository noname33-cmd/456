#!/usr/bin/env bash
set -euo pipefail
CFG="/etc/haproxy/haproxy.cfg"
CMD="/usr/sbin/haproxy"

$CMD -c -f "$CFG"
systemctl reload haproxy
echo "OK: haproxy config validated and reloaded"
