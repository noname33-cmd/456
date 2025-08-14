#!/bin/bash
ROOT="/minjust-74-250/gbdfl/pattern_controller"
NODE="$1"

if [ -z "$NODE" ]; then
    echo "Usage: $0 node_name"
    exit 1
fi

cd "$ROOT" || exit 1
nohup python worker_rebooter.py --node "$NODE" \
    >> "$ROOT/logs/worker_${NODE}.out" 2>&1 &
