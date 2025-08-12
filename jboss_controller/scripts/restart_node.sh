#!/bin/bash
NODE="$1"
if [ -z "$NODE" ]; then
    echo "Usage: $0 node_name"
    exit 1
fi

echo "request_reboot node=${NODE}" > /minjust-74-250/gbdfl/pattern_controller/signals/restart_${NODE}.txt
