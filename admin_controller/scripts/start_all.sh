#!/bin/bash
ROOT="/minjust-74-250/gdbfl/pattern_controller"
cd "$ROOT" || exit 1

# UI и диспетчер очередей
nohup python monitor_35072.py \
    --haproxy-ops-queue "$ROOT/signals/haproxy_ops" \
    --haproxy-backends "Jboss_client" \
    >> "$ROOT/logs/monitor_35072.out" 2>&1 &

nohup python queue_dispatcher.py >> "$ROOT/logs/queue_dispatcher.out" 2>&1 &
nohup python haproxy_ops_worker.py >> "$ROOT/logs/haproxy_ops_worker.out" 2>&1 &
nohup python stats_collector_haproxy.py >> "$ROOT/logs/stats_collector_haproxy.out" 2>&1 &
