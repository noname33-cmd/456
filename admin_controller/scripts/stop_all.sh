#!/bin/bash
pkill -f monitor_35072.py
pkill -f queue_dispatcher.py
pkill -f haproxy_ops_worker.py
pkill -f stats_collector_haproxy.py
