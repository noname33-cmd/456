#!/bin/bash
ps -ef | grep -E "monitor_35072|queue_dispatcher|haproxy_ops_worker|stats_collector_haproxy|worker_rebooter" | grep -v grep
