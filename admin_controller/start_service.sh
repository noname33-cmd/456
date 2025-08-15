#!/usr/bin/env bash
set -euo pipefail

# === пути и имена ===
ENV_FILE="/etc/systemd/system/pattern-controller.env"
SYSTEMD_DIR="/etc/systemd/system"

# === создаём env ===
cat > "$ENV_FILE" <<'EOF'
PC_BASE=/tmp/pattern_controller

# монитор
MON_PORT=35072
TOGGLE_SECRET=change_me_secret

# HAProxy runtime/cfg
HAPROXY_SOCKET=/var/lib/haproxy/haproxy.sock
HAPROXY_TCP=
HAPROXY_CFG=/etc/haproxy/haproxy.cfg
HAPROXY_RELOAD_CMD=systemctl reload haproxy
HAPROXY_BACKENDS=Jboss_client
HAPROXY_PARSE_INTERVAL=60

# peers (вкладки)
PEER_TABS=primary@55.51,55.52

# воркеров для monitor async job queue
MON_WORKERS=32

# api_server
API_BIND=0.0.0.0
API_PORT=35073

# пути (оставляем дефолт, но можно переопределить)
FLAG_DIR=/tmp/pattern_controller/signals
REPORT_DIR=/tmp/pattern_controller/report
WORKER_REPORT_DIRS=/tmp/pattern_controller/report
LOG_DIR=/tmp/pattern_controller/logs
EOF

# === функция для генерации unit-файлов ===
make_unit() {
    local name="$1"
    shift
    cat > "${SYSTEMD_DIR}/${name}" <<EOF
$@
EOF
}

# === сервисы и таймеры ===

# retry (safe_haproxy_toggle)
make_unit pattern_controller-retry.service "[Unit]
Description=pattern_controller: retry deferred HAProxy actions (min-enabled guard)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/controller/safe_haproxy_toggle.py --action retry
User=root
Group=root

[Install]
WantedBy=multi-user.target"

make_unit pattern_controller-retry.timer "[Unit]
Description=Run pattern_controller HAProxy guard retry every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=1s
Persistent=true

[Install]
WantedBy=timers.target"

# guarded_toggle retry
make_unit pattern_controller-guarded-retry.service "[Unit]
Description=Guarded toggle: retry deferred actions for all clusters
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/controller/guarded_toggle.py --action retry
User=root
Group=root

[Install]
WantedBy=multi-user.target"

make_unit pattern_controller-guarded-retry.timer "[Unit]
Description=Run guarded toggle retry every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=1s
Persistent=true

[Install]
WantedBy=timers.target"

# api_server
make_unit pattern-api-server.service "[Unit]
Description=pattern_controller: API server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/api_server --bind \${API_BIND} --port \${API_PORT}
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# monitor_35072
make_unit pattern-monitor.service "[Unit]
Description=pattern_controller: monitor_35072
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/monitor_35072
Restart=always
RestartSec=5
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# haproxy_ops_worker
make_unit pattern-haproxy-ops-worker.service "[Unit]
Description=pattern_controller: HAProxy ops worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/haproxy_ops_worker
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# graph_builder
make_unit pattern-graph-builder.service "[Unit]
Description=pattern_controller: graph builder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/graph_builder
Restart=always
RestartSec=5
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# stats_collector_haproxy
make_unit pattern-stats-collector.service "[Unit]
Description=pattern_controller: HAProxy stats collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/stats_collector_haproxy
Restart=always
RestartSec=10
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# main controller
make_unit pattern-controller.service "[Unit]
Description=pattern_controller: main controller (queue/policies)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=\${PC_BASE}
ExecStart=/usr/bin/python3 \${PC_BASE}/bin/pattern_controller
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target"

# === перезагрузка и активация ===
systemctl daemon-reload
systemctl enable --now pattern_controller-retry.timer
systemctl enable --now pattern_controller-guarded-retry.timer
systemctl enable --now pattern-api-server.service
systemctl enable --now pattern-monitor.service
systemctl enable --now pattern-haproxy-ops-worker.service
systemctl enable --now pattern-graph-builder.service
systemctl enable --now pattern-stats-collector.service
systemctl enable --now pattern-controller.service

echo "Все юниты созданы и запущены."
