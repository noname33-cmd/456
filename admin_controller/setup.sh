#!/usr/bin/env bash
set -euo pipefail

# =====================================================================
# Installer for pattern_controller systemd units + env + directories
# Creates dirs under /tmp/pattern_controller, writes unit files,
# deploys rules if uploaded, reloads systemd and enables services.
# =====================================================================

#Указываем путь ПО SYSTEMD_DIR=""

SYSTEMD_DIR="/etc/systemd/system"
ENV_FILE="${SYSTEMD_DIR}/pattern-controller.env"

# 0) Write environment file (edit values here if needed)
install_env() {
  mkdir -p "${SYSTEMD_DIR}"
  cat > "${ENV_FILE}" <<'EOF'
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
  chmod 0644 "${ENV_FILE}"
}

# 1) Create base directories using PC_BASE from env
create_dirs() {
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  mkdir -p "${PC_BASE}/controller" \
           "${PC_BASE}/bin" \
           "${PC_BASE}/signals/queue" \
           "${PC_BASE}/signals/locks" \
           "${PC_BASE}/report" \
           "${PC_BASE}/logs"
}

# 2) Optionally deploy rules from /mnt/data if present
deploy_rules() {
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  if [[ -f "/mnt/data/rules.json" ]]; then
    cp -f "/mnt/data/rules.json" "${PC_BASE}/report/rules.json"
    echo "[OK] Deployed rules.json -> ${PC_BASE}/report/"
  fi
  if [[ -f "/mnt/data/rules_safe.json" ]]; then
    cp -f "/mnt/data/rules_safe.json" "${PC_BASE}/report/rules_safe.json"
    echo "[OK] Deployed rules_safe.json -> ${PC_BASE}/report/"
  fi
}

# Helper to write a unit file with literals (${PC_BASE} stays for systemd to expand)
make_unit() {
  local name="$1"; shift
  install -m 0644 /dev/null "${SYSTEMD_DIR}/${name}"
  cat > "${SYSTEMD_DIR}/${name}" <<'EOF'
__CONTENT_PLACEHOLDER__
EOF
  # replace placeholder with provided content
  sed -i "1,/\_\_CONTENT\_PLACEHOLDER\_\_/c\\$*" "${SYSTEMD_DIR}/${name}"
}

# 3) Write unit files

write_units() {
  # pattern_controller-retry.service
  cat > "${SYSTEMD_DIR}/pattern_controller-retry.service" <<'EOF'
[Unit]
Description=pattern_controller: retry deferred HAProxy actions (min-enabled guard)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/controller/safe_haproxy_toggle.py --action retry
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern_controller-retry.timer
  cat > "${SYSTEMD_DIR}/pattern_controller-retry.timer" <<'EOF'
[Unit]
Description=Run pattern_controller HAProxy guard retry every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=1s
Persistent=true

[Install]
WantedBy=timers.target
EOF

  # pattern_controller-guarded-retry.service
  cat > "${SYSTEMD_DIR}/pattern_controller-guarded-retry.service" <<'EOF'
[Unit]
Description=Guarded toggle: retry deferred actions for all clusters
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/controller/guarded_toggle.py --action retry
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern_controller-guarded-retry.timer
  cat > "${SYSTEMD_DIR}/pattern_controller-guarded-retry.timer" <<'EOF'
[Unit]
Description=Run guarded toggle retry every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=1s
Persistent=true

[Install]
WantedBy=timers.target
EOF

  # pattern-api-server.service
  cat > "${SYSTEMD_DIR}/pattern-api-server.service" <<'EOF'
[Unit]
Description=pattern_controller: API server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/api_server --bind ${API_BIND} --port ${API_PORT}
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern-monitor.service
  cat > "${SYSTEMD_DIR}/pattern-monitor.service" <<'EOF'
[Unit]
Description=pattern_controller: monitor_35072
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/monitor_35072
Restart=always
RestartSec=5
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern-haproxy-ops-worker.service
  cat > "${SYSTEMD_DIR}/pattern-haproxy-ops-worker.service" <<'EOF'
[Unit]
Description=pattern_controller: HAProxy ops worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/haproxy_ops_worker
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern-graph-builder.service
  cat > "${SYSTEMD_DIR}/pattern-graph-builder.service" <<'EOF'
[Unit]
Description=pattern_controller: graph builder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/graph_builder
Restart=always
RestartSec=5
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern-stats-collector.service
  cat > "${SYSTEMD_DIR}/pattern-stats-collector.service" <<'EOF'
[Unit]
Description=pattern_controller: HAProxy stats collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/stats_collector_haproxy
Restart=always
RestartSec=10
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  # pattern-controller.service (main controller)
  cat > "${SYSTEMD_DIR}/pattern-controller.service" <<'EOF'
[Unit]
Description=pattern_controller: main controller (queue/policies)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/systemd/system/pattern-controller.env
WorkingDirectory=${PC_BASE}
ExecStart=/usr/bin/python3 ${PC_BASE}/bin/pattern_controller
Restart=always
RestartSec=3
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

  chmod 0644 "${SYSTEMD_DIR}/pattern_controller-retry.service" \
              "${SYSTEMD_DIR}/pattern_controller-retry.timer" \
              "${SYSTEMD_DIR}/pattern_controller-guarded-retry.service" \
              "${SYSTEMD_DIR}/pattern_controller-guarded-retry.timer" \
              "${SYSTEMD_DIR}/pattern-api-server.service" \
              "${SYSTEMD_DIR}/pattern-monitor.service" \
              "${SYSTEMD_DIR}/pattern-haproxy-ops-worker.service" \
              "${SYSTEMD_DIR}/pattern-graph-builder.service" \
              "${SYSTEMD_DIR}/pattern-stats-collector.service" \
              "${SYSTEMD_DIR}/pattern-controller.service"
}

# 4) Reload systemd & enable services/timers
enable_units() {
  systemctl daemon-reload
  systemctl enable --now pattern_controller-retry.timer
  systemctl enable --now pattern_controller-guarded-retry.timer
  systemctl enable --now pattern-api-server.service
  systemctl enable --now pattern-monitor.service
  systemctl enable --now pattern-haproxy-ops-worker.service
  systemctl enable --now pattern-graph-builder.service
  systemctl enable --now pattern-stats-collector.service
  systemctl enable --now pattern-controller.service
}

# 5) Summary
summary() {
  echo "================================================================"
  echo "[OK] Installed env: ${ENV_FILE}"
  echo "[OK] Created systemd units in: ${SYSTEMD_DIR}"
  echo "Timers status:"
  systemctl --no-pager --full status pattern_controller-retry.timer || true
  systemctl --no-pager --full status pattern_controller-guarded-retry.timer || true
  echo "Services status (short):"
  systemctl --no-pager --full --no-legend --plain list-units \
    'pattern-*.service' 'pattern_controller-*.service' || true
  echo "================================================================"
}

main() {
  install_env
  create_dirs
  deploy_rules
  write_units
  enable_units
  summary
}

main "$@"
