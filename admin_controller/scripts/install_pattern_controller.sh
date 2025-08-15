#!/usr/bin/env bash
# install_user_mode.sh — user-mode запуск без systemd
# корень: /minjust-74-250/gbdfl/pattern_controller

set -euo pipefail

ROOT="/minjust-74-250/gbdfl/pattern_controller"
BIN="$ROOT/bin"
LOG="$ROOT/logs"
REP="$ROOT/report"
SIG="$ROOT/signals"
PID="$ROOT/pids"

# ----- 0) Деревья -----
mkdir -p "$BIN" "$LOG" "$REP"/{metrics,graphs,summaries} "$SIG"/{haproxy_ops,haproxy_ops_inprogress,haproxy_ops_done,haproxy_ops_failed,events} "$PID" "$ROOT/scripts"

# ----- 1) Проверка нужных .py в BIN -----
need=( monitor_35072.py haproxy_ops_worker.py haproxy_runtime.py haproxy_cfg_parser.py stats_collector_haproxy.py api_server.py worker_rebooter.py )
miss=0
for f in "${need[@]}"; do
  if [[ ! -f "$BIN/$f" ]]; then echo "[-] нет $BIN/$f"; miss=1; fi
done
if [[ "$miss" -ne 0 ]]; then
  cat <<HINT
[!] Скопируй недостающие файлы в $BIN и перезапусти скрипт.
   Минимум нужно:
     monitor_35072.py
     haproxy_ops_worker.py
     stats_collector_haproxy.py
     api_server.py
     worker_rebooter.py
HINT
  exit 2
fi
chmod 0755 "$BIN"/*.py || true

# ----- 2) env.sh -----
cat > "$ROOT/scripts/env.sh" <<'EOF'
#!/usr/bin/env bash
export ROOT="/minjust-74-250/gbdfl/pattern_controller"
export FLAG_DIR="$ROOT/signals"
export REPORT_DIR="$ROOT/report"
export LOG_DIR="$ROOT/logs"
export PID_DIR="$ROOT/pids"

# HAProxy runtime socket (на входной машине)
export HAPROXY_SOCK="/var/lib/haproxy/haproxy.sock"
export HAPROXY_BACKENDS="Jboss_client"

# UI
export UI_PORT=35072
export UI_REFRESH=5
export TOGGLE_SECRET="S3CR3T"

# API
export API_BIND="127.0.0.1"
export API_PORT=35073
EOF
chmod +x "$ROOT/scripts/env.sh"

# ----- 3) run_scripts -----
cat > "$ROOT/scripts/run_ops_worker.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
cmd=(/usr/bin/python2 "$ROOT/bin/haproxy_ops_worker.py"
  --socket "$HAPROXY_SOCK"
  --cfg /etc/haproxy/haproxy.cfg
  --backends "$HAPROXY_BACKENDS"
  --queue-dir "$ROOT/signals/haproxy_ops"
  --inprogress-dir "$ROOT/signals/haproxy_ops_inprogress"
  --done-dir "$ROOT/signals/haproxy_ops_done"
  --failed-dir "$ROOT/signals/haproxy_ops_failed"
  --report-dir "$REPORT_DIR"
  --log-dir "$LOG_DIR"
  --reload-cmd "systemctl reload haproxy"
  --poll-sec 2)
nohup "${cmd[@]}" >>"$LOG_DIR/haproxy_ops_worker.out" 2>&1 &
echo $! > "$PID_DIR/haproxy_ops_worker.pid"
echo "[ok] ops_worker pid=$(cat "$PID_DIR/haproxy_ops_worker.pid")"
EOF

cat > "$ROOT/scripts/run_stats_collector.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
cmd=(/usr/bin/python2 "$ROOT/bin/stats_collector_haproxy.py"
  --socket "$HAPROXY_SOCK"
  --out-dir "$REPORT_DIR/metrics"
  --every-sec 15
  --backends "$HAPROXY_BACKENDS")
nohup "${cmd[@]}" >>"$LOG_DIR/stats_collector.out" 2>&1 &
echo $! > "$PID_DIR/stats_collector.pid"
echo "[ok] stats_collector pid=$(cat "$PID_DIR/stats_collector.pid")"
EOF

cat > "$ROOT/scripts/run_api.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
cmd=(/usr/bin/python2 "$ROOT/bin/api_server.py"
  --bind "$API_BIND"
  --port "$API_PORT"
  --report "$REPORT_DIR")
nohup "${cmd[@]}" >>"$LOG_DIR/api_server.out" 2>&1 &
echo $! > "$PID_DIR/api_server.pid"
echo "[ok] api_server pid=$(cat "$PID_DIR/api_server.pid")"
EOF

cat > "$ROOT/scripts/run_ui.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
cmd=(/usr/bin/python2 "$ROOT/bin/monitor_35072.py"
  --flag-dir "$FLAG_DIR"
  --controller-report-dir "$REPORT_DIR"
  --worker-report-dirs "$REPORT_DIR"
  --error-log-dir "$LOG_DIR"
  --port "$UI_PORT"
  --refresh-sec "$UI_REFRESH"
  --toggle-secret "$TOGGLE_SECRET"
  --haproxy-backends "$HAPROXY_BACKENDS"
  --haproxy-ops-queue "$ROOT/signals/haproxy_ops")
nohup "${cmd[@]}" >>"$LOG_DIR/monitor_35072.out" 2>&1 &
echo $! > "$PID_DIR/monitor_35072.pid"
echo "[ok] monitor_35072 pid=$(cat "$PID_DIR/monitor_35072.pid")"
EOF

cat > "$ROOT/scripts/start_all.sh" <<'EOF'
#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$DIR/run_ops_worker.sh"
bash "$DIR/run_stats_collector.sh"
bash "$DIR/run_api.sh"
bash "$DIR/run_ui.sh"
echo "[ok] started all"
EOF

cat > "$ROOT/scripts/stop_all.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
stop_one() {
  local name="$1"; local pidf="$PID_DIR/$name.pid"
  if [[ -f "$pidf" ]]; then
    local pid; pid=$(cat "$pidf" || true)
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true; sleep 1; kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidf"; echo "[ok] stopped $name"
  else
    echo "[..] no pid for $name"
  fi
}
stop_one haproxy_ops_worker
stop_one stats_collector
stop_one api_server
stop_one monitor_35072
EOF

cat > "$ROOT/scripts/status_all.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/env.sh"
for n in haproxy_ops_worker stats_collector api_server monitor_35072; do
  pidf="$PID_DIR/$n.pid"
  if [[ -f "$pidf" ]]; then
    pid=$(cat "$pidf" 2>/dev/null || true)
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "$n: RUNNING pid=$pid"
    else
      echo "$n: STALE ($pid)"
    fi
  else
    echo "$n: STOPPED"
  fi
done
EOF

# агент для нод (локально на ноде вызывать)
cat > "$ROOT/scripts/run_agent_node.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="/minjust-74-250/gbdfl/pattern_controller"
NODE="${1:?usage: run_agent_node.sh <node_name>}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8080/health}"
LOG_PATH="${LOG_PATH:-/opt/jboss/standalone/log/server.log}"
TCP_HOST="${TCP_HOST:-127.0.0.1}"
TCP_PORT="${TCP_PORT:-8080}"
mkdir -p "$ROOT/pids" "$ROOT/logs" "$ROOT/report"
nohup /usr/bin/python2 "$ROOT/bin/worker_rebooter.py" \
  --node "$NODE" \
  --flag-dir "$ROOT/signals" \
  --report-dir "$ROOT/report" \
  --log-dir "$ROOT/logs" \
  --health-url "$HEALTH_URL" \
  --log-path "$LOG_PATH" \
  --tcp-host "$TCP_HOST" \
  --tcp-port "$TCP_PORT" \
  --verify-timeout 240 \
  --verify-every 5 \
  >>"$ROOT/logs/worker_${NODE}.out" 2>&1 &
echo $! > "$ROOT/pids/worker_${NODE}.pid"
echo "[ok] agent $NODE pid=$(cat "$ROOT/pids/worker_${NODE}.pid")"
EOF

chmod +x "$ROOT"/scripts/*.sh

cat <<EOM

[OK] Готово.

запуск от текущего пользователя (например, support):
  $ROOT/scripts/start_all.sh

проверить:
  $ROOT/scripts/status_all.sh
  tail -f $LOG/*.out

UI:  http://<входная_машина>:35072
API: http://127.0.0.1:35073/api/metrics?range=5m&backend=Jboss_client

агент на НОДЕ (выполнить на самой ноде):
  $ROOT/scripts/run_agent_node.sh node_150

важно:
- пользователю должен быть доступен сокет HAProxy: /var/lib/haproxy/haproxy.sock
  (обычно: добавить пользователя в группу 'haproxy' и перелогиниться)
EOM
