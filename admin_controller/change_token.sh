#!/usr/bin/env bash
set -euo pipefail

NEW_TOKEN="${1:-}"
if [ -z "$NEW_TOKEN" ]; then
    echo "Использование: $0 <новый_токен>"
    exit 1
fi

# Пути к конфигам мастера и агентов
MASTER_CFG="/tmp/pattern_controller/master/config.yaml"
AGENT_CFG="/tmp/pattern_controller/agent/config.yaml"

# IP-адреса всех агентов (мастер сюда не включаем)
HOSTS=("55.51" "55.52" "55.146" "55.147")

update_config_yaml() {
    local file="$1"
    if [ ! -f "$file" ]; then
        echo "[WARN] Файл $file не найден — пропускаем"
        return
    fi
    sed -i -E "s|^(auth_token:\s*).*$|\1\"$NEW_TOKEN\"|" "$file"
    echo "[OK] Обновлён токен в $file"
}

restart_service_if_exists() {
    local svc="$1"
    if systemctl list-units --full -all | grep -q "$svc"; then
        systemctl restart "$svc"
        echo "[OK] Перезапущен сервис $svc"
    else
        echo "[INFO] Сервис $svc не найден — пропускаем"
    fi
}

echo "=== Обновляем мастер ==="
update_config_yaml "$MASTER_CFG"
restart_service_if_exists "jboss-controller-master"

for host in "${HOSTS[@]}"; do
    echo "=== Обновляем агент на $host ==="
    ssh root@"$host" bash -s <<EOF
NEW_TOKEN="$NEW_TOKEN"
AGENT_CFG="$AGENT_CFG"
$(declare -f update_config_yaml)
$(declare -f restart_service_if_exists)
update_config_yaml "\$AGENT_CFG"
restart_service_if_exists "jboss-controller-agent"
EOF
done

echo "=== Готово! Новый токен: $NEW_TOKEN ==="
