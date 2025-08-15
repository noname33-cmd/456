#!/usr/bin/env bash
set -euo pipefail

BASE="/tmp/pattern_controller"
REPORT="$BASE/report"
mkdir -p "$REPORT"

# Если вы загрузили rules в /mnt/data, скопируйте:
if [ -f "/mnt/data/rules.json" ]; then
  cp -f "/mnt/data/rules.json" "$REPORT/rules.json"
fi
if [ -f "/mnt/data/rules_safe.json" ]; then
  cp -f "/mnt/data/rules_safe.json" "$REPORT/rules_safe.json"
fi

echo "Rules deployed to $REPORT"
ls -l "$REPORT"
