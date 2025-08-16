#!/usr/bin/env bash
set -euo pipefail

# rotate_secrets.sh
# Массовая ротация секретов/токенов в файлах (ENV, systemd, YAML, JSON и др.)
# Примеры:
#   ./rotate_secrets.sh --dir /opt --toggle-secret NEWSECRET
#   ./rotate_secrets.sh --dir . --toggle-secret "s3cr3t" --set API_TOKEN=abc123 --set JWT_SECRET=xyz
#   ./rotate_secrets.sh --dir . --dry-run --toggle-secret "preview"

RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; CYAN=$'\e[36m'; NC=$'\e[0m'

ROOT="."
DRYRUN=0
declare -A KV
KV["TOGGLE_SECRET"]=""   # по умолчанию ключ присутствует, значение зададим из --toggle-secret
FILECOUNT=0
CHANGECOUNT=0

print_help() {
  cat <<EOF
Usage: $0 --dir <PATH> --toggle-secret <VALUE> [--set KEY=VALUE ...] [--dry-run]

Options:
  --dir <PATH>              Корень поиска (по умолчанию .)
  --toggle-secret <VALUE>   Новое значение для TOGGLE_SECRET (обязателен либо этот флаг, либо хотя бы один --set)
  --set KEY=VALUE           Доп. пары ключ=значение (можно указывать несколько раз), напр.: --set API_TOKEN=abc --set JWT_SECRET=xyz
  --dry-run                 Только показать изменения, файлы не трогать
  -h|--help                 Показать справку

Исключаются из поиска: .git, node_modules, venv, .venv, dist, build, __pycache__
Форматы замен:
  - .env / shell:           KEY=VALUE, export KEY=VALUE
  - systemd .service:       Environment=KEY=VALUE (и вариации)
  - YAML:                   KEY: VALUE (учитывает кавычки)
  - JSON:                   "KEY": "VALUE"
  - ini/conf:               KEY = VALUE
EOF
}

# --- разбор аргументов ---
if [[ $# -eq 0 ]]; then print_help; exit 1; fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) ROOT="${2:-}"; shift 2;;
    --toggle-secret)
      KV["TOGGLE_SECRET"]="${2:-}"; shift 2;;
    --set)
      pair="${2:-}"; shift 2
      if [[ "$pair" != *"="* ]]; then
        echo "${RED}[ERR]${NC} --set ожидает формат KEY=VALUE, получено: '$pair'" >&2; exit 2
      fi
      k="${pair%%=*}"; v="${pair#*=}"
      if [[ -z "$k" ]]; then echo "${RED}[ERR]${NC} пустой KEY в --set" >&2; exit 2; fi
      KV["$k"]="$v"
      ;;
    --dry-run) DRYRUN=1; shift;;
    -h|--help) print_help; exit 0;;
    *) echo "${YELLOW}[WARN]${NC} неизвестный аргумент: $1" >&2; shift;;
  esac
done

# Проверка, что есть что менять
nonempty=0
for k in "${!KV[@]}"; do
  if [[ -n "${KV[$k]}" ]]; then nonempty=1; fi
done
if [[ $nonempty -eq 0 ]]; then
  echo "${RED}[ERR]${NC} не задано ни одного значения (нужен --toggle-secret или хотя бы один --set KEY=VALUE)"; exit 3
fi

# Проверка каталога
if [[ ! -d "$ROOT" ]]; then
  echo "${RED}[ERR]${NC} каталога '${ROOT}' не существует"; exit 4
fi

# Определяем sed (gsed на macOS, иначе sed с -i.bak)
SED="sed"
if sed --version >/dev/null 2>&1; then
  : # GNU sed
else
  if command -v gsed >/dev/null 2>&1; then
    SED="gsed"
  fi
fi

# Экранирование для sed
esc_sed_repl() {
  # экранируем \ & / для безопасной подстановки в sed
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

# Список файлов (текстовых), которые трогаем
mapfile -t FILES < <(
  find "$ROOT" -type f \
    -not -path "*/.git/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/dist/*" \
    -not -path "*/build/*" \
    -not -path "*/__pycache__/*" \
    -not -path "*/venv/*" \
    -not -path "*/.venv/*" \
    \( -iname "*.env" -o -iname "*.service" -o -iname "*.yaml" -o -iname "*.yml" \
       -o -iname "*.json" -o -iname "*.ini" -o -iname "*.conf" -o -iname "*.cfg" \
       -o -iname "*.sh" -o -iname "Dockerfile" -o -iname "*.py" -o -iname "*.js" -o -iname "*.ts" -o -iname "*.tsx" \
       -o -iname "*.properties" \)
)

# Функция замены ключа в одном файле разными паттернами
replace_key_in_file() {
  local file="$1"
  local key="$2"
  local val="$3"
  local val_esc; val_esc="$(esc_sed_repl "$val")"

  # подсчёт изменений
  local before after diffc
  before=$(wc -c < "$file" || echo 0)

  # 1) .env / shell: KEY=..., export KEY=...
  $SED -i.bak -E \
    -e "s#^(export[[:space:]]+)?(${key})[[:space:]]*=[[:space:]]*.*#\2=${val_esc}#g" \
    "$file" || true

  # 2) systemd: Environment=KEY=VALUE (в том числе несколько через пробел)
  # заменяем KEY=... внутри Environment=... строки
  $SED -i.bak -E \
    -e "s#^(Environment=.*\b${key}=)[^[:space:]\"']+#\1${val_esc}#g" \
    -e "s#^(Environment=\")[^\"]*\b(${key}=)[^\"']+#\1#; s#^(Environment=\")#\1#g" \
    "$file" || true
  # (примечание: второй -e на случай кавычечной формы Environment="K1=V1 K2=V2")

  # 3) YAML: KEY: VALUE (с кавычками и без)
  $SED -i.bak -E \
    -e "s#^([[:space:]]*${key}[[:space:]]*:[[:space:]]*).*\$#\1${val_esc}#g" \
    -e "s#^([[:space:]]*${key}[[:space:]]*:[[:space:]]*)\"[^\"]*\"#\1\"${val_esc}\"#g" \
    -e "s#^([[:space:]]*${key}[[:space:]]*:[[:space:]]*)'[^']*'#\1'${val_esc}'#g" \
    "$file" || true

  # 4) JSON: "KEY": "VALUE" или 'VALUE'
  $SED -i.bak -E \
    -e "s#(\"${key}\"[[:space:]]*:[[:space:]]*)\"[^\"]*\"#\1\"${val_esc}\"#g" \
    -e "s#(\"${key}\"[[:space:]]*:[[:space:]]*)'[^']*'#\1'${val_esc}'#g" \
    "$file" || true

  # 5) ini/conf: KEY = VALUE
  $SED -i.bak -E \
    -e "s#^([[:space:]]*${key}[[:space:]]*=[[:space:]]*).*\$#\1${val_esc}#g" \
    "$file" || true

  # удаляем .bak, если не было изменений
  after=$(wc -c < "$file" || echo 0)
  diffc=0
  if ! cmp -s "$file" "$file.bak"; then
    diffc=1
  fi
  rm -f "$file.bak" 2>/dev/null || true
  echo "$diffc"
}

echo "${CYAN}>>> Старт ротации секретов в: ${ROOT}${NC}"
[[ $DRYRUN -eq 1 ]] && echo "${YELLOW}[DRY-RUN]${NC} изменения НЕ записываются (но будут показаны)."

for f in "${FILES[@]}"; do
  FILECOUNT=$((FILECOUNT+1))
  file_changed=0
  changes_for_file=0
  declare -A orig
  # сохраним для показа diff-like
  if [[ $DRYRUN -eq 1 ]]; then
    orig["content"]="$(cat "$f")"
  fi

  for k in "${!KV[@]}"; do
    v="${KV[$k]}"
    [[ -z "$v" ]] && continue
    if [[ $DRYRUN -eq 1 ]]; then
      # делаем замену в отображении, в файл не пишем
      tmp="$(mktemp)"
      cp "$f" "$tmp"
      changed="$(replace_key_in_file "$tmp" "$k" "$v")"
      if [[ "$changed" == "1" ]]; then
        file_changed=1; changes_for_file=$((changes_for_file+1))
      fi
      rm -f "$tmp"
    else
      changed="$(replace_key_in_file "$f" "$k" "$v")"
      if [[ "$changed" == "1" ]]; then
        file_changed=1; changes_for_file=$((changes_for_file+1))
      fi
    fi
  done

  if [[ $file_changed -eq 1 ]]; then
    CHANGECOUNT=$((CHANGECOUNT+1))
    echo "${GREEN}[OK]${NC} обновлён: $f (${changes_for_file} ключ(ей))"
    # опционально можно показать строки с ключами
    for k in "${!KV[@]}"; do
      v="${KV[$k]}"; [[ -z "$v" ]] && continue
      grep -Hn -E "(${k}[[:space:]]*=|\"${k}\"[[:space:]]*:|[[:space:]]${k}[[:space:]]*:)" "$f" || true
    done
  fi
done

echo "${CYAN}>>> Готово.${NC} Просмотрено файлов: ${FILECOUNT}. Изменено файлов: ${CHANGECOUNT}."

if [[ $CHANGECOUNT -eq 0 ]]; then
  echo "${YELLOW}Ничего не изменено. Проверьте ключи/маски и наличие файлов.${NC}"
fi
