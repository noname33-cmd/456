# -*- coding: utf-8 -*-
"""
Управление nginx upstream файлами: комментируем/раскомментируем server; и reload.
Требует: один upstream в upstream_conf и "server NAME:PORT;" строками.
Пример upstream_conf:
  upstream api_backend {
      server 10.0.0.11:8080;
      server 10.0.0.12:8080;
      # server 10.0.0.13:8080;  <-- disabled
  }
"""
import re, subprocess
from pathlib import Path
from typing import Tuple

def _reload(cmd: str):
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        raise RuntimeError("nginx reload failed")

def _read(conf_path: str) -> str:
    return Path(conf_path).read_text(encoding="utf-8")

def _write(conf_path: str, content: str):
    Path(conf_path).write_text(content, encoding="utf-8")

def count_enabled(conf) -> Tuple[int,int]:
    text = _read(conf["upstream_conf"])
    # считаем строки "server X;" которые НЕ закомментированы
    enabled = len(re.findall(r'^\s*server\s+[^;]+;\s*$', text, flags=re.MULTILINE))
    total   = enabled + len(re.findall(r'^\s*#\s*server\s+[^;]+;\s*$', text, flags=re.MULTILINE))
    return (enabled, total)

def set_state(conf, server: str, action: str):
    """
    server передаётся как '10.0.0.11:8080' (ровно как в конфиге).
    """
    path = conf["upstream_conf"]; reload_cmd = conf.get("reload_cmd","nginx -s reload")
    text = _read(path)
    # нормализуем пробелы
    srv_pattern = re.escape(server)
    enabled_line = re.compile(rf'(^\s*)server\s+{srv_pattern};\s*$', re.MULTILINE)
    disabled_line= re.compile(rf'(^\s*)#\s*server\s+{srv_pattern};\s*$', re.MULTILINE)
    changed = False

    if action == "enable":
        # раскомментировать, если закомментирован
        if disabled_line.search(text):
            text = disabled_line.sub(r"\1server " + server + ";", text, count=1)
            changed = True
    elif action in ("disable","drain"):
        # закомментировать, если включён
        if enabled_line.search(text):
            text = enabled_line.sub(r"\1# server " + server + ";", text, count=1)
            changed = True
    else:
        raise ValueError("unknown action")

    if changed:
        _write(path, text)
        _reload(reload_cmd)
