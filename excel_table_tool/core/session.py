# core/session.py

import json
import os
from datetime import datetime

SESSION_FILE = "last_session.json"
LOG_FILE = "log.txt"

def save_session_info(file_paths):
    """Сохраняет пути загруженных файлов и время"""
    session_data = {
        "timestamp": datetime.now().isoformat(),
        "files": list(file_paths)
    }
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=4, ensure_ascii=False)
        log(f"Сессия сохранена. Файлы: {len(file_paths)}")
    except Exception as e:
        print(f"Ошибка сохранения сессии: {e}")

def load_last_session():
    """Загружает последнюю сессию"""
    if not os.path.exists(SESSION_FILE):
        return None
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки сессии: {e}")
        return None

def log(message):
    """Пишет строку в лог-файл"""
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{now}] {message}\n")
    except Exception as e:
        print(f"Ошибка логирования: {e}")
