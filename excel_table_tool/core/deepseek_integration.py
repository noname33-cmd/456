# core/deepseek_integration.py

import requests
import pandas as pd

API_URL = "https://api.deepseek.com/v1/chat/completions"  # пример, замените на актуальный
API_KEY = "your-deepseek-api-key"  # Замените на ваш ключ

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

COMMON_KEYS = ["иин", "инн", "uid", "id", "уид"]

def describe_dataframe(df: pd.DataFrame, max_rows=3) -> str:
    """
    Генерирует краткое текстовое описание структуры таблицы.
    """
    info = f"Таблица содержит {df.shape[0]} строк и {df.shape[1]} столбцов.\n"
    sample = df.head(max_rows).fillna("").astype(str)
    for col in df.columns:
        values = ", ".join(sample[col].unique()[:3])
        info += f"Столбец '{col}': Примеры значений — {values}\n"
    return info

def ask_deepseek(prompt: str) -> dict:
    """
    Отправляет запрос в DeepSeek и ожидает структурированный JSON-ответ.
    Пример: {"action": "filter", "column": "статус", "value": "Закрыт"}
    """
    messages = [
        {"role": "system", "content": "Ты помощник для анализа Excel-файлов. Всегда отвечай в JSON-формате."},
        {"role": "user", "content": prompt}
    ]

    payload = {
        "model": "deepseek-chat",  # Уточни модель, если другая
        "messages": messages,
        "temperature": 0.2
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        answer = response.json()["choices"][0]["message"]["content"]
        return eval(answer) if answer.strip().startswith("{") else {"error": "Неверный формат ответа"}
    except Exception as e:
        return {"error": f"Ошибка DeepSeek API: {e}"}
