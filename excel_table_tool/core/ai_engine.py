# core/ai_engine.py
import pandas as pd
import json
import requests

API_URL = "https://api.deepseek.com/v1/chat/completions"
API_KEY = "your_deepseek_api_key_here"
MODEL = "deepseek-chat"

def describe_dataframe(df: pd.DataFrame) -> str:
    description = f"Таблица содержит {len(df)} строк и {len(df.columns)} столбцов.\n"
    description += "Столбцы:\n"
    for col in df.columns:
        sample_values = df[col].dropna().astype(str).unique()[:5]
        sample_text = ", ".join(sample_values)
        description += f" - {col} (примеры: {sample_text})\n"
    return description

def build_prompt(df: pd.DataFrame, user_instruction: str) -> str:
    return (
        "Ты — интеллектуальный помощник, работающий с таблицами. "
        "Твоя задача — понять, что хочет пользователь, и вернуть действие в виде JSON.\n"
        "Вот описание таблицы:\n"
        f"{describe_dataframe(df)}\n"
        f"Запрос пользователя:\n{user_instruction}\n\n"
        "Ответ должен быть в формате JSON со структурой:\n"
        "{ 'action': 'тип_действия', 'params': { ... } }\n"
        "Поддерживаемые типы действий: merge, filter, drop_columns, add_column\n"
    )

def call_deepseek(prompt: str) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "Ты помощник по работе с данными."},
            {"role": "user", "content": prompt}
        ]
    }
    response = requests.post(API_URL, headers=headers, json=body)
    response.raise_for_status()
    result = response.json()
    content = result["choices"][0]["message"]["content"]
    try:
        return json.loads(content)
    except Exception as e:
        raise ValueError(f"Ошибка парсинга ответа: {e}\nОтвет: {content}")

def apply_ai_action(df: pd.DataFrame, reference_df: pd.DataFrame, action: dict) -> pd.DataFrame:
    act = action.get("action")
    params = action.get("params", {})
    if act == "merge":
        return df.merge(reference_df, how="left", left_on=params["on"], right_on=params["on"])
    elif act == "filter":
        col, val = params["column"], params["value"]
        return df[df[col] == val]
    elif act == "drop_columns":
        return df.drop(columns=params["columns"], errors="ignore")
    elif act == "add_column":
        return df.assign(**{params["name"]: params["value"]})
    else:
        raise ValueError(f"Неизвестное действие: {act}")
