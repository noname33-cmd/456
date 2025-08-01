# core/user_interaction.py

import pandas as pd
from tkinter import messagebox, simpledialog
from ui.dialogs import select_merge_columns_dialog

# Предопределённые ключи — но пользователь может задать вручную
COMMON_KEYS = ["иин", "инн", "id", "uid", "уид"]

def log_chat(log_func, text):
    """Лог в виде общения"""
    log_func(f"🤖: {text}")

def ask_user_yes_no(prompt: str, title="Excel Super Tool") -> bool:
    return messagebox.askyesno(title=title, message=prompt)

def ask_user_input(prompt: str, title="Введите значение") -> str:
    return simpledialog.askstring(title, prompt)

def detect_common_keys(df1: pd.DataFrame, df2: pd.DataFrame):
    keys1 = set(df1.columns.str.lower())
    keys2 = set(df2.columns.str.lower())
    return list(keys1.intersection(keys2).intersection(COMMON_KEYS))

def merge_suggestion(df_list, log_func):
    if len(df_list) < 2:
        return df_list[0] if df_list else pd.DataFrame()

    df1, df2 = df_list[0], df_list[1]
    log_chat(log_func, "Обнаружено 2 таблицы. Анализирую...")

    detected_keys = detect_common_keys(df1, df2)
    if detected_keys:
        key = detected_keys[0]
        log_chat(log_func, f"Обнаружен общий столбец: '{key}'. Хотите объединить по нему?")
        if ask_user_yes_no(f"Объединить таблицы по ключу: '{key}'?"):
            result = pd.merge(df1, df2, on=key, how="left")
            log_chat(log_func, f"✅ Таблицы объединены по '{key}'")
            return result
    else:
        log_chat(log_func, "Не удалось автоматически найти общий ключ.")
        if ask_user_yes_no("Указать ключи объединения вручную?"):
            key1, key2 = select_merge_columns_dialog(df1.columns.tolist(), df2.columns.tolist())
            if key1 and key2:
                log_chat(log_func, f"Пользователь указал ключи: {key1} и {key2}. Объединяю...")
                result = pd.merge(df1, df2, left_on=key1, right_on=key2, how="left")
                log_chat(log_func, f"✅ Объединение выполнено по '{key1}' и '{key2}'")
                return result
            else:
                log_chat(log_func, "❌ Ключи не выбраны. Объединение отменено.")
        else:
            log_chat(log_func, "Объединение пропущено.")

    return df1
