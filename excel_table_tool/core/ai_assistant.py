# ui/ai_assistant.py
import tkinter as tk
from tkinter import simpledialog, messagebox
from core import ai_engine

def run_ai_assistant(df_main, df_reference=None):
    root = tk.Tk()
    root.withdraw()  # скрыть главное окно

    instruction = simpledialog.askstring("[*] AI-помощник", "Что вы хотите сделать с таблицей?")
    if not instruction:
        return df_main  # если отмена

    try:
        prompt = ai_engine.build_prompt(df_main, instruction)
        action = ai_engine.call_deepseek(prompt)
        df_result = ai_engine.apply_ai_action(df_main, df_reference, action)
        messagebox.showinfo("[+] Готово", f"АИ применил действие: {action['action']}")
        return df_result
    except Exception as e:
        messagebox.showerror("[-] Ошибка", str(e))
        return df_main
