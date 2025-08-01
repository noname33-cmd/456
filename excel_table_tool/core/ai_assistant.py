# ui/ai_assistant.py
import tkinter as tk
from tkinter import simpledialog, messagebox
from core import ai_engine
from core.ai_command_router import execute_command
from ui import dialogs


def run_ai_assistant(df, update_ui_callback):
    query = dialogs.ask_string("AI", "Что вы хотите сделать?")
    if not query:
        return df
    return execute_command(query, df, update_ui_callback)
