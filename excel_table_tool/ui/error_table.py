import tkinter as tk
import pandas as pd
from ttkbootstrap import Frame, Label, Entry, Button, Scrollbar, Style, Checkbutton, BooleanVar, constants as C
from tkinter import Toplevel, messagebox


def show_error_table(errors: list, df: pd.DataFrame, on_corrected_callback=None):
    if not errors:
        messagebox.showinfo("Проверка", "Ошибок не найдено.")
        return

    win = Toplevel()
    win.title("Ошибки данных")
    win.geometry("900x500")

    container = Frame(win)
    container.pack(fill="both", expand=True)

    canvas = tk.Canvas(container)
    scrollbar = Scrollbar(container, orient="vertical", command=canvas.yview)
    scroll_frame = Frame(canvas)

    scroll_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )
    canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    # Заголовки
    Label(scroll_frame, text="Строка", width=8, font=("Arial", 10, "bold")).grid(row=0, column=0)
    Label(scroll_frame, text="Колонка", width=20, font=("Arial", 10, "bold")).grid(row=0, column=1)
    Label(scroll_frame, text="Ошибка", width=30, font=("Arial", 10, "bold")).grid(row=0, column=2)
    Label(scroll_frame, text="Значение", width=20, font=("Arial", 10, "bold")).grid(row=0, column=3)
    Label(scroll_frame, text="Новое значение", width=20, font=("Arial", 10, "bold")).grid(row=0, column=4)
    Label(scroll_frame, text="Заменить всё", width=12, font=("Arial", 10, "bold")).grid(row=0, column=5)

    entries = []

    for i, err in enumerate(errors, start=1):
        row_idx = err["row"]
        col_name = err["column"]
        old_value = df.at[row_idx, col_name]

        Label(scroll_frame, text=row_idx, width=8).grid(row=i, column=0)
        Label(scroll_frame, text=col_name, width=20).grid(row=i, column=1)
        Label(scroll_frame, text=err["error"], width=30).grid(row=i, column=2)
        Label(scroll_frame, text=str(old_value), width=20).grid(row=i, column=3)

        new_entry = Entry(scroll_frame, width=20)
        new_entry.grid(row=i, column=4)

        apply_all_var = BooleanVar()
        Checkbutton(scroll_frame, variable=apply_all_var).grid(row=i, column=5)

        entries.append((row_idx, col_name, old_value, new_entry, apply_all_var))

    def apply_corrections():
        changed = 0
        for row, col, old_val, entry, apply_all in entries:
            new_val = entry.get().strip()
            if new_val and new_val != str(old_val):
                if apply_all.get():
                    df[col] = df[col].replace(old_val, new_val)
                    changed += df[col].tolist().count(new_val)
                else:
                    df.at[row, col] = new_val
                    changed += 1

        messagebox.showinfo("Обновление", f"Изменено {changed} значений.")
        if on_corrected_callback:
            on_corrected_callback()
        win.destroy()

    Button(win, text="Применить исправления", command=apply_corrections, bootstyle="success").pack(pady=10)


def apply_correction_to_all(df, column, old_value, new_value):
    df[column] = df[column].replace(old_value, new_value)
    return df
