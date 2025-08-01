# ui/table_preview.py
from ttkbootstrap.tableview import Tableview
from tkinter import ttk

def show_table_preview(df, frame):
    """Показать DataFrame в Treeview внутри указанного фрейма."""
    # Очистка фрейма от предыдущей таблицы
    for widget in frame.winfo_children():
        widget.destroy()

    if df.empty:
        return

    # Создание Treeview
    tree = ttk.Treeview(frame, show="headings", height=25)
    tree.pack(fill="both", expand=True)

    # Настройка столбцов
    tree["columns"] = list(df.columns)
    for col in df.columns:
        tree.heading(col, text=col)
        tree.column(col, width=150, anchor="w")

    # Вставка строк
    for _, row in df.iterrows():
        tree.insert("", "end", values=list(row.astype(str)))

def show_table_preview(df, container, highlight_cells=None):
    """Показать DataFrame в Tableview от ttkbootstrap с подсветкой ошибок."""

    for widget in container.winfo_children():
        widget.destroy()

    if df.empty:
        return

    tv = Tableview(container, dataframe=df, showtoolbar=True, autofit=True)
    tv.pack(fill="both", expand=True)

    # Подсветка ячеек (если переданы)
    if highlight_cells:
        for row_idx, col_name in highlight_cells:
            try:
                tv.highlight(row_idx, col_name, background="#ffcdd2")
            except Exception:
                continue
