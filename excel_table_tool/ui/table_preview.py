import ttkbootstrap as tb
from ttkbootstrap.tableview import Tableview
from tkinter import ttk

def show_table_preview(df, frame):
    """Показать DataFrame в Treeview внутри указанного фрейма с возможностью редактирования."""
    for widget in frame.winfo_children():
        widget.destroy()

    if df.empty:
        return

    tree = ttk.Treeview(frame, show="headings", height=25)
    tree.pack(fill="both", expand=True)
    enable_cell_editing(tree, df)

    tree["columns"] = list(df.columns)
    for col in df.columns:
        tree.heading(col, text=col)
        tree.column(col, width=150, anchor="w")

    for _, row in df.iterrows():
        tree.insert("", "end", values=list(row.astype(str)))

def show_table_with_highlight(df, container, highlight_cells=None):
    """Показать DataFrame в Tableview от ttkbootstrap с подсветкой ошибок (без редактирования)."""
    for widget in container.winfo_children():
        widget.destroy()

    if df.empty:
        return

    tv = Tableview(container, dataframe=df, showtoolbar=True, autofit=True)
    tv.pack(fill="both", expand=True)

    if highlight_cells:
        for row_idx, col_name in highlight_cells:
            try:
                tv.highlight(row_idx, col_name, background="#ffcdd2")
            except Exception:
                continue

def enable_cell_editing(tree, df):
    def on_double_click(event):
        item_id = tree.identify_row(event.y)
        column = tree.identify_column(event.x)
        if not item_id or not column:
            return

        col_index = int(column[1:]) - 1
        row_index = tree.index(item_id)
        cell_value = df.iloc[row_index, col_index]

        x, y, width, height = tree.bbox(item_id, column)

        entry = tb.Entry(tree)
        entry.place(x=x, y=y, width=width, height=height)
        entry.insert(0, cell_value)
        entry.focus()

        def save_edit(event):
            new_val = entry.get()
            entry.destroy()
            tree.set(item_id, column, new_val)
            df.iloc[row_index, col_index] = new_val

        entry.bind("<Return>", save_edit)
        entry.bind("<FocusOut>", save_edit)

    tree.bind("<Double-1>", on_double_click)
