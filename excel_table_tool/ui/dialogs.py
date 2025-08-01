import tkinter as tk
from tkinter import simpledialog, messagebox, ttk
import ttkbootstrap as tb
from tkinter import filedialog

def ask_string(title, prompt):
    return simpledialog.askstring(title, prompt)

def advanced_filter_dialog(columns):
    result = {}

    def on_submit():
        result["column"] = column_var.get()
        result["operator"] = op_var.get()
        result["value"] = value_var.get()
        dialog.destroy()

    dialog = tk.Toplevel()
    dialog.title("Расширенный фильтр")
    dialog.geometry("300x180")
    dialog.grab_set()

    tk.Label(dialog, text="Столбец:").pack(pady=5)
    column_var = ttk.Combobox(dialog, values=columns)
    column_var.pack()

    tk.Label(dialog, text="Оператор:").pack(pady=5)
    op_var = ttk.Combobox(dialog, values=["==", "!=", "contains", ">", "<", ">=", "<="])
    op_var.pack()

    tk.Label(dialog, text="Значение:").pack(pady=5)
    value_var = tk.Entry(dialog)
    value_var.pack()

    tk.Button(dialog, text="Применить", command=on_submit).pack(pady=10)

    dialog.wait_window()
    return result if result else None

def ask_column_dialog(columns, title="Выбор столбца"):
    root = tk.Toplevel()
    root.withdraw()
    choice = simpledialog.askstring(title, f"Укажите название столбца из списка:\n{', '.join(columns)}")
    root.destroy()
    if choice and choice in columns:
        return choice
    else:
        messagebox.showwarning("Ошибка", "Некорректный столбец.")
        return None

def ask_two_columns_dialog(columns, title="Выбор двух столбцов"):
    root = tk.Toplevel()
    root.withdraw()
    x = simpledialog.askstring(title, f"X-ось (из: {', '.join(columns)}):")
    y = simpledialog.askstring(title, f"Y-ось (из: {', '.join(columns)}):")
    root.destroy()
    if x in columns and y in columns:
        return x, y
    else:
        messagebox.showwarning("Ошибка", "Некорректные столбцы.")
        return None, None

def select_template_dialog(template_names):
    result = {}

    def on_submit():
        result["selected"] = var.get()
        dialog.destroy()

    dialog = tk.Toplevel()
    dialog.title("Выбор шаблона")
    dialog.geometry("300x150")
    dialog.grab_set()

    tk.Label(dialog, text="Выберите шаблон:").pack(pady=10)
    var = ttk.Combobox(dialog, values=template_names)
    var.pack(pady=5)

    tk.Button(dialog, text="Применить", command=on_submit).pack(pady=10)
    dialog.wait_window()
    return result.get("selected")

def select_template_dialog(templates):
    dialog = tk.Toplevel()
    dialog.title("Выберите шаблон")
    dialog.geometry("300x300")
    dialog.resizable(False, False)

    var = tk.StringVar(value=templates[0])

    listbox = tk.Listbox(dialog, listvariable=tk.StringVar(value=templates), height=10)
    listbox.pack(fill="both", expand=True, padx=10, pady=10)

    result = []

    def on_ok():
        selection = listbox.curselection()
        if selection:
            result.append(templates[selection[0]])
        dialog.destroy()

    def on_cancel():
        dialog.destroy()

    btn_frame = tk.Frame(dialog)
    btn_frame.pack(pady=5)
    tk.Button(btn_frame, text="OK", command=on_ok).pack(side="left", padx=5)
    tk.Button(btn_frame, text="Отмена", command=on_cancel).pack(side="left", padx=5)

    dialog.transient()
    dialog.grab_set()
    dialog.wait_window()

    return result[0] if result else None


def ask_column_dialog(columns, title="Выбор столбца"):
    dialog = tk.Toplevel()
    dialog.title(title)
    dialog.geometry("300x200")

    var = tk.StringVar(value=columns[0])

    tk.Label(dialog, text="Выберите столбец:").pack(pady=10)
    combo = tb.Combobox(dialog, values=columns, textvariable=var, state="readonly")
    combo.pack(padx=10)

    result = []

    def on_ok():
        result.append(var.get())
        dialog.destroy()

    tk.Button(dialog, text="OK", command=on_ok).pack(pady=10)
    dialog.transient()
    dialog.grab_set()
    dialog.wait_window()
    return result[0] if result else None


def ask_two_columns_dialog(columns, title="Выбор столбцов"):
    dialog = tk.Toplevel()
    dialog.title(title)
    dialog.geometry("350x200")

    var1 = tk.StringVar(value=columns[0])
    var2 = tk.StringVar(value=columns[1] if len(columns) > 1 else columns[0])

    tk.Label(dialog, text="X-ось:").pack()
    combo1 = tb.Combobox(dialog, values=columns, textvariable=var1, state="readonly")
    combo1.pack()

    tk.Label(dialog, text="Y-ось:").pack()
    combo2 = tb.Combobox(dialog, values=columns, textvariable=var2, state="readonly")
    combo2.pack()

    result = []

    def on_ok():
        result.extend([var1.get(), var2.get()])
        dialog.destroy()

    tk.Button(dialog, text="OK", command=on_ok).pack(pady=10)
    dialog.transient()
    dialog.grab_set()
    dialog.wait_window()
    return result if result else (None, None)


def ask_string(title, prompt):
    return simpledialog.askstring(title, prompt)


def advanced_filter_dialog(columns):
    dialog = tk.Toplevel()
    dialog.title("Расширенный фильтр")
    dialog.geometry("300x250")

    var_column = tk.StringVar(value=columns[0])
    var_operator = tk.StringVar(value="==")
    var_value = tk.StringVar()

    ops = ["==", "!=", ">", ">=", "<", "<=", "contains", "startswith", "endswith"]

    tb.Label(dialog, text="Столбец:").pack()
    tb.Combobox(dialog, values=columns, textvariable=var_column, state="readonly").pack()

    tb.Label(dialog, text="Оператор:").pack()
    tb.Combobox(dialog, values=ops, textvariable=var_operator, state="readonly").pack()

    tb.Label(dialog, text="Значение:").pack()
    tb.Entry(dialog, textvariable=var_value).pack()

    result = {}

    def on_ok():
        result["column"] = var_column.get()
        result["operator"] = var_operator.get()
        result["value"] = var_value.get()
        dialog.destroy()

    tb.Button(dialog, text="OK", command=on_ok).pack(pady=10)

    dialog.transient()
    dialog.grab_set()
    dialog.wait_window()
    return result if result else None


def template_manager_dialog(templates, actions):
    dialog = tk.Toplevel()
    dialog.title("Управление шаблонами")
    dialog.geometry("400x400")

    lb = tk.Listbox(dialog)
    lb.pack(fill="both", expand=True, padx=10, pady=10)
    for t in templates:
        lb.insert("end", t)

    def refresh():
        lb.delete(0, "end")
        for t in actions["list"]():
            lb.insert("end", t)

    def get_selected():
        idx = lb.curselection()
        return lb.get(idx) if idx else None

    def do_rename():
        t = get_selected()
        if not t: return
        new = ask_string("Переименование", f"Новое имя для {t}:")
        if new: actions["rename"](t, new); refresh()

    def do_delete():
        t = get_selected()
        if not t: return
        if messagebox.askyesno("Удалить", f"Удалить шаблон {t}?"):
            actions["delete"](t)
            refresh()

    def do_duplicate():
        t = get_selected()
        if not t: return
        new = ask_string("Дублировать", f"Имя нового шаблона:")
        if new: actions["duplicate"](t, new); refresh()

    btn_frame = tk.Frame(dialog)
    btn_frame.pack(pady=5)
    tk.Button(btn_frame, text="Переименовать", command=do_rename).pack(side="left", padx=5)
    tk.Button(btn_frame, text="Удалить", command=do_delete).pack(side="left", padx=5)
    tk.Button(btn_frame, text="Дублировать", command=do_duplicate).pack(side="left", padx=5)

    dialog.transient()
    dialog.grab_set()
    dialog.wait_window()

def ask_columns_multi_dialog(columns, title="Выберите столбцы"):
    import tkinter as tk
    from tkinter import simpledialog

    selected = []

    def on_ok():
        nonlocal selected
        selected = [columns[i] for i in listbox.curselection()]
        top.destroy()

    top = tk.Toplevel()
    top.title(title)
    listbox = tk.Listbox(top, selectmode="multiple", height=15, width=50)
    for col in columns:
        listbox.insert(tk.END, col)
    listbox.pack(padx=10, pady=10)

    btn = tb.Button(top, text="OK", command=on_ok)
    btn.pack(pady=(0, 10))

    top.grab_set()
    top.wait_window()
    return selected

def select_merge_columns_dialog(cols1, cols2):
    selected = {"left": None, "right": None}
    root = tk.Toplevel()
    root.title("Выбор ключей объединения")
    root.geometry("400x200")
    root.grab_set()
    root.resizable(False, False)

    tk.Label(root, text="Столбец из первой таблицы (левая):").pack(pady=(10, 0))
    combo1 = ttk.Combobox(root, values=cols1, state="readonly")
    combo1.pack(pady=5)

    tk.Label(root, text="Столбец из второй таблицы (правая):").pack(pady=(10, 0))
    combo2 = ttk.Combobox(root, values=cols2, state="readonly")
    combo2.pack(pady=5)

    def confirm():
        selected["left"] = combo1.get()
        selected["right"] = combo2.get()
        root.destroy()

    tk.Button(root, text="Объединить", command=confirm).pack(pady=10)
    root.wait_window()

    return selected["left"], selected["right"]

def ask_file_paths():
    return filedialog.askopenfilenames(title="Выберите файлы")