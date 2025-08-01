# ui/main_window.py
import threading
import time
import os
import json
import pandas as pd
import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox
from tkinter import Menu
from core.analytics import plot_histogram, plot_pie, plot_line
from core import template_manager, ai_assistant
from core import file_loader, exporter, analytics, session
from ui.table_preview import show_table_preview
from ui import dialogs
from core import intelligent_processor
from core.user_interaction import merge_suggestion
from ui import error_table
import core.data_processor as data_processor
from core import validator


HISTORY_FILE = "history.json"
df_global = pd.DataFrame()
df_undo_stack = []
df_redo_stack = []
watching = False
watch_thread = None
WATCH_FOLDER = "watch"
TEMPLATE_TO_APPLY = "default_template"
os.makedirs(WATCH_FOLDER, exist_ok=True)

def df_to_json(df): return df.to_json(orient="split")
def df_from_json(js): return pd.read_json(js, orient="split")

def save_history():
    data = {
        "undo": [df_to_json(df) for df in df_undo_stack],
        "redo": [df_to_json(df) for df in df_redo_stack]
    }
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)

def load_history():
    global df_undo_stack, df_redo_stack
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                df_undo_stack = [df_from_json(d) for d in data.get("undo", [])]
                df_redo_stack = [df_from_json(d) for d in data.get("redo", [])]
            except Exception as e:
                print("Ошибка загрузки истории:", e)


def launch_gui():
    global df_global, df_undo_stack, df_redo_stack, watching, watch_thread

    current_theme = ["darkly"]
    app = tb.Window(title="Excel Super Tool", themename=current_theme[0])
    app.geometry("1200x800")

    def update_status(text): status_label.config(text=text)
    def log(msg): log_box.insert(tb.END, f"{msg}\n"); log_box.yview_moveto(1)
    def save_state(): df_undo_stack.append(df_global.copy()); df_redo_stack.clear(); save_history()

    def undo():
        global df_global
        if df_undo_stack:
            df_redo_stack.append(df_global.copy())
            df_global = df_undo_stack.pop()
            show_table_preview(df_global, table_frame)
            update_status("Откат выполнен")
            log("↩️ Undo")
            save_history()
        else: update_status("Нет предыдущего состояния")

    def redo():
        global df_global
        if df_redo_stack:
            df_undo_stack.append(df_global.copy())
            df_global = df_redo_stack.pop()
            show_table_preview(df_global, table_frame)
            update_status("Повтор выполнен")
            log("↪️ Redo")
            save_history()
        else: update_status("Нет состояния для повтора")

    def open_template_manager():
        dialogs.template_manager_dialog(
            template_manager.list_templates(),
            actions={
                "list": template_manager.list_templates,
                "rename": template_manager.rename_template,
                "delete": template_manager.delete_template,
                "duplicate": template_manager.duplicate_template,
            })

    def merge_by_key():
        global df_global
        if df_global.empty:
            messagebox.showwarning("Ошибка", "Загрузите основной файл.")
            return

        path = filedialog.askopenfilename(title="Выберите файл со справочником", filetypes=[("Excel", "*.xlsx *.xls"), ("CSV", "*.csv"), ("JSON", "*.json")])
        if not path:
            return

        ref_df = file_loader.load_files([path])
        if ref_df.empty:
            messagebox.showwarning("Ошибка", "Не удалось загрузить справочник.")
            return

        # Спросим у пользователя ключи и нужные столбцы
        key = dialogs.ask_column_dialog(df_global.columns.tolist(), "Выберите общий ключ (например, ИИН)")
        ref_key = dialogs.ask_column_dialog(ref_df.columns.tolist(), "Выберите ключ в справочнике (например, ИИН)")
        columns_to_merge = dialogs.ask_columns_multi_dialog(ref_df.columns.tolist(), "Выберите столбцы для подстановки")

        if key and ref_key and columns_to_merge:
            save_state()
            merged = pd.merge(df_global, ref_df[[ref_key] + columns_to_merge], left_on=key, right_on=ref_key, how="left")
            df_global = merged
            show_table_preview(df_global, table_frame)
            update_status(f"Объединено по ключу: {key}")
            log(f"🔗 Объединение по '{key}' ← '{ref_key}'")
            save_history()

    def watch_folder_loop():
        global df_global
        seen_files = set()
        while watching:
            try:
                files = [f for f in os.listdir(WATCH_FOLDER) if f.endswith((".xlsx", ".xls", ".csv", ".json", ".txt"))]
                new_files = [f for f in files if f not in seen_files]
                for f in new_files:
                    full_path = os.path.join(WATCH_FOLDER, f)
                    df = file_loader.load_files([full_path])
                    if not df.empty:
                        save_state()
                        df_global = data_processor.apply_template(df, TEMPLATE_TO_APPLY)
                        show_table_preview(df_global, table_frame)
                        update_status(f"📥 Автообработка файла: {f}")
                        log(f"⚙️ Автообработка {f} через шаблон {TEMPLATE_TO_APPLY}")
                        save_history()
                    seen_files.add(f)
            except Exception as e:
                print("Ошибка при автообработке:", e)
            time.sleep(5)

    def toggle_autowatch():
        global watching, watch_thread
        watching = not watching
        if watching:
            watch_thread = threading.Thread(target=watch_folder_loop, daemon=True)
            watch_thread.start()
            log("🟢 Автообработка включена")
            update_status("Автообработка активна")
        else:
            log("🔴 Автообработка отключена")
            update_status("Автообработка остановлена")

    def load_files():
        global df_global
        paths = filedialog.askopenfilenames(filetypes=[("Файлы", "*.xlsx *.xls *.csv *.json *.txt")])
        if not paths: return

        df_list = file_loader.load_multiple_files(paths)
        if not df_list:
            messagebox.showwarning("Ошибка", "Не удалось загрузить данные.")
            return

        if len(df_list) >= 2:
            df = merge_suggestion(df_list)
        else:
            df = df_list[0]

        df_global = df
        df_undo_stack.clear()
        df_redo_stack.clear()
        save_state()
        show_table_preview(df_global, table_frame)
        session.save_session_info(paths)
        update_status(f"Загружено: {len(df_global)} строк")
        log("📁 Загружены файлы")

    def load_and_merge_smart():
        global df_global
        paths = filedialog.askopenfilenames(filetypes=[("Файлы", "*.xlsx *.xls *.csv *.json *.txt")])
        if not paths: return

        dfs = {}
        for p in paths:
            df = file_loader.load_files([p])
            if not df.empty:
                dfs[os.path.basename(p)] = df

        from core import multi_file_analyzer
        main_name, ref_files, key_candidates, err = multi_file_analyzer.analyze_files(dfs)

        if err:
            messagebox.showwarning("Ошибка", err)
            return

        if not key_candidates:
            messagebox.showinfo("Ключи не найдены", "Не удалось найти общий ключ между файлами.")
            return

    # Запрос у пользователя ключа
        from ui.dialogs import ask_column_dialog
        key = ask_column_dialog(key_candidates, f"Выберите ключ для объединения файлов")

        if not key:
            messagebox.showinfo("Отмена", "Объединение отменено.")
            return

        ref_pairs = [(name, dfs[name]) for name, _ in ref_files]
        result_df = multi_file_analyzer.join_with_references(dfs[main_name], ref_pairs, key)

        if not result_df.empty:
            save_state()
            df_global = result_df
            show_table_preview(df_global, table_frame)
            update_status("📎 Файлы объединены")
            log(f"🔗 Умное объединение по '{key}'")
            save_history()

    def export_to_pdf():
        if df_global.empty: return
        path = filedialog.asksaveasfilename(defaultextension=".pdf")
        if path:
            exporter.export_to_pdf(df_global, path)
            update_status("Экспорт PDF выполнен"); log("📄 Экспорт в PDF")

    def export_to_excel():
        if df_global.empty: return
        path = filedialog.asksaveasfilename(defaultextension=".xlsx")
        if path:
            exporter.export_to_excel(df_global, path)
            update_status("Экспорт Excel выполнен"); log("📤 Экспорт в Excel")

    def show_summary():
        if df_global.empty: return
        summary = analytics.get_summary(df_global)
        messagebox.showinfo("Сводка", summary)

    def filter_data():
        global df_global
        keyword = dialogs.ask_string("Фильтрация", "Введите ключевое слово:")
        if keyword:
            save_state()
            df_global = data_processor.filter_by_keyword(df_global, keyword)
            show_table_preview(df_global, table_frame)
            update_status(f"Фильтр: {keyword}"); log(f"🔍 Фильтрация: {keyword}"); save_history()

    def apply_advanced_filter():
        global df_global
        if df_global.empty: return
        res = dialogs.advanced_filter_dialog(df_global.columns.tolist())
        if res:
            save_state()
            df_global = data_processor.advanced_filter(df_global, res["column"], res["operator"], res["value"])
            show_table_preview(df_global, table_frame)
            update_status(f"Фильтр: {res}"); log(f"🧪 Расширенный фильтр: {res}"); save_history()

    def clean_data():
        global df_global
        if df_global.empty: return
        save_state()
        df_global = data_processor.clean_dataframe(df_global)
        show_table_preview(df_global, table_frame)
        update_status("Таблица очищена"); log("🧹 Очистка таблицы"); save_history()

    def save_current_as_template():
        if df_global.empty: return
        name = dialogs.ask_string("Шаблон", "Название шаблона:")
        if not name: return
        config = {
            "drop_columns": [col for col in df_global.columns if col.lower().startswith("unnamed")],
            "filters": [], "add_columns": [], "clean": True
        }
        template_manager.save_template(name, config)
        update_status(f"Шаблон '{name}' сохранён"); log(f"💾 Шаблон '{name}' сохранён")

    def apply_template_action():
        global df_global
        templates = template_manager.list_templates()
        if not templates:
            messagebox.showwarning("Нет шаблонов", "Сначала сохраните шаблон."); return
        selected = dialogs.select_template_dialog(templates)
        if selected:
            save_state()
            df_global = data_processor.apply_template(df_global, selected)
            show_table_preview(df_global, table_frame)
            update_status(f"Применён шаблон: {selected}"); log(f"📥 Применён шаблон: {selected}"); save_history()

    def add_column():
        global df_global
        name = dialogs.ask_string("Столбец", "Название:")
        val = dialogs.ask_string("Столбец", "Значение по умолчанию:")
        if name:
            save_state()
            df_global = data_processor.add_column(df_global, name, val)
            show_table_preview(df_global, table_frame)
            update_status(f"Добавлен столбец: {name}"); log(f"➕ Столбец '{name}'"); save_history()

    def switch_theme():
        themes = ["darkly", "flatly"]
        idx = themes.index(current_theme[0])
        new_theme = themes[(idx + 1) % len(themes)]
        current_theme[0] = new_theme
        app.style.theme_use(new_theme)
        update_status(f"Тема: {new_theme}")

    def show_histogram():
        if df_global.empty: return
        col = dialogs.ask_column_dialog(df_global.columns.tolist(), "Гистограмма")
        if col:
            plot_histogram(df_global, col); log(f"📊 Гистограмма по '{col}'")

    def show_pie_chart():
        if df_global.empty: return
        col = dialogs.ask_column_dialog(df_global.columns.tolist(), "Круговая диаграмма")
        if col:
            plot_pie(df_global, col); log(f"🥧 Круговая по '{col}'")

    #def show_line_chart():
    #    if df_global.empty: return
    #    x_col, y_col = dialogs.ask_two_columns_dialog(df_global.columns.tolist(), "Линейный график")
    #    if x_col and y_col:
    #        plot_line(df_global, x_col, y_col); log(f"📈 Линейный: {x_col} → {y_col}")

    def smart_merge_gui():
        global df_global
        if df_global.empty:
            messagebox.showwarning("Ошибка", "Сначала загрузите основной файл.")
            return
        path = filedialog.askopenfilename(title="Выберите файл-справочник")
        if not path: return
        save_state()
        df_global, msg = intelligent_processor.smart_load_and_merge(df_global, path)
        show_table_preview(df_global, table_frame)
        update_status(msg); log(f"🤖 {msg}"); save_history()

    def show_line_chart():
        if df_global.empty: return
        x_col, y_col = dialogs.ask_two_columns_dialog(df_global.columns.tolist(), "Линейный график")
        if x_col and y_col:
            plot_line(df_global, x_col, y_col)
            log(f"📈 Линейный: {x_col} → {y_col}")

    def ai_assistant_callback():
        global df_global
        new_df = ai_assistant.run_ai_assistant(df_global)
        if new_df is not None and not new_df.equals(df_global):
            save_state()
            df_global = new_df
            show_table_preview(df_global, table_frame)
            update_status("AI применил действие")
            log("🤖 AI действие выполнено")
            save_history()

    def validate_data():
        global df_global
        if df_global.empty:
            messagebox.showwarning("Проверка", "Сначала загрузите таблицу.")
            return
        errors = validator.validate_dataframe(df_global)
        if errors:
            error_table.show_error_table(errors, df_global, on_corrected_callback=lambda: show_table_preview(df_global, table_frame))
        else:
            messagebox.showinfo("Проверка", "Ошибок не найдено.")

    def check_errors_and_edit():
        global df_global

        errors, highlight_cells = validator.validate_dataframe(df_global)
        if not errors:
            messagebox.showinfo("Проверка", "Ошибок не найдено.")
            return

        def after_correct():
            show_table_preview(df_global, table_frame, highlight_cells=None)
            update_status("Ошибки исправлены")
            log("✅ Ошибки исправлены")

        error_table.show_error_table(errors, df_global, after_correct)

    def calculate_duration():
        global df_global
        if df_global.empty:
            messagebox.showwarning("Ошибка", "Сначала загрузите таблицу.")
            return

        start_col = dialogs.ask_column_dialog(df_global.columns.tolist(), "Выберите столбец начала времени")
        end_col = dialogs.ask_column_dialog(df_global.columns.tolist(), "Выберите столбец окончания времени")
        if not start_col or not end_col:
            return

        output_col = dialogs.ask_string("Имя нового столбца", "Введите имя нового столбца (по умолчанию 'Длительность (мин)'):")
        if not output_col:
            output_col = "Длительность (мин)"

        try:
            save_state()
            df_global = data_processor.calculate_duration_column(df_global, start_col, end_col, output_col)
            show_table_preview(df_global, table_frame)
            update_status(f"Добавлен столбец длительности: {output_col}")
            log(f"⏱ Расчёт длительности: {start_col} → {end_col} → {output_col}")
            save_history()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось рассчитать длительность: {e}")

    # === Menu ===
    menubar = Menu(app)

    tools_menu = Menu(menubar, tearoff=0)
    tools_menu.add_command(label="🤖 AI-помощник", command=ai_assistant_callback)
    menubar.add_cascade(label="Помощник", menu=tools_menu)

    file_menu = Menu(menubar, tearoff=0)
    file_menu.add_command(label="Загрузить", command=load_files)
    file_menu.add_command(label="Умный импорт и объединение", command=load_and_merge_smart)
    file_menu.add_command(label="Сохранить в Excel", command=export_to_excel)
    file_menu.add_command(label="Сохранить в PDF", command=export_to_pdf)
    file_menu.add_command(label="Объединить по ключу", command=merge_by_key)
    file_menu.add_command(label="Умное объединение", command=smart_merge_gui)
    file_menu.add_separator()
    file_menu.add_command(label="Выход", command=app.destroy)
    menubar.add_cascade(label="Файл", menu=file_menu)

    edit_menu = Menu(menubar, tearoff=0)
    edit_menu.add_command(label="Отменить", command=undo)
    edit_menu.add_command(label="Повторить", command=redo)
    edit_menu.add_command(label="Очистить", command=clean_data)
    edit_menu.add_command(label="Добавить столбец", command=add_column)
    menubar.add_cascade(label="Редактирование", menu=edit_menu)

    filter_menu = Menu(menubar, tearoff=0)
    filter_menu.add_command(label="Простой фильтр", command=filter_data)
    filter_menu.add_command(label="Расширенный фильтр", command=apply_advanced_filter)
    menubar.add_cascade(label="Фильтрация", menu=filter_menu)

    analytics_menu = Menu(menubar, tearoff=0)
    analytics_menu.add_command(label="Сводка", command=show_summary)
    analytics_menu.add_command(label="Гистограмма", command=show_histogram)
    analytics_menu.add_command(label="Круговая", command=show_pie_chart)
    analytics_menu.add_command(label="Линия", command=show_line_chart)
    analytics_menu.add_command(label="Расчёт длительности", command=calculate_duration)
    analytics_menu.add_command(label="Проверка данных", command=validate_data)
    menubar.add_cascade(label="Аналитика", menu=analytics_menu)


    templates_menu = Menu(menubar, tearoff=0)
    templates_menu.add_command(label="Применить шаблон", command=apply_template_action)
    templates_menu.add_command(label="Сохранить как шаблон", command=save_current_as_template)
    templates_menu.add_command(label="Управление шаблонами", command=open_template_manager)
    menubar.add_cascade(label="Шаблоны", menu=templates_menu)

    settings_menu = Menu(menubar, tearoff=0)
    settings_menu.add_command(label="Сменить тему", command=switch_theme)
    settings_menu.add_command(label="Автообработка", command=toggle_autowatch)
    menubar.add_cascade(label="Настройки", menu=settings_menu)


    validation_menu = Menu(menubar, tearoff=0)
    validation_menu.add_command(label="Проверить данные", command=check_errors_and_edit)
    menubar.add_cascade(label="Валидация", menu=validation_menu)

    app.config(menu=menubar)

    global table_frame
    table_frame = tb.Frame(app)
    table_frame.pack(fill=tb.BOTH, expand=True, padx=10, pady=10)

    tb.Label(app, text="Журнал:").pack(anchor="w", padx=10)
    log_box = tb.Text(app, height=6, bg="#2c2c2c", fg="white", insertbackground="white")
    log_box.pack(fill=tb.X, padx=10, pady=(0, 5))

    status_label = tb.Label(app, text="Готово", anchor="w", bootstyle="inverse")
    status_label.pack(fill=tb.X, side=tb.BOTTOM, ipady=2)

    load_history()
    app.mainloop()