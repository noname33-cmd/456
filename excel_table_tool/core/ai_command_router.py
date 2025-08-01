# core/ai_command_router.py
from core import data_processor, template_manager, file_loader
from ui import dialogs
import os

def execute_command(text, df, update_ui_callback):
    text = text.lower()

    if "загрузить" in text and "файл" in text:
        paths = dialogs.ask_file_paths()
        return file_loader.load_multiple_files(paths)[0]

    if "очистить" in text:
        return data_processor.clean_dataframe(df)

    if "добавь столбец" in text or "новый столбец" in text:
        col = dialogs.ask_string("AI", "Название столбца")
        val = dialogs.ask_string("AI", "Значение по умолчанию")
        return data_processor.add_column(df, col, val)

    if "применить шаблон" in text:
        tpl = dialogs.select_template_dialog(template_manager.list_templates())
        return data_processor.apply_template(df, tpl)

    if "фильтр" in text:
        kw = dialogs.ask_string("Фильтр", "Ключевое слово:")
        return data_processor.filter_by_keyword(df, kw)

    update_ui_callback(f"Неизвестная команда: {text}")
    return df
