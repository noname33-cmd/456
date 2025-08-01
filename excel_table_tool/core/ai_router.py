# core/ai_router.py

def route_command(command: str, df, ui_actions: dict):
    """
    Интерпретировать и выполнить команду, используя доступные действия интерфейса.
    :param command: Текстовая команда от пользователя
    :param df: текущий DataFrame
    :param ui_actions: словарь {ключ: callable функция}
    :return: сообщение или новый df
    """
    command = command.strip().lower()

    if "загрузи файл" in command or "открой файл" in command:
        return ui_actions["load_files"]()

    elif "умное объединение" in command:
        return ui_actions["load_and_merge_smart"]()

    elif "сохранить в excel" in command:
        return ui_actions["export_excel"]()

    elif "сохранить в pdf" in command:
        return ui_actions["export_pdf"]()

    elif "добавить столбец" in command:
        return ui_actions["add_column"]()

    elif "очисти" in command:
        return ui_actions["clean_data"]()

    elif "применить шаблон" in command:
        return ui_actions["apply_template"]()

    elif "сохрани шаблон" in command:
        return ui_actions["save_template"]()

    elif "объединить по ключу" in command:
        return ui_actions["merge_by_key"]()

    elif "показать сводку" in command:
        return ui_actions["show_summary"]()

    elif "гистограмма" in command:
        return ui_actions["show_histogram"]()

    elif "круговая" in command:
        return ui_actions["show_pie"]()

    elif "линейный график" in command:
        return ui_actions["show_line"]()

    elif "фильтр" in command and "расширенный" in command:
        return ui_actions["advanced_filter"]()

    elif "фильтр" in command:
        return ui_actions["simple_filter"]()

    elif "валидация" in command or "проверить" in command:
        return ui_actions["check_errors"]()

    elif "отменить" in command:
        return ui_actions["undo"]()

    elif "повторить" in command:
        return ui_actions["redo"]()

    elif "сменить тему" in command:
        return ui_actions["switch_theme"]()

    elif "автообработка" in command:
        return ui_actions["toggle_autowatch"]()

    elif "объединение с ai" in command or "smart объединение" in command:
        return ui_actions["smart_merge"]()

    elif "запусти ai" in command or "ai-помощник" in command:
        return ui_actions["ai_assistant"]()

    else:
        return f"⚠️ Неизвестная команда: {command}"
