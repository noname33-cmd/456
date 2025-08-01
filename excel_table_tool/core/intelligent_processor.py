# core/intelligent_processor.py

import pandas as pd
from tkinter import messagebox


def auto_detect_reference_table(dfs):
    """
    Из двух DataFrame определить, какой содержит больше уникальных ИИН (как справочник)
    """
    if len(dfs) != 2:
        return None, None

    df1, df2 = dfs[0], dfs[1]
    for key in ["ИИН", "iin", "iin_number"]:
        if key in df1.columns and key in df2.columns:
            if df1[key].nunique() > df2[key].nunique():
                return df2, df1  # df_main, df_lookup
            else:
                return df1, df2
    return None, None


def smart_merge_by_key(df_main, df_lookup, key="ИИН"):
    """
    Автоматически объединить два DataFrame по ключевому столбцу
    """
    if key not in df_main.columns or key not in df_lookup.columns:
        return df_main

    columns_to_add = [col for col in df_lookup.columns if col != key and col not in df_main.columns]
    if not columns_to_add:
        return df_main

    df_merge = df_lookup[[key] + columns_to_add].drop_duplicates(subset=[key])
    df_result = pd.merge(df_main, df_merge, how="left", on=key)
    return df_result


def smart_merge_flow(dfs, key="ИИН"):
    """
    Управление всей логикой smart merge
    """
    df_main, df_lookup = auto_detect_reference_table(dfs)
    if df_main is None or df_lookup is None:
        return dfs[0] if dfs else pd.DataFrame()

    # Предлагаем пользователю
    confirmed = messagebox.askyesno("Объединение", f"Найден общий ключ '{key}'. Объединить таблицы?")
    if not confirmed:
        return df_main

    df_merged = smart_merge_by_key(df_main, df_lookup, key)
    messagebox.showinfo("Успешно", f"Объединено по '{key}'. Добавлено столбцов: {len(df_merged.columns) - len(df_main.columns)}")
    return df_merged

def smart_load_and_merge(main_df, path_to_reference):
    import re

    ref_df = file_loader.load_files([path_to_reference])
    if ref_df.empty:
        return main_df, "Не удалось загрузить справочник."

    ref_df.columns = [c.strip().replace("\n", " ") for c in ref_df.columns]

    possible_keys = [c for c in ref_df.columns if re.search(r'(иин|id|инн)', c.lower())]
    if not possible_keys:
        possible_keys = [ref_df.columns[0]]

    from_col = possible_keys[0]
    to_col = [c for c in ref_df.columns if c != from_col][:3]  # до 3 столбцов максимум

    ref_df = ref_df.drop_duplicates(subset=[from_col])
    main_df[from_col] = main_df[from_col].astype(str)
    ref_df[from_col] = ref_df[from_col].astype(str)

    merged = pd.merge(main_df, ref_df[[from_col] + to_col], on=from_col, how="left")
    return merged, f"Объединено по: {from_col} (добавлены: {to_col})"
