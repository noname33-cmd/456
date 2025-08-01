# core/multi_file_analyzer.py

import pandas as pd
from collections import Counter
from core.intelligent_processor import COMMON_KEYS

def normalize_column(col):
    return col.strip().lower().replace(" ", "").replace("_", "")

def detect_key_candidates(df):
    norm_cols = [normalize_column(c) for c in df.columns]
    return [c for c, norm in zip(df.columns, norm_cols) if norm in COMMON_KEYS]

def analyze_files(file_dfs: dict):
    """
    Принимает словарь {имя_файла: DataFrame}
    Возвращает:
    - основной файл (наибольшее число строк)
    - список справочных файлов
    - найденные общие ключевые столбцы
    """
    if not file_dfs:
        return None, [], [], "Нет загруженных данных."

    sorted_dfs = sorted(file_dfs.items(), key=lambda x: len(x[1]), reverse=True)
    main_file_name, main_df = sorted_dfs[0]
    ref_files = sorted_dfs[1:]

    key_scores = Counter()
    for name, df in file_dfs.items():
        for col in detect_key_candidates(df):
            key_scores[col] += 1

    best_keys = [k for k, v in key_scores.items() if v > 1]

    return main_file_name, ref_files, best_keys, None

def join_with_references(main_df: pd.DataFrame, ref_dfs: list, key_col: str):
    """
    Объединяет основной датафрейм с другими по ключу key_col
    """
    result_df = main_df.copy()
    for name, ref_df in ref_dfs:
        if key_col in ref_df.columns:
            merged = ref_df.drop_duplicates(subset=key_col)
            merged = merged.set_index(key_col)
            result_df = result_df.merge(merged, how="left", left_on=key_col, right_index=True, suffixes=("", f"_{name}"))
    return result_df
