# core/file_loader.py

import pandas as pd
import os

def load_files(paths):
    """Загружает и объединяет список файлов в один DataFrame."""
    df_list = []

    for path in paths:
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(path, engine="openpyxl")
            elif ext == ".csv":
                df = pd.read_csv(path, sep=None, engine="python")  # auto-separator
            elif ext == ".json":
                df = pd.read_json(path)
            elif ext == ".txt":
                df = pd.read_csv(path, sep=None, engine="python")
            else:
                print(f"Неподдерживаемый формат файла: {path}")
                continue
            df_list.append(df)
        except Exception as e:
            print(f"Ошибка при загрузке файла {path}: {e}")

    if df_list:
        return pd.concat(df_list, ignore_index=True)
    else:
        return pd.DataFrame()

def load_multiple_files(paths):
    dfs = []
    for path in paths:
        df = load_files([path])
        if not df.empty:
            dfs.append(df)
    return dfs