import pandas as pd

def filter_by_keyword(df, keyword):
    mask = df.apply(lambda row: row.astype(str).str.contains(keyword, case=False, na=False).any(), axis=1)
    return df[mask]

def add_column(df, name, default_value=""):
    df[name] = default_value
    return df

def advanced_filter(df, column, operator, value):
    if column not in df.columns:
        return df

    try:
        if operator == "==":
            return df[df[column] == value]
        elif operator == "!=":
            return df[df[column] != value]
        elif operator == "contains":
            return df[df[column].astype(str).str.contains(str(value), na=False)]
        elif operator == ">":
            return df[pd.to_numeric(df[column], errors='coerce') > float(value)]
        elif operator == "<":
            return df[pd.to_numeric(df[column], errors='coerce') < float(value)]
        elif operator == ">=":
            return df[pd.to_numeric(df[column], errors='coerce') >= float(value)]
        elif operator == "<=":
            return df[pd.to_numeric(df[column], errors='coerce') <= float(value)]
    except Exception as e:
        print(f"Ошибка в фильтрации: {e}")
        return df

    return df

def clean_dataframe(df):
    df = df.copy()
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df = df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
    df = df.convert_dtypes()
    df = df.drop_duplicates()
    return df

def apply_template(df, template_name):
    import json
    import os
    template_file = os.path.join("templates", f"{template_name}.json")
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Шаблон '{template_name}' не найден.")

    with open(template_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    if config.get("drop_columns"):
        df = df.drop(columns=config["drop_columns"], errors="ignore")

    if config.get("add_columns"):
        for col in config["add_columns"]:
            df[col["name"]] = col.get("default", "")

    if config.get("filters"):
        for f in config["filters"]:
            col, op, val = f["column"], f["operator"], f["value"]
            if op == "==":
                df = df[df[col] == val]
            elif op == "!=":
                df = df[df[col] != val]
            elif op == ">":
                df = df[df[col] > val]
            elif op == "<":
                df = df[df[col] < val]
            elif op == "contains":
                df = df[df[col].astype(str).str.contains(str(val))]

    if config.get("clean"):
        df = clean_dataframe(df)

    return df

def calculate_duration_column(df, start_col, end_col, output_col="Длительность (мин)"):
    """
    Рассчитать разницу между start_col и end_col и сохранить в новый столбец output_col.
    """
    df = df.copy()
    try:
        start_times = pd.to_datetime(df[start_col], errors="coerce")
        end_times = pd.to_datetime(df[end_col], errors="coerce")
        df[output_col] = (end_times - start_times).dt.total_seconds() / 60
        df[output_col] = df[output_col].round(2)
    except Exception as e:
        print(f"Ошибка при расчёте длительности: {e}")
    return df
