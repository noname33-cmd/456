# core/validator.py
import re
import pandas as pd

KEY_COLUMNS = ["иин", "инн", "uid", "id", "уид"]
EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

def validate_dataframe(df: pd.DataFrame):
    errors = []
    highlight_cells = []

    columns_lower = {col.lower(): col for col in df.columns}

    for key in KEY_COLUMNS:
        if key in columns_lower:
            col = columns_lower[key]
            for idx, val in df[col].items():
                if pd.isna(val) or not str(val).isdigit() or len(str(val)) not in (12, 10):
                    errors.append({"row": idx, "column": col, "error": f"Некорректный {col}"})
                    highlight_cells.append((idx, col))

    for col in df.columns:
        if "email" in col.lower():
            for idx, val in df[col].items():
                if pd.notna(val) and not EMAIL_REGEX.match(str(val)):
                    errors.append({"row": idx, "column": col, "error": "Некорректный email"})
                    highlight_cells.append((idx, col))

    for key in KEY_COLUMNS:
        if key in columns_lower:
            col = columns_lower[key]
            duplicated = df[df.duplicated(subset=[col], keep=False)]
            for idx in duplicated.index:
                errors.append({"row": idx, "column": col, "error": "Дубликат значения"})
                highlight_cells.append((idx, col))

    return errors, highlight_cells

