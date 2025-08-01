import matplotlib.pyplot as plt

def get_summary(df):
    summary = f"📝 Сводка:\n"
    summary += f"Строк: {len(df)}\n"
    summary += f"Столбцов: {len(df.columns)}\n"
    summary += f"Названия столбцов: {', '.join(df.columns)}\n"
    summary += "\nТипы данных:\n"
    summary += df.dtypes.to_string()
    return summary

def plot_histogram(df, column):
    try:
        plt.figure(figsize=(10, 5))
        df[column].value_counts().plot(kind="bar", color="skyblue", edgecolor="black")
        plt.title(f"Гистограмма: {column}")
        plt.xlabel(column)
        plt.ylabel("Частота")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.show()
    except Exception as e:
        print(f"Ошибка при построении гистограммы: {e}")

def plot_pie(df, column):
    try:
        plt.figure(figsize=(6, 6))
        df[column].value_counts().plot(kind="pie", autopct="%1.1f%%", startangle=90, shadow=True)
        plt.title(f"Круговая диаграмма: {column}")
        plt.ylabel("")  # убрать подпись оси Y
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Ошибка при построении круговой диаграммы: {e}")

def plot_line(df, x_col, y_col):
    try:
        plt.figure(figsize=(10, 5))
        df_sorted = df.sort_values(by=x_col)
        plt.plot(df_sorted[x_col], df_sorted[y_col], marker="o", linestyle="-", color="mediumvioletred")
        plt.title(f"Линейный график: {x_col} → {y_col}")
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Ошибка при построении линейного графика: {e}")
