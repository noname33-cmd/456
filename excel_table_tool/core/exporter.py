# core/exporter.py

import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet


def export_to_excel(df, path):
    """Сохраняет DataFrame в Excel (.xlsx)"""
    try:
        df.to_excel(path, index=False)
    except Exception as e:
        print(f"Ошибка при сохранении Excel: {e}")


def export_to_csv(df, path, sep=";"):
    """Сохраняет DataFrame в CSV"""
    try:
        df.to_csv(path, sep=sep, index=False)
    except Exception as e:
        print(f"Ошибка при сохранении CSV: {e}")


def export_to_pdf(df, path):
    """Сохраняет DataFrame в PDF"""
    try:
        pdf = SimpleDocTemplate(path, pagesize=landscape(A4))
        styles = getSampleStyleSheet()

        data = [list(df.columns)] + df.astype(str).values.tolist()

        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ]))

        title = Paragraph("Отчёт по таблице", styles['Heading2'])
        pdf.build([title, table])
    except Exception as e:
        print(f"Ошибка при создании PDF: {e}")
