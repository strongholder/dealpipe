from pathlib import Path

import pandas as pd
from dagster import SolidExecutionContext, solid
from pandas import DataFrame


@solid(
    config_schema={"errors_excel_file": str},
)
def save_error_report(context: SolidExecutionContext, df: DataFrame):
    errors_excel_file = context.solid_config["errors_excel_file"]
    Path(errors_excel_file).parent.mkdir(parents=True, exist_ok=True)

    writer = pd.ExcelWriter(errors_excel_file, engine="xlsxwriter")
    sheet_name = "Errors"
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    wrap_format = workbook.add_format({"text_wrap": True, "font_color": "red"})
    errors_column = "O:O"
    worksheet.set_column(errors_column, width=55, cell_format=wrap_format)
    writer.save()
