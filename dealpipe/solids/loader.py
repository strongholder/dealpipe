import pandas as pd
from dagster import solid
from pandas import DataFrame, read_csv, read_excel

from dealpipe.mime import is_excel


def to_numeric(x):
    return pd.to_numeric(x, errors="ignore")


@solid(
    config_schema={"deals_file": str},
)
def load_deals_file(context) -> DataFrame:
    deals_file = context.solid_config["deals_file"]

    converters = {f"D{i}": to_numeric for i in range(1, 6)}
    converters["CompanyId"] = to_numeric

    if is_excel(deals_file):
        result = read_excel(deals_file, converters=converters)
    else:
        result = read_csv(deals_file, converters=converters)

    return result
