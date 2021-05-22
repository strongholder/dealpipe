import pandas as pd
from dagster import solid
from pandas import DataFrame

from dealpipe import reader


def to_numeric(x):
    return pd.to_numeric(x, errors="ignore")


def build_converters():
    converters = {f"D{i}": to_numeric for i in range(1, 6)}
    converters["CompanyId"] = to_numeric

    return converters


@solid(
    config_schema={"deals_file": str},
)
def load_deals_file(context) -> DataFrame:
    deals_file = context.solid_config["deals_file"]

    return reader.read(deals_file, build_converters())
