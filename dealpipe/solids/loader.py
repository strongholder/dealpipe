from dagster import solid
from pandas import DataFrame

from dealpipe import reader


@solid(
    config_schema={"deals_file": str},
)
def load_deals(context) -> DataFrame:
    deals_file = context.solid_config["deals_file"]

    return reader.read(deals_file)
