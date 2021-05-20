import decimal

from dagster import EventMetadataEntry, InputDefinition, Output, OutputDefinition, SolidExecutionContext, solid
from pandas import DataFrame

from dealpipe.solids.validation.types import DealsDataFrame, LookupDict

COL_TYPES = {
    "RowNo": "int64",
    "DealName": "str",
    "D1": "O",
    "D2": "O",
    "D3": "O",
    "D4": "O",
    "D5": "O",
    "IsActive": "bool",
    "CountryCode": "str",
    "CurrencyCode": "str",
    "CompanyId": "int64",
    "CompanyName": "str",
    "AsOfDate": "datetime64[ns]",
    "ProcessIdentifier": "str",
    "RowHash": "str",
}


@solid(
    input_defs=[InputDefinition("valid", bool)],
    output_defs=[OutputDefinition(dagster_type=DealsDataFrame)],
)
def postprocess(context: SolidExecutionContext, df: DataFrame, deals_lookup: LookupDict, valid: bool):
    df["CompanyName"] = df["CompanyId"].map(lambda x: deals_lookup["companies"][x])

    df = df[list(COL_TYPES.keys())].astype(COL_TYPES)

    for i in range(1, 6):
        df[f"D{i}"] = df[f"D{i}"].astype(str).map(decimal.Decimal)

    meta_stats = EventMetadataEntry.md(
        md_str=df.to_markdown(),
        label="Processed Deals",
    )
    yield Output(
        value=df,
        metadata_entries=[
            meta_stats,
        ],
    )
