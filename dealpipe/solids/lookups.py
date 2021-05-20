from dagster import EventMetadataEntry, Output, solid
from pandas import read_csv, read_excel

from dealpipe.mime import is_excel
from dealpipe.solids.validation.types import LookupDict


@solid(
    config_schema={"lookups_file": str},
)
def load_deals_lookup(context) -> LookupDict:
    lookup = {}
    lookups_file = context.solid_config["lookups_file"]
    if is_excel(lookups_file):
        result = read_excel(lookups_file, sheet_name=1)
    else:
        result = read_csv(lookups_file)

    lookup = dict(
        companies=dict(zip(result.CompanyId, result.CompanyName)),
        currencies=result["Currencies"].dropna().unique().tolist(),
        countries=result["Countries"].dropna().unique().tolist(),
    )

    meta_stats = EventMetadataEntry.json(
        data=lookup,
        label="Deals validation lookups",
    )

    return Output(
        value=lookup,
        metadata_entries=[
            meta_stats,
        ],
    )
