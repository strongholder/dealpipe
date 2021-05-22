from dagster import EventMetadataEntry, Output, OutputDefinition, solid

from dealpipe import reader
from dealpipe.solids.validation.types import LookupDict


@solid(
    config_schema={"lookups_file": str},
    output_defs=[OutputDefinition(dagster_type=LookupDict)],
)
def load_deals_lookup(context):
    lookup = {}
    lookups_file = context.solid_config["lookups_file"]
    result = reader.read(lookups_file, sheet=1)

    lookup = dict(
        companies=dict(zip(result["CompanyId"], result["CompanyName"])),
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
