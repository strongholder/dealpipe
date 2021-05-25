from typing import Dict

from dagster import EventMetadataEntry, Output, OutputDefinition, solid

from dealpipe.lookups import build_lookups


@solid(
    config_schema={"lookups_file": str},
    output_defs=[OutputDefinition(dagster_type=Dict)],
)
def load_deals_lookup(context):
    lookups_file = context.solid_config["lookups_file"]
    lookup = build_lookups(lookups_file)

    meta_stats = EventMetadataEntry.json(
        data={**lookup, "countries": list(lookup["countries"]), "currencies": list(lookup["currencies"])},
        label="Deals validation lookups",
    )

    return Output(
        value=lookup,
        metadata_entries=[
            meta_stats,
        ],
    )
