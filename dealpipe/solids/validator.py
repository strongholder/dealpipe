from typing import Dict

from dagster import EventMetadataEntry, Output, OutputDefinition, SolidExecutionContext, solid
from pandas import DataFrame

from dealpipe.schema import validate_input


@solid(
    output_defs=[
        OutputDefinition(name="valid", dagster_type=DataFrame, is_required=False),
        OutputDefinition(name="errors", dagster_type=DataFrame, is_required=False),
    ],
)
def validate(context: SolidExecutionContext, df: DataFrame, lookup: Dict):
    valid, input_df = validate_input(df, lookup)

    meta_stats = EventMetadataEntry.md(
        md_str=input_df.to_markdown(),
        label="Deals validation Stats",
    )

    if valid:
        yield Output(
            input_df,
            "valid",
            metadata_entries=[
                meta_stats,
            ],
        )
    else:
        yield Output(
            input_df,
            "errors",
            metadata_entries=[
                meta_stats,
            ],
        )
