import datetime
from typing import Dict

from dagster import EventMetadataEntry, Output, OutputDefinition, SolidExecutionContext, solid
from pandas import DataFrame

from dealpipe import schema


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame)],
)
def transform(context: SolidExecutionContext, df: DataFrame, deals_lookup: Dict):
    run_id = context.pipeline_run.run_id
    run_stats = context.instance.get_run_stats(run_id)
    as_of_date = datetime.datetime.fromtimestamp(run_stats.start_time)
    df = schema.transform(df, run_id, as_of_date, deals_lookup)

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
