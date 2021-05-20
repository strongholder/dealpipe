from collections import defaultdict
from typing import Dict

import pandas as pd
from dagster import EventMetadataEntry, Output, OutputDefinition, solid
from pandas import DataFrame

from .expectations import render_error_description


@solid(
    output_defs=[
        OutputDefinition(is_required=False),
    ]
)
def generate_error_report(_, df: DataFrame, validations: Dict) -> DataFrame:

    errors = defaultdict(list)
    failed_validations = [result for result in validations["results"] if not result["success"]]

    for result in failed_validations:
        error = result["expectation_config"]
        unexpected_list = result["result"]["unexpected_index_list"]
        for row_index in unexpected_list:
            errors[row_index].append(render_error_description(error))

    errors = {k + 2: "\n".join(v) for k, v in errors.items()}
    error_df = DataFrame({"Row": errors.keys(), "Errors": errors.values()})
    merged_df = pd.merge(df, error_df, left_on="RowNo", right_on="Row")
    merged_df.drop("Row", axis="columns", inplace=True)

    meta_stats = EventMetadataEntry.md(
        md_str=merged_df.to_markdown(),
        label="Deals error report",
    )
    yield Output(
        value=merged_df,
        metadata_entries=[
            meta_stats,
        ],
    )
