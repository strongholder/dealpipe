import great_expectations as ge
from dagster import EventMetadataEntry, Output, OutputDefinition, SolidExecutionContext, solid
from pandas import DataFrame

from dealpipe.lookups import LookupDict
from dealpipe.solids.validation.expectations import build_deals_expectation_suite, render_validation_result_markdown


@solid(
    output_defs=[
        OutputDefinition(name="valid", dagster_type=bool, is_required=False),
        OutputDefinition(name="errors", dagster_type=dict, is_required=False),
    ],
)
def validate(context: SolidExecutionContext, df: DataFrame, lookup: LookupDict):
    ge_df = ge.from_pandas(df)
    expectation_suite = build_deals_expectation_suite(lookup)
    result = ge_df.validate(expectation_suite, result_format="COMPLETE")
    result_dict = result.to_json_dict()

    meta_stats = EventMetadataEntry.md(
        md_str=render_validation_result_markdown(result),
        label="Deals validation Stats",
    )

    if result.success:
        yield Output(
            True,
            "valid",
            metadata_entries=[
                meta_stats,
            ],
        )
    else:
        yield Output(
            result_dict,
            "errors",
            metadata_entries=[
                meta_stats,
            ],
        )
