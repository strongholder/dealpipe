from dagster import AssetMaterialization, EventMetadata, Field, Output, SolidExecutionContext, solid
from pandas import DataFrame

from dealpipe import writer


@solid(
    config_schema={
        "output_file": Field(str, description="path to the output file"),
        "compression": Field(str, default_value="gzip", is_required=False),
    }
)
def save_output(context: SolidExecutionContext, df: DataFrame):
    output_file = context.solid_config["output_file"]
    writer.write_parquet(df, output_file, context.solid_config["compression"])

    yield AssetMaterialization(
        asset_key="output_parquet_file",
        description="Processed parquet output file",
        metadata={"output_parquet_file_path": EventMetadata.path(output_file)},
    )
    yield Output(None)


@solid(
    config_schema={"errors_excel_file": str},
)
def save_errors(context: SolidExecutionContext, df: DataFrame):
    errors_excel_file = context.solid_config["errors_excel_file"]
    writer.write_excel(df, errors_excel_file, sheet_name="Errors")

    yield AssetMaterialization(
        asset_key="errors_excel_file",
        description="Excel row-wise aggregate error report",
        metadata={"errors_excel_file_path": EventMetadata.path(errors_excel_file)},
    )
    yield Output(None)
