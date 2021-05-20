from pathlib import Path

from dagster import AssetMaterialization, EventMetadata, Field, Output, SolidExecutionContext, solid

from dealpipe.solids.validation.types import DealsDataFrame


@solid(
    config_schema={
        "output_file": Field(str, description="path to the output file"),
        "compression": Field(str, default_value="gzip", is_required=False),
    }
)
def save_output(context: SolidExecutionContext, df: DealsDataFrame):
    output_file = context.solid_config["output_file"]
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_file, compression=context.solid_config["compression"])

    yield AssetMaterialization(
        asset_key="output_parquet_file",
        description="Processed parquet output file",
        metadata={"output_parquet_file_path": EventMetadata.path(output_file)},
    )
    yield Output(None)
