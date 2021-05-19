import datetime
import decimal
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import great_expectations as ge
import pandas as pd
from dagster import (
    EventMetadataEntry,
    Field,
    InputDefinition,
    ModeDefinition,
    Output,
    OutputDefinition,
    PresetDefinition,
    SolidExecutionContext,
    file_relative_path,
    pipeline,
    solid,
)
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from dagster_pandas.constraints import ColumnDTypeFnConstraint
from pandas import DataFrame, read_csv, read_excel
from pandas.core.dtypes.common import is_object_dtype

from dealpipe.expectations import build_deals_expectation_suite, render_validation_result_markdown
from dealpipe.mime import is_excel

LookupDict = Dict[str, Any]


DealsDataFrame = create_dagster_pandas_dataframe_type(
    name="DealsDataFrame",
    columns=[
        PandasColumn.integer_column("RowNo"),
        PandasColumn.string_column("DealName", is_required=True),
        PandasColumn(name="D1", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D2", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D3", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D4", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D5", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn.boolean_column("IsActive", is_required=True),
        PandasColumn.string_column("CountryCode", is_required=True),
        PandasColumn.string_column("CurrencyCode", is_required=True),
        PandasColumn.integer_column("CompanyId", is_required=True),
        PandasColumn.string_column("CompanyName", is_required=True),
        PandasColumn.datetime_column("AsOfDate"),
        PandasColumn.string_column("ProcessIdentifier"),
        PandasColumn.string_column("RowHash"),
    ],
)


@solid(
    config_schema={"deals_file": str},
)
def load_deals_file(context) -> DataFrame:
    deals_file = context.solid_config["deals_file"]

    def to_numeric(x):
        return pd.to_numeric(x, errors="ignore")

    converters = {f"D{i}": to_numeric for i in range(1, 6)}
    converters["CompanyId"] = to_numeric

    if is_excel(deals_file):
        result = read_excel(deals_file, converters=converters)
    else:
        result = read_csv(deals_file, converters=converters)

    return result


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


@solid
def preprocess(context: SolidExecutionContext, df: DataFrame) -> DataFrame:
    def remap_is_active(row):
        is_active = row["IsActive"].lower()
        if is_active in ["yes", "no"]:
            return is_active == "yes"
        else:
            return row["IsActive"]

    for i in range(1, 6):
        df[f"D{i}"] = pd.to_numeric(df[f"D{i}"], errors="ignore")

    df["IsActive"] = df.apply(remap_is_active, axis=1)
    df["CompanyId"] = pd.to_numeric(df["CompanyId"], errors="ignore")

    run_id = context.pipeline_run.run_id
    run_stats = context.instance.get_run_stats(run_id)
    df["AsOfDate"] = datetime.datetime.fromtimestamp(run_stats.start_time)
    df["ProcessIdentifier"] = run_id

    def hash_row(row):
        row_string = row.to_csv().encode("utf-8")
        md5_hash = hashlib.md5(row_string).hexdigest()
        return md5_hash

    df["RowHash"] = df.apply(hash_row, axis=1)

    # Insert RowNo at the beginning
    df["RowNo"] = pd.Series(range(2, len(df) + 2))
    columns = ["RowNo"] + [x for x in df.columns if x != "RowNo"]
    df = df[columns]
    context.log.info(str(df.dtypes))

    meta_stats = EventMetadataEntry.md(
        md_str=df.to_markdown(),
        label="Preprocessed deals dataframe",
    )

    return Output(
        value=df,
        metadata_entries=[
            meta_stats,
        ],
    )


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


@solid(
    output_defs=[
        OutputDefinition(is_required=False),
    ]
)
def generate_error_report(context: SolidExecutionContext, df: DataFrame, validations: Dict) -> DataFrame:
    from dealpipe.expectations import render_error_description

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


@solid(
    config_schema={"errors_excel_file": str},
)
def save_error_report(context: SolidExecutionContext, df: DataFrame):
    errors_excel_file = context.solid_config["errors_excel_file"]
    Path(errors_excel_file).parent.mkdir(parents=True, exist_ok=True)

    writer = pd.ExcelWriter(errors_excel_file, engine="xlsxwriter")
    sheet_name = "Errors"
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    wrap_format = workbook.add_format({"text_wrap": True, "font_color": "red"})
    errors_column = "O:O"
    worksheet.set_column(errors_column, width=55, cell_format=wrap_format)
    writer.save()


@solid(
    input_defs=[InputDefinition("valid", bool)],
    output_defs=[OutputDefinition(dagster_type=DealsDataFrame)],
)
def postprocess(context: SolidExecutionContext, df: DataFrame, deals_lookup: LookupDict, valid: bool):
    df["CompanyName"] = df.apply(lambda row: deals_lookup["companies"][row["CompanyId"]], axis="columns")

    col_types = {
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

    df = df[list(col_types.keys())].astype(col_types)

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


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="unittest",
            resource_defs={},
        ),
        ModeDefinition(
            name="dev",
            resource_defs={},
        ),
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "invalid_xlsx_example",
            config_files=[
                file_relative_path(__file__, "../presets/invalid_xlsx_preset.yaml"),
            ],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "valid_xlsx_example",
            config_files=[
                file_relative_path(__file__, "../presets/valid_xlsx_preset.yaml"),
            ],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "invalid_csv_example",
            config_files=[
                file_relative_path(__file__, "../presets/invalid_csv_preset.yaml"),
            ],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "valid_csv_example",
            config_files=[
                file_relative_path(__file__, "../presets/valid_csv_preset.yaml"),
            ],
            mode="dev",
        ),
    ],
)
def process_deals():
    deals_lookup = load_deals_lookup()
    preprocessed_df = preprocess(load_deals_file())
    valid, errors = validate(preprocessed_df, deals_lookup)
    error_report_df = generate_error_report(preprocessed_df, errors)
    save_error_report(error_report_df)
    output_df = postprocess(preprocessed_df, deals_lookup, valid)
    save_output(output_df)
