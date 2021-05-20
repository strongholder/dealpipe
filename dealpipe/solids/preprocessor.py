import datetime
import hashlib

import pandas as pd
from dagster import EventMetadataEntry, Output, SolidExecutionContext, solid
from pandas import DataFrame


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
