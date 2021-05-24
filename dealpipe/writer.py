from pathlib import Path

from pandas import DataFrame


def write_parquet(df: DataFrame, file: str, compression="gzip"):
    Path(file).parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(file, compression=compression)


def write_excel(df: DataFrame, file: str, sheet_name: str):
    Path(file).parent.mkdir(parents=True, exist_ok=True)

    df.to_excel(file, sheet_name=sheet_name, index=False)
