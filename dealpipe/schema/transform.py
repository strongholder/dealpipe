import datetime
import decimal
import hashlib
import math
from typing import cast

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from dealpipe.lookups import LookupDict
from dealpipe.schema.schema import InputSchema, OutputSchema


def hash_row(row):
    row_string = row.to_csv().encode("utf-8")
    md5_hash = hashlib.md5(row_string).hexdigest()
    return md5_hash


def to_decimal(x):
    if x is None:
        return None

    val = decimal.Decimal(str(x))

    if math.isnan(val):
        val = None

    return val


@pa.check_types
def transform(
    df: DataFrame[InputSchema], run_id: str, as_of_date: datetime.datetime, lookups: LookupDict
) -> DataFrame[OutputSchema]:
    columns = {}
    columns[OutputSchema.d1] = lambda x: x[OutputSchema.d1].map(to_decimal)
    columns[OutputSchema.d2] = lambda x: x[OutputSchema.d2].map(to_decimal)
    columns[OutputSchema.d3] = lambda x: x[OutputSchema.d3].map(to_decimal)
    columns[OutputSchema.d4] = lambda x: x[OutputSchema.d4].map(to_decimal)
    columns[OutputSchema.d5] = lambda x: x[OutputSchema.d5].map(to_decimal)
    columns[OutputSchema.is_active] = lambda x: x[OutputSchema.is_active].map({"Yes": True, "No": False}.get)
    columns[OutputSchema.company_name] = lambda x: x[InputSchema.company_id].map(lookups["companies"].get)
    columns[OutputSchema.row_hash] = lambda x: x.apply(hash_row, axis=1)
    columns[OutputSchema.process_identifier] = run_id
    columns[OutputSchema.as_of_date] = as_of_date
    columns[OutputSchema.row_no] = pd.Series(range(len(df)))

    out_df = df.assign(**columns)
    out_df = out_df[list(OutputSchema.__fields__.keys())]
    return cast(DataFrame[OutputSchema], out_df)
