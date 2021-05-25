from typing import Tuple, cast

import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors
from pandera.typing import Bool, DateTime, Int32, Object, Series, String

from dealpipe.lookups import LookupDict
from dealpipe.schema.checks import *  # noqa: F401, F403


class InputSchema(pa.SchemaModel):
    _lookups: LookupDict = LookupDict(currencies=set(), countries=set(), companies={})
    deal_name: Series[String] = pa.Field(alias="DealName", str_length={"min_value": 1})
    d1: Series[String] = pa.Field(alias="D1", coerce=True, is_numeric={})
    d2: Series[String] = pa.Field(alias="D2", coerce=True, nullable=True, is_numeric={})
    d3: Series[String] = pa.Field(alias="D3", coerce=True, nullable=True, is_numeric={})
    d4: Series[String] = pa.Field(alias="D4", coerce=True, nullable=True, is_numeric={})
    d5: Series[String] = pa.Field(alias="D5", coerce=True, nullable=True, is_numeric={})
    is_active: Series[String] = pa.Field(alias="IsActive", is_active={"error": """Must be either "Yes" or "No."""})
    country_code: Series[String] = pa.Field(
        alias="CountryCode", coerce=True, str_length={"min_value": 3, "max_value": 3}
    )
    currency_code: Series[String] = pa.Field(
        alias="CurrencyCode", coerce=True, str_length={"min_value": 3, "max_value": 3}
    )
    company_id: Series[Int32] = pa.Field(alias="CompanyId", coerce=True)

    class Config:
        ordered = True
        strict = True

    @classmethod
    def set_lookups(cls, lookups: LookupDict):
        cls._lookups = lookups

    @pa.check(country_code, name="check_country__in_lookups", error="Country code not allowed.", element_wise=True)
    def check_country(cls, country_code: str) -> bool:
        return country_code in cls._lookups["countries"]

    @pa.check(currency_code, name="check_currency_in_lookups", error="Currency code not allowed.", element_wise=True)
    def check_currency(cls, currency_code: str) -> bool:
        return currency_code in cls._lookups["currencies"]

    @pa.check(company_id, name="check_company_in_lookups", error="Company not allowed.", element_wise=True)
    def check_company(cls, company_id: int) -> bool:
        return company_id in cls._lookups["companies"]


class RowNoMixin(pa.SchemaModel):
    row_no: Series[Int32] = pa.Field(alias="RowNo", coerce=True)


class AdditionalColumnsMixin(pa.SchemaModel):
    company_name: Series[String] = pa.Field(alias="CompanyName", coerce=True)
    as_of_date: Series[DateTime] = pa.Field(alias="AsOfDate", coerce=True)
    process_identifier: Series[String] = pa.Field(alias="ProcessIdentifier", coerce=True)
    row_hash: Series[String] = pa.Field(alias="RowHash", coerce=True)


class OutputSchema(AdditionalColumnsMixin, InputSchema, RowNoMixin):
    d1: Series[Object] = pa.Field(alias="D1", is_decimal={})
    d2: Series[Object] = pa.Field(alias="D2", nullable=True, is_decimal={})
    d3: Series[Object] = pa.Field(alias="D3", nullable=True, is_decimal={})
    d4: Series[Object] = pa.Field(alias="D4", nullable=True, is_decimal={})
    d5: Series[Object] = pa.Field(alias="D5", nullable=True, is_decimal={})
    is_active: Series[Bool] = pa.Field(alias="IsActive")

    class Config:
        ordered = True
        strict = True


def validate_input(df: pd.DataFrame, lookups: LookupDict) -> Tuple[bool, pd.DataFrame]:
    InputSchema.set_lookups(lookups)
    try:
        valid_df = InputSchema.validate(df, lazy=True)
        return True, valid_df
    except SchemaErrors as e:
        return False, cast(pd.DataFrame, e.failure_cases)
