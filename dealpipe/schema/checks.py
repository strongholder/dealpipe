import decimal

import pandas as pd
from pandera.extensions import register_check_method


@register_check_method(check_type="element_wise")
def is_numeric(value, *args):
    return is_numeric_fn(value)


def is_numeric_fn(value):
    return pd.notnull(pd.to_numeric(value, errors="coerce"))


@register_check_method(check_type="element_wise")
def is_decimal(value, *args):
    return is_decimal_fn(value)


def is_decimal_fn(value):
    return isinstance(value, decimal.Decimal)


@register_check_method(check_type="element_wise")
def is_active(value, *args):
    return is_active_fn(value)


def is_active_fn(value):
    return value in {"Yes", "No"}
