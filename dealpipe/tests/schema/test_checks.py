from decimal import Decimal

from dealpipe.schema.checks import is_active_fn, is_decimal_fn, is_numeric_fn


def test_is_numeric():
    assert is_numeric_fn(12.32)
    assert is_numeric_fn(1)
    assert is_numeric_fn("123.3343334")
    assert is_numeric_fn("12")
    assert is_numeric_fn(Decimal("11.33"))


def test_is_decimal():
    assert is_decimal_fn(Decimal("12.22"))
    assert is_decimal_fn(Decimal("NaN"))
    assert not is_decimal_fn(12.22)
    assert not is_decimal_fn(12)


def test_is_active():
    assert is_active_fn("Yes")
    assert is_active_fn("No")
    assert not is_active_fn("True")
    assert not is_active_fn("False")
    assert not is_active_fn(None)
