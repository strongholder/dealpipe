from unittest import TestCase

from pandas import DataFrame

from dealpipe.lookups import LookupDict
from dealpipe.schema import InputSchema, validate_input

LOOKUPS: LookupDict = {"companies": {1: "Microsoft"}, "currencies": {"USD"}, "countries": {"USA"}}
VALID_INPUT = DataFrame(
    [
        {
            InputSchema.deal_name: "deal",
            InputSchema.d1: "0.3",
            InputSchema.d2: None,
            InputSchema.d3: 1,
            InputSchema.d4: "1",
            InputSchema.d5: 12.32,
            InputSchema.is_active: "Yes",
            InputSchema.country_code: "USA",
            InputSchema.currency_code: "USD",
            InputSchema.company_id: "1",
        }
    ]
)


class TestValidateInput(TestCase):
    def test_valid_input(self):
        valid, _ = validate_input(VALID_INPUT, LOOKUPS)
        assert valid

    def test_empty_deal_name(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.deal_name] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_deal_name(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.deal_name] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_deal_name(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.deal_name, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_empty_d1(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d1] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_d1(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d1] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_d1(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.d1, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_empty_d2(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d2] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_d2(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.d2, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_empty_d3(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d3] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_d3(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.d3, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_empty_d4(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d4] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_d4(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.d4, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_empty_d5(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.d5] = [""]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_d5(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.d5, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_valid_is_active(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.is_active] = ["No"]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert valid

    def test_invalid_is_active(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.is_active] = ["False"]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_is_active(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.is_active] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_is_active(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.is_active, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_invalid_country_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.country_code] = ["IRL"]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_country_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.country_code] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_country_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.country_code, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_invalid_currency_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.currency_code] = ["GBP"]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_currency_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.currency_code] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_currency_code(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.currency_code, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_invalid_company_id(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.company_id] = [5]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_null_company_id(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df[InputSchema.company_id] = [None]

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid

    def test_missing_company_id(self):
        input_df = VALID_INPUT.copy(deep=True)
        input_df.drop(InputSchema.company_id, axis="columns", inplace=True)

        valid, _ = validate_input(input_df, LOOKUPS)

        assert not valid
