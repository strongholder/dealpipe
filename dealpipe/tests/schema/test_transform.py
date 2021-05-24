import datetime
from decimal import Decimal
from unittest import TestCase

import numpy as np
from pandas import DataFrame

from dealpipe.lookups import LookupDict
from dealpipe.schema import InputSchema, OutputSchema, transform

LOOKUPS: LookupDict = {"companies": {1: "Microsoft"}, "currencies": {"USD"}, "countries": {"USA"}}
VALID_INPUT = DataFrame(
    [
        {
            InputSchema.deal_name: "deal",
            InputSchema.d1: "129389.3232453454345333387439283",
            InputSchema.d2: None,
            InputSchema.d3: 1,
            InputSchema.d4: "1",
            InputSchema.d5: 129389.3232453454345333387439283,
            InputSchema.is_active: "Yes",
            InputSchema.country_code: "USA",
            InputSchema.currency_code: "USD",
            InputSchema.company_id: "1",
        }
    ]
)


class TestTransform(TestCase):
    def transform(self):
        InputSchema.set_lookups(LOOKUPS)
        return transform(VALID_INPUT, "123", datetime.datetime(2020, 1, 1, 0, 0), LOOKUPS)

    def test_transform_decimal_string(self):
        transformed = self.transform()

        assert transformed[OutputSchema.d1].iloc[0] == Decimal("129389.3232453454345333387439283")

    def test_transform_decimal_null(self):
        transformed = self.transform()

        assert transformed[OutputSchema.d2].iloc[0] is None

    def test_transform_decimal_int(self):
        transformed = self.transform()

        assert transformed[OutputSchema.d3].iloc[0] == Decimal("1")

    def test_transform_decimal_str_int(self):
        transformed = self.transform()

        assert transformed[OutputSchema.d4].iloc[0] == Decimal("1")

    def test_transform_decimal_float(self):
        transformed = self.transform()

        assert transformed[OutputSchema.d5].iloc[0] == Decimal("129389.32324534544")

    def test_transform_is_active_bool(self):
        transformed = self.transform()

        is_active = transformed[OutputSchema.is_active].iloc[0]

        assert isinstance(is_active, np.bool_)
        assert is_active

    def test_transform_company_id(self):
        transformed = self.transform()

        assert transformed[OutputSchema.company_id].iloc[0] == 1

    def test_transform_company_name(self):
        transformed = self.transform()

        assert transformed[OutputSchema.company_name].iloc[0] == "Microsoft"

    def test_transform_as_of_date(self):
        transformed = self.transform()

        assert transformed[OutputSchema.as_of_date].iloc[0] == datetime.datetime(2020, 1, 1, 0, 0)

    def test_transform_process_identifier(self):
        transformed = self.transform()

        assert transformed[OutputSchema.process_identifier].iloc[0] == "123"

    def test_transform_row_hash(self):
        transformed = self.transform()

        assert transformed[OutputSchema.row_hash].iloc[0] == "d99968d4e376a6da7660d6a605bc9d4d"
