from unittest import TestCase, mock

from pandas import DataFrame

from dealpipe.lookups import build_lookups

LOOKUP_DATA = {
    "CompanyId": [1, 2, 3, None],
    "CompanyName": ["A", "B", None, None],
    "Currencies": ["EUR", "USD", "EUR", None],
    "Countries": ["IRL", "USA", "IRL", None],
}


class TestLookups(TestCase):
    @mock.patch("dealpipe.reader.read")
    def test_build_lookups(self, read):
        lookup_df = DataFrame(LOOKUP_DATA)
        read.return_value = lookup_df

        lookup = build_lookups("lookups.csv")

        assert lookup == {"companies": {1: "A", 2: "B"}, "currencies": ["EUR", "USD"], "countries": ["IRL", "USA"]}
