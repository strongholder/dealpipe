from importlib.resources import files
from unittest import TestCase
from unittest.mock import Mock, call

from pandas.core.frame import DataFrame

from dealpipe.reader.formats.excel import ExcelReader

SHEET1 = """
A,B,C,D,E
1,1.2322,string,2021-01-01,Yes
12,2.1323,string,2021-02-01,No
"""

SHEET2 = """
F,G
1,2
3,4
"""


class TestExcelReader(TestCase):
    def setUp(self):
        resource_path = files("dealpipe.tests") / "resources"
        self.xls_file_path = resource_path / "test.xls"
        self.xlsx_file_path = resource_path / "test.xlsx"

    def test_read_xlsx(self):
        reader = ExcelReader()

        sheets = reader.read(str(self.xlsx_file_path))
        output_df1, output_df2 = sheets

        assert isinstance(output_df1, DataFrame)
        assert output_df1.to_csv(index=False).strip() == SHEET1.strip()

        assert isinstance(output_df2, DataFrame)
        assert output_df2.to_csv(index=False).strip() == SHEET2.strip()

    def test_read_xls(self):
        reader = ExcelReader()

        sheets = reader.read(str(self.xls_file_path))
        output_df1, output_df2 = sheets

        assert isinstance(output_df1, DataFrame)
        assert output_df1.to_csv(index=False).strip() == SHEET1.strip()

        assert isinstance(output_df2, DataFrame)
        assert output_df2.to_csv(index=False).strip() == SHEET2.strip()

    def test_read_with_converters(self):
        reader = ExcelReader()
        converters = {"A": Mock(return_value=1), "E": Mock(return_value=True)}

        output_df = reader.read(str(self.xlsx_file_path), converters=converters)[0]

        converters["A"].assert_has_calls((call(1), call(12)))
        converters["E"].assert_has_calls((call("Yes"), call("No")))
        assert output_df["A"].iloc[0] == 1
        assert output_df["E"].iloc[0]
