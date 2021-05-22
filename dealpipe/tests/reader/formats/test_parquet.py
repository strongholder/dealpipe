from importlib.resources import files
from unittest import TestCase

from pandas.core.frame import DataFrame

from dealpipe.reader.formats.parquet import ParquetReader

CONTENT = """
A,B,C,D,E
1,1.2322,string,2021-01-01,Yes
12,2.1323,string,2021-02-01,No
"""


class TestParquetReader(TestCase):
    def test_read(self):
        reader = ParquetReader()
        resource_path = files("dealpipe.tests") / "resources"
        test_file_path = resource_path / "test.parquet.gz"

        sheets = reader.read(str(test_file_path))
        output_df = sheets[0]

        assert len(sheets) == 1
        assert isinstance(output_df, DataFrame)
        assert output_df.to_csv(index=False).strip() == CONTENT.strip()
