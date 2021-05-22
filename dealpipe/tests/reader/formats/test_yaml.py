from importlib.resources import files
from unittest import TestCase
from unittest.mock import Mock, call

from pandas.core.frame import DataFrame

from dealpipe.reader.formats.yaml import YamlReader

CONTENT = """
A,B,C,D,E
1,1.2322,string,2021-01-01,Yes
12,2.1323,string,2021-02-01,No
"""


class TestYamlReader(TestCase):
    def setUp(self):
        resource_path = files("dealpipe.tests") / "resources"
        self.test_file_path = resource_path / "test.yaml"

    def test_read(self):
        reader = YamlReader()

        sheets = reader.read(str(self.test_file_path))
        output_df = sheets[0]

        assert len(sheets) == 1
        assert isinstance(output_df, DataFrame)
        assert output_df.to_csv(index=False).strip() == CONTENT.strip()

    def test_read_with_converters(self):
        reader = YamlReader()
        converters = {"A": Mock(return_value=1), "E": Mock(return_value=True)}

        output_df = reader.read(str(self.test_file_path), converters=converters)[0]

        converters["A"].assert_has_calls((call("1"), call("12")))
        converters["E"].assert_has_calls((call("Yes"), call("No")))
        assert output_df["A"].iloc[0] == 1
        assert output_df["E"].iloc[0]
