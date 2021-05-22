from importlib.resources import files
from unittest import TestCase, mock

from dealpipe.reader.mime import detect_format, is_excel


class TestMime(TestCase):
    def setUp(self):
        self.resource_path = files("dealpipe.tests") / "resources"

    def test_is_excel_xls(self):
        file = str(self.resource_path / "test.xls")

        assert is_excel(file)

    def test_is_excel_xlsx(self):
        file = str(self.resource_path / "test.xlsx")

        assert is_excel(file)

    def test_is_excel_csv(self):
        file = str(self.resource_path / "test.csv")

        assert not is_excel(file)

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_xls(self, _is_excel):
        _is_excel.return_value = True
        file = "test_file.xlsx"

        format = detect_format(file)

        _is_excel.assert_called_with(file)
        assert format == "excel"

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_xlsx(self, _is_excel):
        _is_excel.return_value = True
        file = "test_file.xlsx"

        format = detect_format(file)

        _is_excel.assert_called_with(file)
        assert format == "excel"

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_csv(self, _is_excel):
        file = "test_file.csv"

        format = detect_format(file)

        _is_excel.assert_not_called()
        assert format == "csv"

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_yaml(self, _is_excel):
        file = "test_file.yaml"

        format = detect_format(file)

        _is_excel.assert_not_called()
        assert format == "yaml"

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_parquet(self, _is_excel):
        file = "test_file.parquet.gz"

        format = detect_format(file)

        _is_excel.assert_not_called()
        assert format == "parquet"

    @mock.patch("dealpipe.reader.mime.is_excel")
    def test_detect_format_unknown(self, _is_excel):
        _is_excel.return_value = False
        file = "test_file.unknown"

        format = detect_format(file)

        _is_excel.assert_called_with(file)
        assert format == "unknown"
