from unittest import TestCase, mock

from dealpipe.reader import InvalidSheetError, read


class TestReaderInit(TestCase):
    @mock.patch("dealpipe.reader.detect_format")
    @mock.patch("dealpipe.reader.factory")
    def test_read_return_first(self, factory, detect_format):
        """When only single sheet is available, ignore the `sheet` parameter and always return it"""

        detect_format.return_value = "excel"
        factory.get_reader().read.return_value = [1]

        result = read("dummy.csv", sheet=2)

        assert result == 1

    @mock.patch("dealpipe.reader.detect_format")
    @mock.patch("dealpipe.reader.factory")
    def test_read_return_desired_sheet(self, factory, detect_format):
        """When multiple sheets are available, return the one specified in the `sheet` parameter if within range"""

        detect_format.return_value = "excel"
        factory.get_reader().read.return_value = [1, 2, 3]

        result = read("dummy.csv", sheet=1)

        assert result == 2

    @mock.patch("dealpipe.reader.detect_format")
    @mock.patch("dealpipe.reader.factory")
    def test_read_raise_when_invalid_sheet(self, factory, detect_format):
        """When the desired `sheet` is out of range raise an exception"""

        detect_format.return_value = "excel"
        factory.get_reader().read.return_value = [1, 2, 3]

        with self.assertRaises(InvalidSheetError):
            read("dummy.csv", sheet=4)
