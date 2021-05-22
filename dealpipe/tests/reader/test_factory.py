from unittest import TestCase

from dealpipe.reader.factory import ReaderFactory, UnsupportedFormat
from dealpipe.reader.reader import Reader


class DummyReader(Reader):
    def read(self, file, converters):
        pass


FORMATS = (("dummy", DummyReader), ("test", DummyReader))


class TestReaderFactory(TestCase):
    def test_create_factory(self):
        factory = ReaderFactory(FORMATS)

        assert factory._creators == dict(FORMATS)

    def test_register_format(self):
        factory = ReaderFactory(())

        factory.register_format("dummy", DummyReader)
        factory.register_format("test", DummyReader)

        assert factory._creators == dict(FORMATS)

    def test_get_reader(self):
        factory = ReaderFactory(FORMATS)

        reader = factory.get_reader("dummy")

        assert isinstance(reader, DummyReader)

    def test_get_reader_unsupported_format(self):
        factory = ReaderFactory(())

        with self.assertRaises(UnsupportedFormat):
            factory.get_reader("unknown")
