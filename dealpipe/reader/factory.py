from typing import Tuple, Type

from dealpipe.reader.formats import FORMATS
from dealpipe.reader.reader import Reader


class UnsupportedFormat(Exception):
    """Unsupported format"""


class ReaderFactory:
    def __init__(self, formats: Tuple[Tuple[str, Type[Reader]], ...]):
        self._creators = dict(formats)

    def register_format(self, format: str, creator: type[Reader]):
        self._creators[format] = creator

    def get_reader(self, format: str):
        creator = self._creators.get(format)

        if not creator:
            raise UnsupportedFormat(format)

        return creator()


factory = ReaderFactory(FORMATS)
