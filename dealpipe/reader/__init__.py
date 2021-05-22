from typing import Callable, Dict, Optional

from pandas.core.frame import DataFrame

from dealpipe.reader.factory import factory
from dealpipe.reader.mime import detect_format


class InvalidSheetError(Exception):
    """Raised when someone tries to get an invalid sheet index"""


def read(file: str, converters: Optional[Dict[str, Callable]] = None, sheet: int = 0) -> DataFrame:
    format = detect_format(file)
    reader = factory.get_reader(format)
    sheets = reader.read(file, converters)

    if len(sheets) == 1:
        return sheets[0]
    elif sheet < len(sheets):
        return sheets[sheet]
    else:
        raise InvalidSheetError("Invalid sheet index")
