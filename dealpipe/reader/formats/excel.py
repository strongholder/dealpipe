from typing import Callable, Dict, List, Optional

from pandas import DataFrame, read_excel

from dealpipe.reader.reader import Reader


class ExcelReader(Reader):
    def read(self, file: str, converters: Optional[Dict[str, Callable]] = None) -> List[DataFrame]:
        sheets = read_excel(file, converters=converters, sheet_name=None)

        return list(sheets.values())
