from typing import Callable, Dict, List, Optional

from pandas import DataFrame, read_csv

from dealpipe.reader.reader import Reader


class CsvReader(Reader):
    def read(self, file: str, converters: Optional[Dict[str, Callable]] = None) -> List[DataFrame]:
        return [read_csv(file, converters=converters)]
