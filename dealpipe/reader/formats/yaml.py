from pathlib import Path
from typing import Callable, Dict, List, Optional

from pandas import DataFrame
from yaml import load

from dealpipe.reader.reader import Reader

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class YamlReader(Reader):
    def read(self, file: str, converters: Optional[Dict[str, Callable]] = None) -> List[DataFrame]:
        table_data = load(Path(file).read_text(), Loader=Loader)

        if converters:
            self._convert_values(table_data, converters)

        return [DataFrame(table_data)]

    def _convert_values(self, data: List[Dict], converters: Dict):
        """Inplace conversion of values"""
        for row_idx, row in enumerate(data):
            for column, value in row.items():
                if column in converters:
                    data[row_idx][column] = converters[column](value)
