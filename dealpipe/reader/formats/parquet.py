from typing import Callable, Dict, List, Optional

from pandas import DataFrame, read_parquet

from dealpipe.reader.reader import Reader


class ParquetReader(Reader):
    def read(self, file: str, converters: Optional[Dict[str, Callable]] = None) -> List[DataFrame]:
        # converters is not used with parquet files because they already have a schema specified
        return [read_parquet(file)]
