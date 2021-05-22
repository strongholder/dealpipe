from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

from pandas import DataFrame


class Reader(ABC):
    @abstractmethod
    def read(self, file: str, converters: Optional[Dict[str, Callable]] = None) -> List[DataFrame]:
        pass
