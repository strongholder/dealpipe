from dealpipe.reader.formats.csv import CsvReader
from dealpipe.reader.formats.excel import ExcelReader
from dealpipe.reader.formats.parquet import ParquetReader
from dealpipe.reader.formats.yaml import YamlReader

FORMATS = (
    ("csv", CsvReader),
    ("excel", ExcelReader),
    ("parquet", ParquetReader),
    ("yaml", YamlReader),
)

__all__ = ["FORMATS"]
