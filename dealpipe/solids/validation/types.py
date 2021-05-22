from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from dagster_pandas.constraints import ColumnDTypeFnConstraint
from pandas.core.dtypes.common import is_object_dtype

DealsDataFrame = create_dagster_pandas_dataframe_type(
    name="DealsDataFrame",
    columns=[
        PandasColumn.integer_column("RowNo"),
        PandasColumn.string_column("DealName", is_required=True),
        PandasColumn(name="D1", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D2", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D3", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D4", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn(name="D5", constraints=[ColumnDTypeFnConstraint(is_object_dtype)]),
        PandasColumn.boolean_column("IsActive", is_required=True),
        PandasColumn.string_column("CountryCode", is_required=True),
        PandasColumn.string_column("CurrencyCode", is_required=True),
        PandasColumn.integer_column("CompanyId", is_required=True),
        PandasColumn.string_column("CompanyName", is_required=True),
        PandasColumn.datetime_column("AsOfDate"),
        PandasColumn.string_column("ProcessIdentifier"),
        PandasColumn.string_column("RowHash"),
    ],
)
