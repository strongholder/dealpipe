import unittest
import warnings
from unittest import mock

import pandas as pd
from dagster import AssetMaterialization, DagsterEventType, ExperimentalWarning, execute_pipeline

from dealpipe.pipelines.process_deals import (
    INVALID_CSV_PRESET,
    INVALID_XLSX_PRESET,
    INVALID_YAML_PRESET,
    VALID_CSV_PRESET,
    VALID_XLSX_PRESET,
    VALID_YAML_PRESET,
    process_deals,
)

VALIDATE_COLUMNS = [
    "RowNo",
    "DealName",
    "D1",
    "D2",
    "D3",
    "D4",
    "D5",
    "IsActive",
    "CountryCode",
    "CurrencyCode",
    "CompanyId",
    "CompanyName",
]
VALID_OUTPUT = """
RowNo,DealName,D1,D2,D3,D4,D5,IsActive,CountryCode,CurrencyCode,CompanyId,CompanyName
0,Kindle Paperwhite,12.22322,15.33333,18.0001,9.999999,5.3,True,USA,USD,1,Amazon
1,Surface Pro,123.22,11.0,22.12222,14.2,123123.123123,True,USA,USD,2,Microsoft
2,Glass,-12.33,221.22,,5.33,11.2,True,USA,USD,3,Google
3,Airbus A320,22219387192.1293,,,,,False,BGR,BGN,4,Bulgaria Air
"""
ERROR_OUTPUT = """
schema_context,column,check,check_number,failure_case,index
Column,CompanyId,coerce_dtype('int32'),,object,
Column,DealName,not_nullable,,,8
Column,D1,not_nullable,,,1
Column,D1,not_nullable,,,8
Column,D2,is_numeric,0,invalid,8
Column,D3,is_numeric,0,invalid,8
Column,D4,is_numeric,0,invalid,8
Column,D5,is_numeric,0,invalid,8
Column,IsActive,"Must be either ""Yes"" or ""No.",0,yes,3
Column,IsActive,"Must be either ""Yes"" or ""No.",0,no,4
Column,IsActive,"Must be either ""Yes"" or ""No.",0,Y,6
Column,IsActive,"Must be either ""Yes"" or ""No.",0,N,7
Column,IsActive,"Must be either ""Yes"" or ""No.",0,invalid,8
Column,CountryCode,not_nullable,,,6
Column,CurrencyCode,not_nullable,,,5
Column,CompanyId,coerce_dtype('int32'),,object,
"""


def is_skipped(pipeline_result, solid_name):
    return pipeline_result.result_for_solid(solid_name).step_events[0].event_type == DagsterEventType.STEP_SKIPPED


@mock.patch("dealpipe.writer.write_excel")
@mock.patch("dealpipe.writer.write_parquet")
class TestProcessDealsPipeline(unittest.TestCase):
    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ExperimentalWarning)
        pd.options.mode.chained_assignment = None
        return super().setUp()

    def run_valid_test(self, preset, write_parquet, write_excel):
        pipeline_result = execute_pipeline(process_deals, preset.run_config, mode="test")
        solid_result = pipeline_result.result_for_solid("save_output")
        postprocess_solid_result = pipeline_result.result_for_solid("transform")
        output_value = postprocess_solid_result.output_value()

        assert pipeline_result.success
        assert isinstance(output_value, pd.DataFrame)
        assert output_value[VALIDATE_COLUMNS].to_csv(index=False).strip() == VALID_OUTPUT.strip()

        assert [se.event_type for se in solid_result.step_events] == [
            DagsterEventType.STEP_START,
            DagsterEventType.LOADED_INPUT,
            DagsterEventType.STEP_INPUT,
            DagsterEventType.ASSET_MATERIALIZATION,
            DagsterEventType.STEP_OUTPUT,
            DagsterEventType.HANDLED_OUTPUT,
            DagsterEventType.STEP_SUCCESS,
        ]
        materialization_event = solid_result.step_events[3]
        materialization = materialization_event.event_specific_data.materialization
        assert isinstance(materialization, AssetMaterialization)
        assert materialization.label == "output_parquet_file"
        assert materialization.metadata_entries[0].entry_data.path == "output/deals.parquet.gz"
        assert is_skipped(pipeline_result, "save_errors")
        write_parquet.assert_called_with(output_value, "output/deals.parquet.gz", "gzip")
        write_excel.assert_not_called()

    def run_invalid_test(self, preset, write_parquet, write_excel):
        pipeline_result = execute_pipeline(process_deals, preset.run_config, mode="test")
        solid_result = pipeline_result.result_for_solid("save_errors")
        error_report_solid_result = pipeline_result.result_for_solid("validate")
        output_value = error_report_solid_result.output_value("errors")

        assert pipeline_result.success
        assert isinstance(output_value, pd.DataFrame)
        assert output_value.to_csv(index=False).strip() == ERROR_OUTPUT.strip()

        assert [se.event_type for se in solid_result.step_events] == [
            DagsterEventType.STEP_START,
            DagsterEventType.LOADED_INPUT,
            DagsterEventType.STEP_INPUT,
            DagsterEventType.ASSET_MATERIALIZATION,
            DagsterEventType.STEP_OUTPUT,
            DagsterEventType.HANDLED_OUTPUT,
            DagsterEventType.STEP_SUCCESS,
        ]
        materialization_event = solid_result.step_events[3]
        materialization = materialization_event.event_specific_data.materialization
        assert isinstance(materialization, AssetMaterialization)
        assert materialization.label == "errors_excel_file"
        assert materialization.metadata_entries[0].entry_data.path == "output/deals_errors.xlsx"
        assert is_skipped(pipeline_result, "transform")
        assert is_skipped(pipeline_result, "save_output")
        write_excel.assert_called_with(output_value, "output/deals_errors.xlsx", sheet_name="Errors")
        write_parquet.assert_not_called()

    def test_valid_xlsx(self, write_parquet, write_excel):
        self.run_valid_test(VALID_XLSX_PRESET, write_parquet, write_excel)

    def test_invalid_xlsx(self, write_parquet, write_excel):
        self.run_invalid_test(INVALID_XLSX_PRESET, write_parquet, write_excel)

    def test_valid_csv(self, write_parquet, write_excel):
        self.run_valid_test(VALID_CSV_PRESET, write_parquet, write_excel)

    def test_invalid_csv(self, write_parquet, write_excel):
        self.run_invalid_test(INVALID_CSV_PRESET, write_parquet, write_excel)

    def test_valid_yaml(self, write_parquet, write_excel):
        self.run_valid_test(VALID_YAML_PRESET, write_parquet, write_excel)

    def test_invalid_yaml(self, write_parquet, write_excel):
        self.run_invalid_test(INVALID_YAML_PRESET, write_parquet, write_excel)
