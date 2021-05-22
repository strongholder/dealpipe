import unittest
import warnings

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
VALIDATE_ERROR_COLUMNS = [x for x in VALIDATE_COLUMNS if x != "CompanyName"] + ["Errors"]
VALID_OUTPUT = """
RowNo,DealName,D1,D2,D3,D4,D5,IsActive,CountryCode,CurrencyCode,CompanyId,CompanyName
2,Kindle Paperwhite,12.22322,15.33333,18.0001,9.999999,5.3,True,USA,USD,1,Amazon
3,Surface Pro,123.22,11.0,22.12222,14.2,123123.123123,True,USA,USD,2,Microsoft
4,Glass,-12.33,221.22,NaN,5.33,11.2,True,USA,USD,3,Google
5,Airbus A320,22219387192.1293,NaN,NaN,NaN,NaN,False,BGR,BGN,4,Bulgaria Air
"""
ERROR_OUTPUT = """
RowNo,DealName,D1,D2,D3,D4,D5,IsActive,CountryCode,CurrencyCode,CompanyId,Errors
3,Kindle,,15.33333,18.0001,9.999999,5.3,False,USA,USD,1,D1 values must never be null.
7,Item,122.3314453,,,,,True,GBR,,5,CurrencyCode values must never be null.
8,Item 1,125.0,,,,,Y,,GBP,5,"IsActive values must be of type bool.
CountryCode values must never be null."
9,Item 2,125.0,,,,,N,invalid,GBP,5,"IsActive values must be of type bool.
CountryCode values must belong to this set: USA GBR BGR."
10,,,invalid,invalid,invalid,invalid,invalid,invalid,invalid,invalid,"DealName values must never be null.
D1 values must never be null.
D2 value types must belong to this set: float int nan null.
D3 value types must belong to this set: float int nan null.
D4 value types must belong to this set: float int nan null.
D5 value types must belong to this set: float int nan null.
IsActive values must be of type bool.
CountryCode values must belong to this set: USA GBR BGR.
CurrencyCode values must belong to this set: USD GBP BGN.
CompanyId values must belong to this set: 1 2 3 4 5."
"""


def is_skipped(pipeline_result, solid_name):
    return pipeline_result.result_for_solid(solid_name).step_events[0].event_type == DagsterEventType.STEP_SKIPPED


class TestProcessDealsPipeline(unittest.TestCase):
    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ExperimentalWarning)
        pd.options.mode.chained_assignment = None
        return super().setUp()

    def run_valid_test(self, preset):
        pipeline_result = execute_pipeline(process_deals, preset.run_config, mode="test")
        solid_result = pipeline_result.result_for_solid("save_output")
        postprocess_solid_result = pipeline_result.result_for_solid("postprocess")
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
        assert is_skipped(pipeline_result, "generate_error_report")
        assert is_skipped(pipeline_result, "save_error_report")

    def run_invalid_test(self, preset):
        pipeline_result = execute_pipeline(process_deals, preset.run_config, mode="test")
        solid_result = pipeline_result.result_for_solid("save_error_report")
        error_report_solid_result = pipeline_result.result_for_solid("generate_error_report")
        output_value = error_report_solid_result.output_value()

        assert pipeline_result.success
        assert isinstance(output_value, pd.DataFrame)
        assert output_value[VALIDATE_ERROR_COLUMNS].to_csv(index=False).strip() == ERROR_OUTPUT.strip()

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
        assert is_skipped(pipeline_result, "postprocess")
        assert is_skipped(pipeline_result, "save_output")

    def test_valid_xlsx(self):
        self.run_valid_test(VALID_XLSX_PRESET)

    def test_invalid_xlsx(self):
        self.run_invalid_test(INVALID_XLSX_PRESET)

    def test_valid_csv(self):
        self.run_valid_test(VALID_CSV_PRESET)

    def test_invalid_csv(self):
        self.run_invalid_test(INVALID_CSV_PRESET)

    def test_valid_yaml(self):
        self.run_valid_test(VALID_YAML_PRESET)

    def test_invalid_yaml(self):
        self.run_invalid_test(INVALID_YAML_PRESET)
