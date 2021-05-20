import unittest
import warnings

import pandas as pd
from dagster import AssetMaterialization, DagsterEventType, ExperimentalWarning, SolidExecutionResult, execute_pipeline

from dealpipe.pipelines.process_deals import (
    INVALID_CSV_PRESET,
    INVALID_XLSX_PRESET,
    VALID_CSV_PRESET,
    VALID_XLSX_PRESET,
    process_deals,
)


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

        assert pipeline_result.success
        assert isinstance(solid_result, SolidExecutionResult)
        assert solid_result.output_value() is None
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

        assert pipeline_result.success
        assert isinstance(solid_result, SolidExecutionResult)
        assert solid_result.output_value() is None
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
