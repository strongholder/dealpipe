from pathlib import Path

from dagster import ModeDefinition, PresetDefinition, pipeline

from dealpipe.solids.loader import load_deals_file
from dealpipe.solids.lookups import load_deals_lookup
from dealpipe.solids.postprocessor import postprocess
from dealpipe.solids.preprocessor import preprocess
from dealpipe.solids.validation import generate_error_report, save_error_report, validate
from dealpipe.solids.writer import save_output

MODE_DEV = ModeDefinition(name="dev", resource_defs={})
MODE_TEST = ModeDefinition(name="test", resource_defs={})
PRESET_PATH = Path(__file__).parent.parent / Path("presets").absolute()

INVALID_XLSX_PRESET = PresetDefinition.from_files(
    "invalid_xlsx_example",
    config_files=[str(PRESET_PATH / Path("invalid_xlsx_preset.yaml"))],
    mode="dev",
)
VALID_XLSX_PRESET = PresetDefinition.from_files(
    "valid_xlsx_example",
    config_files=[str(PRESET_PATH / Path("valid_xlsx_preset.yaml"))],
    mode="dev",
)
INVALID_CSV_PRESET = PresetDefinition.from_files(
    "invalid_csv_example",
    config_files=[str(PRESET_PATH / Path("invalid_csv_preset.yaml"))],
    mode="dev",
)
VALID_CSV_PRESET = PresetDefinition.from_files(
    "valid_csv_example",
    config_files=[str(PRESET_PATH / Path("valid_csv_preset.yaml"))],
    mode="dev",
)


@pipeline(
    mode_defs=[MODE_DEV, MODE_TEST],
    preset_defs=[
        INVALID_XLSX_PRESET,
        VALID_XLSX_PRESET,
        INVALID_CSV_PRESET,
        VALID_CSV_PRESET,
    ],
)
def process_deals():
    deals_lookup = load_deals_lookup()
    preprocessed_df = preprocess(load_deals_file())
    valid, errors = validate(preprocessed_df, deals_lookup)
    error_report_df = generate_error_report(preprocessed_df, errors)
    save_error_report(error_report_df)
    output_df = postprocess(preprocessed_df, deals_lookup, valid)
    save_output(output_df)
