from pathlib import Path

from dagster import ModeDefinition, PresetDefinition, pipeline

import dealpipe
from dealpipe.solids.loader import load_deals
from dealpipe.solids.lookups import load_deals_lookup
from dealpipe.solids.transform import transform
from dealpipe.solids.validator import validate
from dealpipe.solids.writer import save_errors, save_output

MODE_DEV = ModeDefinition(name="dev", resource_defs={})
MODE_TEST = ModeDefinition(name="test", resource_defs={})
PRESET_PATH = Path(dealpipe.__file__).parent.parent / "presets"

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
INVALID_YAML_PRESET = PresetDefinition.from_files(
    "invalid_yaml_example",
    config_files=[str(PRESET_PATH / Path("invalid_yaml_preset.yaml"))],
    mode="dev",
)
VALID_YAML_PRESET = PresetDefinition.from_files(
    "valid_yaml_example",
    config_files=[str(PRESET_PATH / Path("valid_yaml_preset.yaml"))],
    mode="dev",
)


@pipeline(
    mode_defs=[MODE_DEV, MODE_TEST],
    preset_defs=[
        INVALID_XLSX_PRESET,
        VALID_XLSX_PRESET,
        INVALID_CSV_PRESET,
        VALID_CSV_PRESET,
        INVALID_YAML_PRESET,
        VALID_YAML_PRESET,
    ],
)
def process_deals():
    deals_lookup = load_deals_lookup()
    deals_df = load_deals()
    valid_df, errors_df = validate(deals_df, deals_lookup)
    save_errors(errors_df)
    output_df = transform(valid_df, deals_lookup)
    save_output(output_df)
