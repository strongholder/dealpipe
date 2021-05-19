from typing import Dict

import great_expectations as ge
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultMarkdownPageView


def build_deals_expectation_suite(lookup: Dict) -> ge.core.ExpectationSuite:
    return ge.core.ExpectationSuite(
        **{
            "expectation_suite_name": "default",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "DealName"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "DealName"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "D1"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D1"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D2"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_type_list",
                    "kwargs": {
                        "column": "D2",
                        "type_list": ["float", "int", "nan", "null"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D3"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_type_list",
                    "kwargs": {
                        "column": "D3",
                        "type_list": ["float", "int", "nan", "null"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D4"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_type_list",
                    "kwargs": {
                        "column": "D4",
                        "type_list": ["float", "int", "nan", "null"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D5"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_type_list",
                    "kwargs": {
                        "column": "D5",
                        "type_list": ["float", "int", "nan", "null"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "IsActive"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "IsActive"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {
                        "column": "IsActive",
                        "type_": "bool",
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "CountryCode"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "CountryCode"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "CountryCode",
                        "value_set": lookup["countries"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "CurrencyCode"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "CurrencyCode"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "CurrencyCode",
                        "value_set": lookup["currencies"],
                    },
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "CompanyId"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "CompanyId"},
                    "meta": {},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "CompanyId",
                        "value_set": list(lookup["companies"].keys()),
                    },
                    "meta": {},
                },
            ],
        }
    )


def render_validation_result_markdown(result: ExpectationSuiteValidationResult) -> str:
    renderer = ValidationResultsPageRenderer(run_info_at_end=True)
    markdown = DefaultMarkdownPageView().render(renderer.render(result))
    return markdown


def render_error_description(config: dict):
    from great_expectations.core.expectation_configuration import ExpectationConfiguration

    content_block_fn = get_renderer_impl(object_name=config["expectation_type"], renderer_type="renderer.prescriptive")
    content_block_fn = content_block_fn[1] if content_block_fn else None
    expectation_config = ExpectationConfiguration(**config)

    return str(content_block_fn(configuration=expectation_config)[0])
