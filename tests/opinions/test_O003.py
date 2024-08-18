import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O003


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {
                                "name": "column_1",
                                "description": "Description",
                            },
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2 from dim_customers",
                    }
                )
            ),
            [True],
            id="All columns have descriptions",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {"name": "column_1"},
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2 from dim_customers",
                    }
                )
            ),
            [False],
            id="One columns don't have a description",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {"name": "column_1", "description": ""},
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2 from dim_customers",
                    }
                )
            ),
            [False],
            id="One columns has empty description",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {
                                "name": "column_1",
                                "description": "Description",
                            },
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2, column_3 from dim_customers",
                    }
                )
            ),
            [False],
            id="Missing column identified by ast_extracted_columns",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                )
            ),
            [False],
            id="No column keys",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_sql_C003(mock_sqlfilehandler, expected_passed):
    opinion = O003()
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "mock_yamlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {
                                "name": "column_1",
                                "description": "Description",
                            },
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2 from dim_customers",
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "column_1": {
                                "name": "column_1",
                                "description": "Description",
                            },
                            "column_2": {
                                "name": "column_2",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select column_1, column_2, column_3 from dim_customers",
                    }
                ),
            ),
            [True, False],
            id="One model with columns with descriptions, one with Missing column identified by ast_extracted_columns",
        )
    ],
    indirect=["mock_yamlfilehandler"],
)
def test_yaml_C003(mock_yamlfilehandler, expected_passed):
    opinion = O003()
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
