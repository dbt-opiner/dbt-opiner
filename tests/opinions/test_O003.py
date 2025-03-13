import pytest

from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import O003


@pytest.mark.parametrize(
    "node, expected_passed",
    [
        pytest.param(
            DbtModel(
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
            [True],
            id="All columns have descriptions",
        ),
        pytest.param(
            DbtModel(
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
            ),
            [False],
            id="One columns don't have a description",
        ),
        pytest.param(
            DbtModel(
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
            ),
            [False],
            id="One columns has empty description",
        ),
        pytest.param(
            DbtModel(
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
            [False],
            id="Missing column identified by ast_extracted_columns",
        ),
        pytest.param(
            DbtModel(
                {
                    "resource_type": "model",
                }
            ),
            [False],
            id="No column keys",
        ),
    ],
)
def test_sql_C003(node, mock_sqlfilehandler, expected_passed):
    mock_sqlfilehandler.dbt_node = node
    opinion = O003()
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "nodes, expected_passed",
    [
        pytest.param(
            [
                DbtModel(
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
                DbtModel(
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
            ],
            [True, False],
            id="One model with columns with descriptions, one with Missing column identified by ast_extracted_columns",
        )
    ],
)
def test_yaml_O003(nodes, mock_yamlfilehandler, expected_passed):
    mock_yamlfilehandler.dbt_nodes = nodes
    opinion = O003()
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
