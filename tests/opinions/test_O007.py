import pytest

from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import O007


@pytest.mark.parametrize(
    "node, expected_passed",
    [
        pytest.param(
            DbtModel(
                {
                    "resource_type": "model",
                    "columns": {
                        "column_1": {"name": "column_1", "description": "Description"},
                        "column_2": {"name": "column_2", "description": "Description"},
                    },
                    "compiled_code": "select column_1, column_2 from dim_customers",
                }
            ),
            [True],
            id="All columns match between YAML and SQL",
        ),
        pytest.param(
            DbtModel(
                {
                    "resource_type": "model",
                    "columns": {
                        "column_1": {"name": "column_1", "description": "Description"},
                        "column_2": {"name": "column_2", "description": "Description"},
                        "extra_column": {
                            "name": "extra_column",
                            "description": "Extra",
                        },
                    },
                    "compiled_code": "select column_1, column_2 from dim_customers",
                }
            ),
            [False],
            id="Unnecessary columns in YAML",
        ),
        pytest.param(
            DbtModel(
                {
                    "resource_type": "model",
                    "columns": {
                        "column_1": {"name": "column_1", "description": "Description"},
                    },
                    "compiled_code": "select column_1, column_2 from dim_customers",
                }
            ),
            [True],
            id="No unnecessary columns in YAML.",
        ),
    ],
)
def test_sql_O007(node, mock_sqlfilehandler, expected_passed):
    mock_sqlfilehandler.dbt_node = node
    opinion = O007()
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
                            "extra_column": {
                                "name": "extra_column",
                                "description": "Extra Description",
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
                        },
                        "compiled_code": "select column_1, column_2 from dim_customers",
                    }
                ),
            ],
            [True, False, True],
            id="One model with matched columns, one with unnecessary columns, one with no unnecessary columns",
        ),
    ],
)
def test_yaml_O007(nodes, mock_yamlfilehandler, expected_passed):
    mock_yamlfilehandler.dbt_nodes = nodes

    opinion = O007()
    results = opinion.check_opinion(mock_yamlfilehandler)

    assert [result.passed for result in results] == expected_passed
