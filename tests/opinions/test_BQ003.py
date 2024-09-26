import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import BQ003


@pytest.mark.parametrize(
    "node, config, expected_passed",
    [
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "description": "partition cluster",
                    "config": {"materialized": "view"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            [True],
            id="model with description with keywords",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "description": "no keys",
                    "config": {"materialized": "view"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            [False],
            id="model without keywords",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "view"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            [],
            id="model without description",
        ),
        pytest.param(
            DbtNode({}),
            {"sqlglot_dialect": "other"},
            [],
            id="not bq dialect",
        ),
    ],
)
def test_sql_BQ003(node, config, expected_passed, mock_sqlfilehandler):
    mock_sqlfilehandler.dbt_node = node
    opinion = BQ003(config)
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "nodes, config, expected_passed",
    [
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "partition cluster",
                        "config": {"materialized": "view"},
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "partition cluster",
                        "config": {"materialized": "view"},
                    }
                ),
            ],
            {"sqlglot_dialect": "bigquery"},
            [True, True],
            id="two models with keywords",
        ),
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "Some description",
                        "config": {"materialized": "view"},
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "partition cluster",
                        "config": {"materialized": "view"},
                    }
                ),
            ],
            {"sqlglot_dialect": "bigquery"},
            [False, True],
            id="two models one with keywords, one not",
        ),
    ],
)
def test_yaml_BQ003(nodes, config, expected_passed, mock_yamlfilehandler):
    mock_yamlfilehandler.dbt_nodes = nodes
    opinion = BQ003(config)
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
