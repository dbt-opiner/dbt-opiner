import pytest

from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import O001


@pytest.mark.parametrize(
    "node, expected_passed",
    [
        pytest.param(
            DbtModel({"resource_type": "model", "description": "Some description"}),
            [True],
            id="model with description",
        ),
        pytest.param(
            DbtModel({"resource_type": "model", "description": ""}),
            [False],
            id="model with empty description",
        ),
        pytest.param(
            DbtModel({"resource_type": "model"}),
            [False],
            id="model with no description",
        ),
    ],
)
def test_sql_O001(node, expected_passed, mock_sqlfilehandler):
    mock_sqlfilehandler.dbt_node = node
    opinion = O001()
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "nodes, expected_passed",
    [
        pytest.param(
            [
                DbtModel({"resource_type": "model", "description": "Some description"}),
                DbtModel({"resource_type": "model", "description": "Some description"}),
            ],  #
            [True, True],
            id="two models with description",
        ),
        pytest.param(
            [
                DbtModel({"resource_type": "model", "description": "Some description"}),
                DbtModel({"resource_type": "model", "description": ""}),
            ],
            [True, False],
            id="two models one with description, one empty",
        ),
        pytest.param(
            [
                DbtModel({"resource_type": "model", "description": "Some description"}),
                DbtModel(
                    {
                        "resource_type": "model",
                    }
                ),
            ],
            [True, False],
            id="two models one with description, one without",
        ),
    ],
)
def test_yaml_O001(nodes, mock_yamlfilehandler, expected_passed):
    mock_yamlfilehandler.dbt_nodes = nodes
    opinion = O001()
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
