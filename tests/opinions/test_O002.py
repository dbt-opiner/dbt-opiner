import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O002

config_dict = {
    "opinions_config": {"extra_opinions_config": {"O002": {"keywords": ["keyword"]}}}
}


@pytest.mark.parametrize(
    "node, config, expected_passed",
    [
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "description": "Some description with keyword",
                }
            ),
            config_dict,
            [True],
            id="model with description with keyword",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "description": "Some description without",
                }
            ),
            config_dict,
            [False],
            id="model with description without keyword",
        ),
        pytest.param(
            DbtNode({"resource_type": "model", "description": "Some description"}),
            {},
            True,
            id="No keywords config defined",
        ),
        pytest.param(
            DbtNode({"resource_type": "model"}),
            config_dict,
            True,
            id="model without description",
        ),
    ],
)
def test_O002(node, mock_sqlfilehandler, config, expected_passed):
    mock_sqlfilehandler.dbt_node = node
    opinion = O002(config)
    results = opinion.check_opinion(mock_sqlfilehandler)
    if results:
        assert [result.passed for result in results] == expected_passed
    else:  # If no keywords or no description it should return None or empty list so no results
        assert expected_passed


@pytest.mark.parametrize(
    "nodes, config, expected_passed",
    [
        pytest.param(
            [
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                            "description": "Some description with keyword",
                        }
                    )
                ),
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                            "description": "Some description with keyword",
                        }
                    )
                ),
            ],
            config_dict,
            [True, True],
            id="Two models with description with keyword",
        ),
        pytest.param(
            [
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                            "description": "Some description with keyword",
                        }
                    )
                ),
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                            "description": "Some description without",
                        }
                    )
                ),
            ],
            config_dict,
            [True, False],
            id="Two models one with description with keyword, another one without",
        ),
        pytest.param(
            [
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                            "description": "Some description with keyword",
                        }
                    )
                ),
                (
                    DbtNode(
                        {
                            "resource_type": "model",
                        }
                    )
                ),
            ],
            config_dict,
            [True],  # Second node returns None so no result expected
            id="Two models one with description with keyword, another one without description",
        ),
    ],
)
def test_sql_O002(nodes, mock_yamlfilehandler, config, expected_passed):
    mock_yamlfilehandler.dbt_nodes = nodes
    opinion = O002(config)
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
