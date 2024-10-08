import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import D001

config_dict = {
    "opinions_config": {"extra_opinions_config": {"D001": {"max_n_allowed": 2}}}
}


@pytest.mark.parametrize(
    "nodes, config, expected_passed",
    [
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
            ],
            {},
            True,
            id="One model, default max_n_allowed",
        ),
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
            ],
            {},
            False,
            id="two models, default max_n_allowed",
        ),
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
            ],
            config_dict,
            True,
            id="two models, max_n_allowed=2",
        ),
        pytest.param(
            [
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "model",
                    }
                ),
                DbtNode(
                    {
                        "resource_type": "source",
                    }
                ),
            ],
            config_dict,
            False,
            id="three models, max_n_allowed=2",
        ),
    ],
)
def test_D001(mock_yamlfilehandler, nodes, config, expected_passed):
    opinion = D001(config)
    mock_yamlfilehandler.dbt_nodes = nodes
    result = opinion.check_opinion(mock_yamlfilehandler)
    assert result.passed == expected_passed
