import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O006


@pytest.mark.parametrize(
    "node, expected_passed",
    [
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "stg_some_model",
                }
            ),
            True,
            id="model with valid prefix",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "some_model",
                }
            ),
            False,
            id="model with invalid prefix",
        ),
    ],
)
def test_O006(node, mock_sqlfilehandler, expected_passed):
    mock_sqlfilehandler.dbt_node = node
    opinion = O006({})
    result = opinion.check_opinion(mock_sqlfilehandler)
    if result:
        assert result.passed == expected_passed
