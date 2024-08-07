import pytest

from dbt_opiner.dbt_artifacts import DbtNode
from dbt_opiner.opinions import O002


@pytest.mark.parametrize(
    "mock_sqlfilehandler, config, expected_passed",
    [
        pytest.param(
            (DbtNode({"description": "Some description with keyword"})),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            True,
            id="model with description with keyword",
        ),
        pytest.param(
            (DbtNode({"description": "Some description without"})),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            False,
            id="model with description without keyword",
        ),
        pytest.param(
            (DbtNode({"description": "Some description"})),
            {},
            True,
            id="No keywords config defined",
        ),
        pytest.param(
            (DbtNode({})),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            True,
            id="model without description",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_C002(mock_sqlfilehandler, config, expected_passed):
    opinion = O002(config)
    result = opinion.check_opinion(mock_sqlfilehandler)
    if result:
        assert result.passed == expected_passed
    else:
        assert expected_passed  # If model has no description or keywords are not defined result should be None
