import pytest

from dbt_opiner.dbt_artifacts import DbtNode
from dbt_opiner.opinions import O004


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (DbtNode({"config": {"unique_key": "column_1"}})),
            True,
            id="Model has a unique key",
        ),
        pytest.param(
            (DbtNode({"config": {}})),
            False,
            id="Model doesn't have a unique key",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_C004(mock_sqlfilehandler, expected_passed):
    opinion = O004()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed
