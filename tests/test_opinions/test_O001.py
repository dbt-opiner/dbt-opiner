import pytest

from dbt_opiner.dbt_artifacts import DbtNode
from dbt_opiner.opinions import O001


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (DbtNode({"description": "Some description"}), True),
            True,
            id="model with description",
        ),
        pytest.param(
            (DbtNode({"description": ""}), False),
            False,
            id="model with empty description",
        ),
        pytest.param((DbtNode({}), False), False, id="model with no description"),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_C001(mock_sqlfilehandler, expected_passed):
    mock_handler, expected_passed = mock_sqlfilehandler
    opinion = O001()
    result = opinion.check_opinion(mock_handler)
    assert result.passed == expected_passed
