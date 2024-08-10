import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O001


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode({"description": "Some description"})
            ),  # This is a tuple because pytest expects a tuple for each set of parameters.
            True,
            id="model with description",
        ),
        pytest.param(
            (DbtNode({"description": ""})),
            False,
            id="model with empty description",
        ),
        pytest.param((DbtNode({})), False, id="model with no description"),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_C001(mock_sqlfilehandler, expected_passed):
    opinion = O001()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed
