import pytest

from dbt_opiner.dbt_artifacts import DbtNode
from dbt_opiner.opinions import O003


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode(
                    {
                        "columns": {
                            "column_1": {
                                "name": "columns_1",
                                "description": "Description",
                            },
                            "column_2": {
                                "name": "columns_2",
                                "description": "Description",
                            },
                        }
                    }
                )
            ),
            True,
            id="All columns have descriptions",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "columns": {
                            "column_1": {"name": "columns_1"},
                            "column_2": {
                                "name": "columns_2",
                                "description": "Description",
                            },
                        }
                    }
                )
            ),
            False,
            id="One columns don't have a description",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "columns": {
                            "column_1": {"name": "columns_1", "description": ""},
                            "column_2": {
                                "name": "columns_2",
                                "description": "Description",
                            },
                        }
                    }
                )
            ),
            False,
            id="One columns has empty description",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_C003(mock_sqlfilehandler, expected_passed):
    opinion = O003()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed
