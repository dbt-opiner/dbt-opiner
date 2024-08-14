import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O001


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode({"resource_type": "model", "description": "Some description"})
            ),  # This is a tuple because pytest expects a tuple for each set of parameters.
            [True],
            id="model with description",
        ),
        pytest.param(
            (DbtNode({"resource_type": "model", "description": ""})),
            [False],
            id="model with empty description",
        ),
        pytest.param(
            (DbtNode({"resource_type": "model"})),
            [False],
            id="model with no description",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_sql_O001(mock_sqlfilehandler, expected_passed):
    opinion = O001()
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "mock_yamlfilehandler, expected_passed",
    [
        pytest.param(
            (
                [
                    DbtNode(
                        {"resource_type": "model", "description": "Some description"}
                    ),
                    DbtNode(
                        {"resource_type": "model", "description": "Some description"}
                    ),
                ]
            ),  #
            [True, True],
            id="two models with description",
        ),
        pytest.param(
            (
                [
                    DbtNode(
                        {"resource_type": "model", "description": "Some description"}
                    ),
                    DbtNode({"resource_type": "model", "description": ""}),
                ]
            ),
            [True, False],
            id="two models one with description, one empty",
        ),
        pytest.param(
            (
                [
                    DbtNode(
                        {"resource_type": "model", "description": "Some description"}
                    ),
                    DbtNode(
                        {
                            "resource_type": "model",
                        }
                    ),
                ]
            ),
            [True, False],
            id="two models one with description, one without",
        ),
    ],
    indirect=["mock_yamlfilehandler"],
)
def test_yaml_O001(mock_yamlfilehandler, expected_passed):
    opinion = O001()
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
