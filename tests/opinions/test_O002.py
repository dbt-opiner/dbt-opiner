import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O002


@pytest.mark.parametrize(
    "mock_sqlfilehandler, config, expected_passed",
    [
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "Some description with keyword",
                    }
                )
            ),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            True,
            id="model with description with keyword",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "description": "Some description without",
                    }
                )
            ),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            False,
            id="model with description without keyword",
        ),
        pytest.param(
            (DbtNode({"resource_type": "model", "description": "Some description"})),
            {},
            True,
            id="No keywords config defined",
        ),
        pytest.param(
            (DbtNode({"resource_type": "model"})),
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            True,
            id="model without description",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test__O002(mock_sqlfilehandler, config, expected_passed):
    opinion = O002(config)
    result = opinion.check_opinion(mock_sqlfilehandler)
    if result:
        assert result.passed == expected_passed
    else:
        assert expected_passed  # If model has no description or keywords are not defined result should be None


@pytest.mark.parametrize(
    "mock_yamlfilehandler, config, expected_passed",
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
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
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
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
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
            {"sql": {"opinions_config": {"O002_keywords": ["keyword"]}}},
            [True],  # Second node returns None so no result expected
            id="Two models one with description with keyword, another one without description",
        ),
    ],
    indirect=["mock_yamlfilehandler"],
)
def test_sql_O002(mock_yamlfilehandler, config, expected_passed):
    opinion = O002(config)
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed
