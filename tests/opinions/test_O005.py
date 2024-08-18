import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O005


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (DbtNode({"resource_type": "model", "config": {"unique_key": "column_1"}})),
            True,
            id="Model has a unique key",
        ),
        pytest.param(
            (DbtNode({"resource_type": "model", "config": {}})),
            False,
            id="Model doesn't have a unique key",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_sql_C005(mock_sqlfilehandler, expected_passed):
    opinion = O005()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed


def test_yaml_C005(mock_yamlfilehandler):
    opinion = O005()
    result = opinion.check_opinion(mock_yamlfilehandler)
    assert result is None
