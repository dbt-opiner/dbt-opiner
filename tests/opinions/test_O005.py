import pytest

from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import O005


@pytest.mark.parametrize(
    "node, expected_passed",
    [
        pytest.param(
            DbtModel({"resource_type": "model", "config": {"unique_key": "column_1"}}),
            True,
            id="Model has a unique key",
        ),
        pytest.param(
            DbtModel({"resource_type": "model", "config": {}}),
            False,
            id="Model doesn't have a unique key config",
        ),
        pytest.param(
            DbtModel({"resource_type": "model", "config": {"unique_key": None}}),
            False,
            id="Model doesn't have a unique key",
        ),
    ],
)
def test_sql_O005(node, mock_sqlfilehandler, expected_passed):
    mock_sqlfilehandler.dbt_node = node
    opinion = O005()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed


def test_yaml_O005(mock_yamlfilehandler):
    opinion = O005()
    result = opinion.check_opinion(mock_yamlfilehandler)
    assert result is None
