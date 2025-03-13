import pytest

from dbt_opiner.dbt import DbtBaseNode
from dbt_opiner.opinions import BQ002


@pytest.mark.parametrize(
    "node, config, expected_passed",
    [
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "table", "cluster_by": ["column"]},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            True,
            id="table_has_clustering",
        ),
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "incremental", "cluster_by": ["column"]},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            True,
            id="incremental_model_has_clustering",
        ),
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "table"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            False,
            id="table_does_not_have_clustering",
        ),
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "incremental"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            False,
            id="incremental_model_does_not_have_clustering",
        ),
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "view"},
                }
            ),
            {"sqlglot_dialect": "bigquery"},
            None,
            id="views_are_not_evaluated",
        ),
        pytest.param(
            DbtBaseNode(
                {
                    "resource_type": "model",
                    "config": {"materialized": "table", "cluster_by": ["column"]},
                }
            ),
            {"sqlglot_dialect": "other"},
            None,
            id="not_BQ_dialect",
        ),
    ],
)
def test_M001(mock_sqlfilehandler, node, config, expected_passed):
    opinion = BQ002(config)
    mock_sqlfilehandler.dbt_node = node
    result = opinion.check_opinion(mock_sqlfilehandler)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None
