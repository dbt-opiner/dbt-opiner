import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import L001


@pytest.mark.parametrize(
    "node, raw_sql, expected_passed",
    [
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "fct_model",
                    "schema": "facts",
                }
            ),
            "select * from {{ source('source') }}",
            False,
            id="fact model uses source macro",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "fct_model",
                    "schema": "facts",
                }
            ),
            "select * from {{source\n('source')}}",
            False,
            id="fact model uses source macro (with weird formatting)",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "fct_model",
                    "schema": "facts",
                }
            ),
            "select * from {{ ref('source') }}",
            True,
            id="fact model doesn't source macro",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "stage_model",
                    "schema": "ci_schema",
                }
            ),
            "select * from {{ source('source') }}",
            None,
            id="Staging model (by prefix) uses source macro",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "stage",
                }
            ),
            "select * from {{ source('source') }}",
            None,
            id="Staging model (by schema) uses source macro",
        ),
    ],
)
def test_L001(node, mock_sqlfilehandler, raw_sql, expected_passed):
    config = {
        "opinions_config": {
            "extra_opinions_config": {
                "L001": {"staging_schema": "stage", "staging_prefix": "stage_"}
            }
        }
    }

    mock_sqlfilehandler.dbt_node = node
    mock_sqlfilehandler.content = raw_sql
    opinion = L001(config)
    result = opinion.check_opinion(mock_sqlfilehandler)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None
