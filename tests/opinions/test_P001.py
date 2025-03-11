import pytest

from dbt_opiner.dbt import DbtModelNode
from dbt_opiner.opinions import P001


@pytest.mark.parametrize(
    "node, is_policy_tag, expected_passed",
    [
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "columns": {
                        "user_id": {
                            "name": "user_id",
                            "description": "Description",
                            "tags": ["some_tag", "some other tag"],
                        },
                        "column_1": {
                            "name": "column_1",
                            "description": "Description",
                        },
                    },
                    "compiled_code": "select user_id, column_1 from dim_customers",
                }
            ),
            False,
            [True],
            id="All PII columns have a tag",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "columns": {
                        "user_id": {"name": "user_id", "description": "Description"},
                        "column_1": {
                            "name": "column_1",
                            "description": "Description",
                        },
                    },
                    "compiled_code": "select user_id, column_1 from dim_customers",
                }
            ),
            False,
            [False],
            id="PII columns don't have a tag",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "columns": {
                        "user_id": {
                            "name": "user_id",
                            "description": "Description",
                            "tags": ["some_tag"],
                        },
                        "column_1": {
                            "name": "column_1",
                            "description": "Description",
                        },
                    },
                    "compiled_code": "select user_id, column_1, pii_column from dim_customers",
                }
            ),
            False,
            [False],
            id="Undocumented PII columns doesn't have a policy tag",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "compiled_code": "select user_id, column_1, pii_column from dim_customers",
                }
            ),
            False,
            [],
            id="No columns are defined.",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "columns": {
                        "user_id": {
                            "name": "user_id",
                            "description": "Description",
                            "policy_tags": ["some_tag"],
                        },
                        "column_1": {
                            "name": "column_1",
                            "description": "Description",
                        },
                    },
                    "compiled_code": "select user_id, column_1 from dim_customers",
                }
            ),
            True,
            [True],
            id="All PII columns have a policy tag",
        ),
    ],
)
def test_sql_P001(node, is_policy_tag, mock_sqlfilehandler, expected_passed):
    config = {
        "opinions_config": {
            "extra_opinions_config": {
                "P001": {
                    "policy_tag": is_policy_tag,
                    "pii_columns": {
                        "user_id": ["some_tag"],
                        "pii_column": ["some_tag"],
                    },
                }
            }
        }
    }
    mock_sqlfilehandler.dbt_node = node
    opinion = P001(config)
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert [result.passed for result in results] == expected_passed


@pytest.mark.parametrize(
    "nodes, is_policy_tag, expected_passed",
    [
        pytest.param(
            [
                DbtModelNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "user_id": {
                                "name": "user_id",
                                "description": "Description",
                                "tags": ["some_tag"],
                            },
                            "column_1": {
                                "name": "column_1",
                                "description": "Description",
                            },
                        },
                        "compiled_code": "select user_id, column_1 from dim_customers",
                    }
                ),
                DbtModelNode(
                    {
                        "resource_type": "model",
                        "columns": {
                            "user_id": {
                                "name": "user_id",
                                "description": "Description",
                                "tags": ["some_tag"],
                            }
                        },
                        "compiled_code": "select user_id, column_1, pii_column from dim_customers",
                    }
                ),
            ],
            False,
            [True, False],
            id="One model correct, one incorrect",
        )
    ],
)
def test_yaml_P001(nodes, is_policy_tag, mock_yamlfilehandler, expected_passed):
    config = {
        "opinions_config": {
            "extra_opinions_config": {
                "P001": {
                    "policy_tag": is_policy_tag,
                    "pii_columns": {
                        "user_id": ["some_tag"],
                        "pii_column": ["some_tag"],
                    },
                }
            }
        }
    }
    mock_yamlfilehandler.dbt_nodes = nodes
    opinion = P001(config)
    results = opinion.check_opinion(mock_yamlfilehandler)
    assert [result.passed for result in results] == expected_passed


def test_not_configured_P001(caplog, mock_sqlfilehandler):
    mock_sqlfilehandler.dbt_node = {}
    opinion = P001({})
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert results == []
    assert "No pii_columns configured for P001. Skipping." in caplog.text
