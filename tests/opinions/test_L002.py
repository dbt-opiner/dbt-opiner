import json
import os
from unittest.mock import MagicMock

import pytest

from dbt_opiner.dbt import DbtManifest
from dbt_opiner.dbt import DbtModelNode
from dbt_opiner.opinions import L002

config_dict = {
    "opinions_config": {
        "extra_opinions_config": {
            "L002": {
                "layer_pairs": [
                    "staging,stg selects from facts,fct",
                    "staging,stg selects from marts,mrt",
                    "facts,fct selects from marts,mrt",
                ]
            }
        }
    }
}

no_restrictions_dict = {
    "opinions_config": {
        "extra_opinions_config": {
            "L002": {"layer_pairs": ["whatever,wth selects from facts,fct"]}
        }
    }
}


@pytest.mark.parametrize(
    "node, config, expected_passed",
    [
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "staging",
                    "depends_on": {
                        "nodes": ["model.project.fct_model", "model.project.mrt_model"]
                    },
                }
            ),
            config_dict,
            False,
            id="Staging incorrectly selects from facts and marts (by schema)",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "other_schema",
                    "depends_on": {
                        "nodes": ["model.project.fct_model", "model.project.mrt_model"]
                    },
                }
            ),
            config_dict,
            False,
            id="Staging incorrectly selects from facts and marts (by prefix)",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "staging",
                    "depends_on": {"nodes": ["model.project.stg_model"]},
                }
            ),
            config_dict,
            True,
            id="Staging doesn't violate layer directionality",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "staging",
                    "depends_on": {"nodes": ["model.project.stg_model"]},
                }
            ),
            no_restrictions_dict,
            None,
            id="No restrictions for this layer.",
        ),
        pytest.param(
            DbtModelNode(
                {
                    "resource_type": "model",
                    "alias": "stg_model",
                    "schema": "staging",
                    "depends_on": {"nodes": []},
                }
            ),
            no_restrictions_dict,
            None,
            id="No selected models.",
        ),
    ],
)
def test_L002(temp_empty_git_repo, mock_sqlfilehandler, config, node, expected_passed):
    os.chdir(temp_empty_git_repo)

    manifest = {
        "nodes": {
            "model.project.fct_model": {
                "resource_type": "model",
                "schema": "facts",
                "alias": "fct_model",
            },
            "model.project.mrt_model": {
                "resource_type": "model",
                "schema": "marts",
                "alias": "mrt_model",
            },
            "model.project.stg_model": {
                "resource_type": "model",
                "schema": "staging",
                "alias": "stg_model",
            },
        }
    }
    manifest_file = temp_empty_git_repo / "target" / "manifest.json"
    manifest_file.parent.mkdir(parents=True)

    with open(manifest_file, "w") as f:
        json.dump(manifest, f)
    manifest = DbtManifest(manifest_file)

    DbtProject = MagicMock()
    mock_dbt_project = DbtProject()
    mock_dbt_project.dbt_manifest = manifest

    mock_sqlfilehandler.dbt_node = node
    mock_sqlfilehandler.parent_dbt_project = mock_dbt_project

    opinion = L002(config)
    result = opinion.check_opinion(mock_sqlfilehandler)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None


def test_L002_no_config(caplog, mock_sqlfilehandler):
    opinion = L002({})
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result is None
    assert "No layer pairs configured for L002. Skipping." in caplog.text


def test_L002_wrong_layer_pairs(caplog, mock_sqlfilehandler):
    layer_pairs = [
        "staging,stg -> facts,fct",
        "staging,stg selects from marts,mrt selects from facts,fct",
        "staging,stg,sttg selects from facts,fct",
        "staging,stg,sttg selects from facts,fct,fctt",
    ]
    config = {
        "opinions_config": {
            "extra_opinions_config": {"L002": {"layer_pairs": layer_pairs}}
        }
    }
    opinion = L002(config)
    result = opinion.check_opinion(mock_sqlfilehandler)
    for pair in layer_pairs:
        assert f"Invalid layer pair configuration: {pair}" in caplog.text
    assert result is None


def test_L002_not_sql_file(mock_yamlfilehandler):
    opinion = L002(config_dict)
    result = opinion.check_opinion(mock_yamlfilehandler)
    assert result is None
