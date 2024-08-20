import json
from unittest.mock import patch

import pytest
import yaml

from dbt_opiner.config_singleton import ConfigSingleton


@pytest.fixture
def mock_sqlfilehandler():
    with patch("dbt_opiner.file_handlers.SqlFileHandler") as MockClass:
        MockClass.type = ".sql"
        yield MockClass


@pytest.fixture
def mock_yamlfilehandler():
    with patch("dbt_opiner.file_handlers.YamlFileHandler") as MockClass:
        MockClass.type = ".yaml"
        yield MockClass


@pytest.fixture(autouse=True)
def reset_singletons():
    ConfigSingleton._instance = None


@pytest.fixture
def temp_empty_git_repo(tmp_path):
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    return tmp_path


@pytest.fixture
def temp_complete_git_repo(temp_empty_git_repo):
    # Create the dbt-opiner directory and config
    dbt_opiner_dir = temp_empty_git_repo / "dbt-opiner"
    dbt_opiner_dir.mkdir()
    dbt_opiner_file = dbt_opiner_dir / ".dbt-opiner.yaml"
    dbt_opiner_file.touch()
    config = {"config": "test"}
    with open(dbt_opiner_file, "w") as f:
        yaml.dump(config, f)
    # Create a dbt directory
    dbt_dir = temp_empty_git_repo / "dbt_project"
    dbt_dir.mkdir()
    dbt_project_file = dbt_dir / "dbt_project.yml"
    dbt_project_file.touch()
    project = {
        "name": "project",
    }
    with open(dbt_project_file, "w") as f:
        yaml.dump(project, f)
    dbt_profiles_file = dbt_dir / "profiles.yml"
    dbt_profiles_file.touch()
    # Create a sql in models
    models_dir = dbt_dir / "models" / "test"
    models_dir.mkdir(parents=True)
    sql_file = models_dir / "model.sql"
    sql_file.touch()
    with open(sql_file, "w") as f:
        f.write("select id, value from table")
    # Create a yaml in models
    yaml_file = models_dir / "_model__models.yaml"
    yaml_file.touch()
    yml_dict = {
        "version": 2,
        "models": [
            {
                "name": "model",
                "columns": [
                    {"name": "id", "description": "id"},
                    {"name": "value", "description": "value"},
                ],
            }
        ],
    }
    with open(yaml_file, "w") as f:
        yaml.dump(yml_dict, f)
    # Create .md file in models
    md_file = models_dir / "model.md"
    md_file.touch()
    with open(md_file, "w") as f:
        f.write("{% docs id %} Id of the table {% enddocs %}")
    # Add target and manifest file
    target_dir = dbt_dir / "target"
    target_dir.mkdir()
    manifest_dir = target_dir / "manifest.json"
    manifest_dir.touch()
    manifest_dict = {
        "nodes": {
            "model.project.model": {
                "database": "project",
                "schema": "test",
                "name": "model",
                "alias": "model",
                "resource_type": "model",
                "compiled_code": "",
                "original_file_path": "",
                "patch_path": "",
            }
        },
        "macros": {},
    }
    with open(manifest_dir, "w") as f:
        json.dump(manifest_dict, f)

    return temp_empty_git_repo
