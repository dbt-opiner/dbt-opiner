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
    git_file = tmp_path / ".git"
    git_file.touch()
    return tmp_path


@pytest.fixture
def temp_complete_git_repo(temp_empty_git_repo):
    """Create a complete git repo that can be used for testing.

    It has the following structure:
    temp_empty_git_repo
    ├── dbt-opiner
    │   └── .dbt-opiner.yaml
    ├── dbt_project
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   ├── models
    │   │   └── test
    │   │       ├── model.sql
    │   │       ├── model.md
    │   │       └── _model__models.yaml
    │   ├── target
    │   |    └── manifest.json
    |   └── dbt_packages
    |   |    └── package
    |   |        ├── dbt_project.yml
    |   |        └── macros
    |   |            └── macro.sql
    │   └── .venv
    │        └── dbt_project
    |             └──dbt_project.yml
    |             └── models
    |                 └── test
    |                     ├── model.sql
    |                     ├── model.md
    |                     └── _model__models.yaml
    └── .git

    Returns: The root path (temp_empty_git_repo)
    """
    # Create empty file structure
    directories_to_create = [
        ["dbt-opiner"],
        ["dbt_project", "models", "test"],
        ["dbt_project", "target"],
        ["dbt_project", "dbt_packages", "package", "macros"],
        ["dbt_project", ".venv", "dbt_project", "models", "test"],
    ]
    for directory in directories_to_create:
        temp_empty_git_repo.joinpath(*directory).mkdir(parents=True)

    # File paths and contents
    files = [
        [["dbt-opiner", ".dbt-opiner.yaml"], {"config": "test"}],
        [["dbt_project", "dbt_project.yml"], {"name": "project"}],
        [["dbt_project", "profiles.yml"], {}],
        [["dbt_project", "models", "test", "model.sql"], "select id, value from table"],
        [
            ["dbt_project", "models", "test", "_model__models.yaml"],
            {
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
            },
        ],
        [
            ["dbt_project", "models", "test", "model.md"],
            "{% docs id %} Id of the table {% enddocs %}",
        ],
        [
            ["dbt_project", "target", "manifest.json"],
            {
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
            },
        ],
        [
            ["dbt_project", "dbt_packages", "package", "dbt_project.yml"],
            {"name": "package"},
        ],
        [
            ["dbt_project", "dbt_packages", "package", "macros", "macro.sql"],
            "select id, value from table",
        ],
        [
            ["dbt_project", ".venv", "dbt_project", "dbt_project.yml"],
            {"name": "project"},
        ],
        [
            ["dbt_project", ".venv", "dbt_project", "models", "test", "model.sql"],
            "select id, value from table",
        ],
        [
            [
                "dbt_project",
                ".venv",
                "dbt_project",
                "models",
                "test",
                "_model__models.yaml",
            ],
            {
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
            },
        ],
        [
            ["dbt_project", ".venv", "dbt_project", "models", "test", "model.md"],
            "{% docs id %} Id of the table {% enddocs %}",
        ],
    ]
    for file_path, content in files:
        path = temp_empty_git_repo.joinpath(*file_path)
        with open(temp_empty_git_repo.joinpath(*file_path), "w") as f:
            if path.suffix in [".yaml", ".yml"]:
                yaml.dump(content, f)
            elif path.suffix == ".json":
                json.dump(content, f)
            else:
                f.write(content)

    return temp_empty_git_repo
