import json
import os
from unittest import mock

import pytest
import yaml
from _pytest.logging import LogCaptureFixture
from loguru import logger

from dbt_opiner import config_singleton
from dbt_opiner import dbt
from dbt_opiner import file_handlers


# See https://loguru.readthedocs.io/en/stable/resources/migration.html#replacing-caplog-fixture-from-pytest-library
@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def mock_sqlfilehandler():
    mock_instance = mock.MagicMock()
    mock_instance.type = ".sql"
    mock_instance.__class__ = file_handlers.SqlFileHandler
    yield mock_instance


@pytest.fixture
def mock_yamlfilehandler():
    mock_instance = mock.MagicMock()
    mock_instance.type = ".yaml"
    mock_instance.__class__ = file_handlers.YamlFileHandler
    yield mock_instance


@pytest.fixture(autouse=True)
def reset_singletons():
    config_singleton.ConfigSingleton._instance = None


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
    |   ├── .dbt-opiner.yaml
    │   └── custom_opinions
    │       ├── C001.py
    │       ├── C002.py
    ├── dbt_project
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    |   ├── macros
    |   |   └── macro.sql
    │   ├── models
    │   │   └── test
    │   │       ├── model
    |   |           ├── model.sql
    │   │           ├── model.md
    │   │           └── _model__models.yaml
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
        ["dbt-opiner", "custom_opinions"],
        ["dbt_project", "macros"],
        ["dbt_project", "models", "test", "model"],
        ["dbt_project", "models", "test", "model_2"],
        ["dbt_project", "target"],
        ["dbt_project", "dbt_packages", "package", "macros"],
        ["dbt_project", ".venv", "dbt_project", "models", "test"],
    ]
    for directory in directories_to_create:
        temp_empty_git_repo.joinpath(*directory).mkdir(parents=True)

    # File paths and contents
    files = [
        [
            ["dbt-opiner", "custom_opinions", "C001.py"],
            (
                "from dbt_opiner.opinions.base_opinion import BaseOpinion\n"
                "from dbt_opiner.linter import OpinionSeverity\n"
                "class C001(BaseOpinion):\n"
                "    required_dependencies=['some_pypi_package']\n"
                "    def __init__(self, **kwargs):\n"
                "        super().__init__(code='C001', description='', severity=OpinionSeverity.SHOULD)\n"
                "    def _eval(self, file):\n"
                "        pass\n"
            ),
        ],
        [
            ["dbt-opiner", "custom_opinions", "C002.py"],
            (
                "from dbt_opiner.opinions.base_opinion import BaseOpinion\n"
                "from dbt_opiner.linter import OpinionSeverity\n"
                "class C002(BaseOpinion):\n"
                "    def __init__(self, **kwargs):\n"
                "        super().__init__(code='C002', description='', severity=OpinionSeverity.SHOULD)\n"
                "    def _eval(self, file):\n"
                "        pass\n"
            ),
        ],
        [
            ["dbt-opiner", ".dbt-opiner.yaml"],
            {},
        ],
        [["dbt_project", "dbt_project.yml"], {"name": "project"}],
        [
            ["dbt_project", "profiles.yml"],
            {"config": {"send_anonymous_usage_stats": False}},
        ],
        [
            ["dbt_project", "macros", "my_macro.sql"],
            "{% macro my_macro() %} select id, value from table {% endmacro %}",
        ],
        [
            ["dbt_project", "models", "test", "model", "model.sql"],
            "select id, value from table",
        ],
        [
            ["dbt_project", "models", "test", "model", "_model__models.yaml"],
            {
                "version": 2,
                "models": [
                    {
                        "name": "model",
                        "compiled_code": "select id, value from table",
                        "columns": [
                            {"name": "id", "description": "id"},
                            {"name": "value", "description": "value"},
                        ],
                    }
                ],
            },
        ],
        [
            ["dbt_project", "models", "test", "model", "model.md"],
            "{% docs id %} Id of the table {% enddocs %}",
        ],
        [
            ["dbt_project", "target", "manifest.json"],
            {
                "nodes": {
                    "model.project.model": {
                        "database": "project",
                        "resource_type": "model",
                        "schema": "test",
                        "description": "model",
                        "name": "model",
                        "alias": "fct_model",
                        "compiled_code": "select id, value from table",
                        "original_file_path": "test/model/model.sql",
                        "patch_path": "dbt_project://models/test/model/_model__models.yaml",
                        "config": {"unique_key": "pk"},
                        "columns": {
                            "id": {"description": "id"},
                            "value": {"description": "value"},
                        },
                    }
                },
                "macros": {
                    "macro.project.my_macro": {
                        "name": "my_macro",
                        "resource_type": "macro",
                        "original_file_path": "macros/my_macro.sql",
                    }
                },
                "sources": {
                    "source.this_project.dataset.table": {
                        "database": "my_db",
                        "schema": "dataset",
                        "name": "table",
                        "resource_type": "source",
                        "original_file_path": "models/staging/dataset/sources.yml",
                    }
                },
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
                        "compiled_code": "select id, value from table",
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


@pytest.fixture
def dbt_project(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    dbt_project_path = temp_complete_git_repo / "dbt_project" / "dbt_project.yml"
    dbt_project = dbt.DbtProject(dbt_project_path, all_files=True)
    return dbt_project
