import os
from pathlib import Path

import pytest

from dbt_opiner.dbt import DbtManifest
from dbt_opiner.file_handlers import MarkdownFileHandler
from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler


@pytest.fixture
def manifest(temp_complete_git_repo):
    manifest = DbtManifest(
        temp_complete_git_repo / "dbt_project" / "target" / "manifest.json"
    )
    return manifest


# Test SqlFileHandler
def test_sql_file_handler_model(temp_complete_git_repo, manifest):
    file = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model"
        / "model.sql"
    )
    handler = SqlFileHandler(file, manifest)
    assert handler.content == "select id, value from table"
    assert handler.dbt_node.type == "model"


def test_sql_file_handler_macro(temp_complete_git_repo, manifest):
    file = temp_complete_git_repo / "dbt_project" / "macros" / "my_macro.sql"
    handler = SqlFileHandler(file, manifest)
    assert (
        handler.content
        == "{% macro my_macro() %} select id, value from table {% endmacro %}"
    )
    assert handler.dbt_node.type == "macro"


def test_get_no_qa_opinion(temp_complete_git_repo, manifest):
    file_path = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model"
        / "model.sql"
    )
    # Modify file and include no_qa_opinions
    with open(file_path, "r") as file:
        original_content = file.read()

    # Write the new line followed by the original content
    with open(file_path, "w") as file:
        file.write("-- noqa: dbt-opiner all" + "\n" + original_content)

    handler = SqlFileHandler(file_path, manifest)
    assert handler.no_qa_opinions == ["all"]


def test_not_found_in_manifest(temp_complete_git_repo, manifest):
    file = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model_2"
        / "model_2.sql"
    )
    file.touch()
    with pytest.raises(SystemExit) as excinfo:
        SqlFileHandler(file, manifest)
    assert excinfo.value.code == 1


def test_file_does_not_exist(tmp_path, manifest):
    file = tmp_path / "dbt_project" / "model_2.sql"
    with pytest.raises(FileNotFoundError):
        SqlFileHandler(file, manifest)


def test_wrong_extension_sql(temp_complete_git_repo, manifest):
    file = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model"
        / "model.md"
    )
    file.touch()
    with pytest.raises(ValueError):
        SqlFileHandler(file, manifest)


# Test YamlFileHandler
def test_yaml_file_handler(temp_complete_git_repo, manifest):
    os.chdir(temp_complete_git_repo)
    file = Path("dbt_project") / "models" / "test" / "model" / "_model__models.yaml"
    handler = YamlFileHandler(file, manifest)
    assert handler.dbt_nodes[0].type == "model"
    assert handler.to_dict() == {
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
    assert handler.get("version") == 2


def test_wrong_extension_yaml(temp_complete_git_repo, manifest):
    file = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model"
        / "model.md"
    )
    file.touch()
    with pytest.raises(ValueError):
        YamlFileHandler(file)


# Test markdown file handler
def test_markdown_file_handler(temp_complete_git_repo):
    file = (
        temp_complete_git_repo
        / "dbt_project"
        / "models"
        / "test"
        / "model"
        / "model.md"
    )
    handler = MarkdownFileHandler(file)
    assert handler.content == "{% docs id %} Id of the table {% enddocs %}"
