from unittest import mock

import pytest
import yaml

from dbt_opiner import file_handlers


# Test SqlFileHandler
def test_sql_file_handler_model(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.sql"
    handler = file_handlers.SqlFileHandler(file, dbt_project)
    assert handler.content == "select id, value from table"
    assert handler.dbt_node.type == "model"
    assert str(handler) == str(file)


def test_sql_file_handler_macro(dbt_project):
    file = dbt_project.dbt_project_dir_path / "macros" / "my_macro.sql"
    handler = file_handlers.SqlFileHandler(file, dbt_project)
    assert (
        handler.content
        == "{% macro my_macro() %} select id, value from table {% endmacro %}"
    )
    assert handler.dbt_node.type == "macro"


@pytest.mark.parametrize("no_qa_opinions", ["all", "C001"])
def test_get_no_qa_opinion(dbt_project, no_qa_opinions):
    file_path = (
        dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.sql"
    )
    # Modify file and include no_qa_opinions
    with open(file_path, "r") as file:
        original_content = file.read()

    # Write the new line followed by the original content
    with open(file_path, "w") as file:
        file.write(f"-- noqa: dbt-opiner {no_qa_opinions}" + "\n" + original_content)

    handler = file_handlers.SqlFileHandler(file_path, dbt_project)
    assert handler.no_qa_opinions == [no_qa_opinions]


def test_not_found_in_manifest(dbt_project):
    file = (
        dbt_project.dbt_project_dir_path / "models" / "test" / "model_2" / "model_2.sql"
    )
    file.touch()
    with pytest.raises(SystemExit) as excinfo:
        file_handlers.SqlFileHandler(file, dbt_project)
    assert excinfo.value.code == 1


def test_file_does_not_exist(dbt_project):
    file = dbt_project.dbt_project_dir_path / "model_2.sql"
    with pytest.raises(FileNotFoundError):
        file_handlers.SqlFileHandler(file, dbt_project)


def test_wrong_extension_sql(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.md"
    with pytest.raises(ValueError):
        file_handlers.SqlFileHandler(file, dbt_project)


def test_runtime_open(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.sql"
    with mock.patch("pathlib.Path.open") as mock_open:
        mock_open.side_effect = Exception("Mocked exception")
        with pytest.raises(RuntimeError, match="Error reading file: Mocked exception"):
            file_handlers.SqlFileHandler(file, dbt_project)


# Testfile_handlers.YamlFileHandler
def test_yaml_file_handler(dbt_project):
    file = (
        dbt_project.dbt_project_dir_path
        / "models"
        / "test"
        / "model"
        / "_model__models.yaml"
    )
    handler = file_handlers.YamlFileHandler(file, dbt_project)
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


def test_wrong_extension_yaml(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.md"
    with pytest.raises(ValueError):
        file_handlers.YamlFileHandler(file, dbt_project)


def test_runtime_safe_load(dbt_project):
    file = (
        dbt_project.dbt_project_dir_path
        / "models"
        / "test"
        / "model"
        / "_model__models.yaml"
    )
    with mock.patch("yaml.safe_load") as mock_safe_load:
        mock_safe_load.side_effect = yaml.YAMLError
        handler = file_handlers.YamlFileHandler(file, dbt_project)
        with pytest.raises(RuntimeError, match="Error parsing YAML file"):
            handler.to_dict()
        mock_safe_load.side_effect = Exception
        with pytest.raises(RuntimeError, match="Error reading YAML file"):
            handler.to_dict()


# Test markdown file handler
def test_markdown_file_handler(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.md"
    handler = file_handlers.MarkdownFileHandler(file, dbt_project)
    assert handler.content == "{% docs id %} Id of the table {% enddocs %}"


def test_wrong_extension_md(dbt_project):
    file = dbt_project.dbt_project_dir_path / "models" / "test" / "model" / "model.sql"
    with pytest.raises(ValueError):
        file_handlers.MarkdownFileHandler(file, dbt_project)
