from unittest.mock import create_autospec

import pytest
import yaml

from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler


@pytest.fixture
def mock_sqlfilehandler(request):
    mock_handler = create_autospec(SqlFileHandler)
    mock_handler.type = ".sql"
    try:
        mock_handler.dbt_node = request.param
    except AttributeError:
        mock_handler.dbt_node = None
    return mock_handler


@pytest.fixture
def mock_yamlfilehandler(request):
    mock_handler = create_autospec(YamlFileHandler)
    mock_handler.type = ".yaml"
    try:
        mock_handler.dbt_nodes = request.param
    except AttributeError:
        mock_handler.dbt_nodes = []
    return mock_handler


@pytest.fixture(autouse=True)
def reset_singletons():
    ConfigSingleton._instance = None


@pytest.fixture
def temp_empty_git_repo(tmp_path):
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    return tmp_path


@pytest.fixture
def temp_git_repo_with_config(temp_empty_git_repo):
    dbt_opiner_dir = temp_empty_git_repo / "dbt-opiner"
    dbt_opiner_dir.mkdir()
    dbt_opiner_file = dbt_opiner_dir / ".dbt-opiner.yaml"
    dbt_opiner_file.touch()
    config = {"config": "test"}
    with open(dbt_opiner_file, "w") as f:
        yaml.dump(config, f)
    return temp_empty_git_repo
