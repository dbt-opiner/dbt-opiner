from unittest.mock import create_autospec

import pytest

from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler


@pytest.fixture
def mock_sqlfilehandler(request):
    mock_handler = create_autospec(SqlFileHandler)
    mock_handler.type = ".sql"
    mock_handler.dbt_node = request.param
    return mock_handler


@pytest.fixture
def mock_yamlfilehandler(request):
    mock_handler = create_autospec(YamlFileHandler)
    mock_handler.type = ".yaml"
    mock_handler.dbt_nodes = request.param
    return mock_handler
