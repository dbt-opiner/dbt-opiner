from unittest.mock import create_autospec

import pytest

from dbt_opiner.file_handlers import SqlFileHandler


@pytest.fixture
def mock_sqlfilehandler(request):
    mock_handler = create_autospec(SqlFileHandler)
    mock_handler.type = ".sql"
    mock_handler.dbt_node = request.param
    return mock_handler
