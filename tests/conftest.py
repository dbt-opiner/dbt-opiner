from unittest.mock import create_autospec

import pytest

from dbt_opiner.file_handlers import SQLFileHandler


@pytest.fixture
def mock_sqlfilehandler(request):
    dbt_node, expected_passed = request.param
    mock_handler = create_autospec(SQLFileHandler)
    mock_handler.file_type = ".sql"
    mock_handler.dbt_node = dbt_node
    return mock_handler, expected_passed
