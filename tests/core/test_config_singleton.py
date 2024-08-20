import os
from unittest.mock import patch

from dbt_opiner.config_singleton import ConfigSingleton


@patch.object(ConfigSingleton, "_initialize")
def test_singleton_instance(mock_initialize):
    # Ensure that the same instance is returned every time
    instance1 = ConfigSingleton()
    instance2 = ConfigSingleton()
    assert instance1 is instance2
    # Ensure that the _initialize method is called only once
    mock_initialize.assert_called_once()


def test_initialize_with_config(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    config = ConfigSingleton().get_config()
    config_file_path = ConfigSingleton().get_config_file_path()
    assert config == {"config": "test"}
    assert (
        config_file_path == temp_complete_git_repo / "dbt-opiner" / ".dbt-opiner.yaml"
    )


def test_initialize_without_config(temp_empty_git_repo):
    os.chdir(temp_empty_git_repo)
    config = ConfigSingleton().get_config()
    assert config == {}
