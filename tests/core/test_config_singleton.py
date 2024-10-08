import logging
import os
import pathlib
import shutil
import tempfile
from unittest import mock

import pytest

from dbt_opiner import config_singleton


@mock.patch.object(config_singleton.ConfigSingleton, "_initialize")
def test_singleton_instance(mock_initialize):
    # Ensure that the same instance is returned every time
    instance1 = config_singleton.ConfigSingleton()
    instance2 = config_singleton.ConfigSingleton()
    assert instance1 is instance2
    # Ensure that the _initialize method is called only once
    mock_initialize.assert_called_once()


def test_initialize_with_config(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    with open(".dbt-opiner.yaml", "w") as file:
        file.write(
            "sqlglot_dialect: test\n"
            "files:\n"
            "  sql: ${{ ENV_VAR }}\n"
            "  md: ${{ENV_VAR}}${{ ENV_VAR }}"
        )

    env_var = "test"
    os.environ["ENV_VAR"] = env_var
    config = config_singleton.ConfigSingleton().get_config()
    config_file_path = config_singleton.ConfigSingleton().get_config_file_path()
    assert config == {
        "sqlglot_dialect": "test",
        "files": {
            "sql": env_var,
            "md": f"{env_var}{env_var}",
        },
    }
    assert (
        config_file_path == temp_complete_git_repo / "dbt-opiner" / ".dbt-opiner.yaml"
    )


def test_initialize_without_config(temp_empty_git_repo):
    os.chdir(temp_empty_git_repo)
    config = config_singleton.ConfigSingleton().get_config()
    assert config == {}


@pytest.mark.parametrize(
    "config, expected",
    [
        pytest.param(
            "sqlglot_dialect: test\ninvalid_key: test",
            "Unexpected key:",
            id="invalid_key",
        ),
        pytest.param(
            "opinions_config: test",
            "Expected opinions_config to be a dictionary but got str",
            id="invalid_type_main_key",
        ),
        pytest.param(
            "sqlglot_dialect: 1",
            "Expected sqlglot_dialect to be of type str, but got int",
            id="invalid_type_key",
        ),
        pytest.param(
            "shared_config:\n  rev: test",
            "Missing required key: repository",
            id="missing_key",
        ),
    ],
)
def test_initialize_with_invalid_config(
    caplog, temp_complete_git_repo, config, expected
):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    with open(".dbt-opiner.yaml", "w") as file:
        file.write(config)

    with pytest.raises(SystemExit) as excinfo:
        config_singleton.ConfigSingleton().get_config()
    assert excinfo.value.code == 1
    with caplog.at_level(logging.CRITICAL):
        assert expected in caplog.text


@pytest.mark.parametrize(
    "overwrite, expected",
    [
        pytest.param(
            True,
            {
                "sqlglot_dialect": "test_2",
                "files": {"sql": "c", "md": "d", "yaml": "e"},
            },
            id="overwrite_true",
        ),
        pytest.param(
            False,
            {
                "shared_config": {"repository": "some_git_repo", "overwrite": False},
                "sqlglot_dialect": "test",
                "files": {"sql": "a", "md": "b", "yaml": "e"},
            },
            id="overwrite_false",
        ),
        pytest.param(
            None,
            {
                "sqlglot_dialect": "test_2",
                "files": {"sql": "c", "md": "d", "yaml": "e"},
            },
            id="overwrite_missing_defaults_to_true",
        ),
    ],
)
def test_initialize_with_shared_config(temp_complete_git_repo, overwrite, expected):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    if overwrite is not None:
        overwrite = f"  overwrite: {overwrite}\n"
    else:
        overwrite = ""

    with open(".dbt-opiner.yaml", "w") as file:
        file.write(
            "shared_config:\n"
            "  repository: some_git_repo\n"
            f"{overwrite}"
            "sqlglot_dialect: test\n"
            "files:\n"
            "  sql: a\n"
            "  md: b"
        )

    with mock.patch(
        "dbt_opiner.config_singleton.git.clone_git_repo_and_checkout_revision"
    ) as mock_clone:
        shared_config_repo = tempfile.mkdtemp()
        (pathlib.Path(shared_config_repo) / ".git").touch()
        with open(pathlib.Path(shared_config_repo) / ".dbt-opiner.yaml", "w") as file:
            file.write(
                "sqlglot_dialect: test_2\n"
                "files:\n"
                "  sql: c\n"
                "  md: d\n"
                "  yaml: e\n"
            )
        mock_clone.return_value = pathlib.Path(shared_config_repo)

        config = config_singleton.ConfigSingleton().get_config()
        mock_clone.assert_called_once()
        assert config == expected

        # This should raise an error because it should be deleted in the _initialize method
        with pytest.raises(FileNotFoundError):
            shutil.rmtree(shared_config_repo)


def test_initialize_with_invalid_shared_config(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    with open(".dbt-opiner.yaml", "w") as file:
        file.write("shared_config:\n  repository: some_git_repo\n  overwrite: true\n")

    with mock.patch(
        "dbt_opiner.config_singleton.git.clone_git_repo_and_checkout_revision"
    ) as mock_clone:
        shared_config_repo = tempfile.mkdtemp()
        (pathlib.Path(shared_config_repo) / ".git").touch()
        with open(pathlib.Path(shared_config_repo) / ".dbt-opiner.yaml", "w") as file:
            file.write(
                "invalid_key: test_2\n"
                "files:\n"
                "  sql: c\n"
                "  md: d\n"
                "  yaml: e\n"
            )
        mock_clone.return_value = pathlib.Path(shared_config_repo)
        with pytest.raises(SystemExit) as excinfo:
            config_singleton.ConfigSingleton().get_config()
        assert excinfo.value.code == 1


def test_initialize_with_missing_shared_config(caplog, temp_complete_git_repo):
    os.chdir(temp_complete_git_repo / "dbt-opiner")
    with open(".dbt-opiner.yaml", "w") as file:
        file.write("shared_config:\n  repository: some_git_repo\n  overwrite: true\n")

    with mock.patch(
        "dbt_opiner.config_singleton.git.clone_git_repo_and_checkout_revision"
    ) as mock_clone:
        shared_config_repo = tempfile.mkdtemp()
        (pathlib.Path(shared_config_repo) / ".git").touch()
        mock_clone.return_value = pathlib.Path(shared_config_repo)
        with pytest.raises(SystemExit) as excinfo:
            config_singleton.ConfigSingleton().get_config()
        assert excinfo.value.code == 1
        with caplog.at_level(logging.CRITICAL):
            assert (
                "Shared configuration file 'dbt-opiner.yaml' not found in repository"
                in caplog.text
            )
