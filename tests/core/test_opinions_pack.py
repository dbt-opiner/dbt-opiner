import logging
import os
import subprocess
import sys
from unittest.mock import patch

import pytest

from dbt_opiner.opinions.opinions_pack import OpinionsPack


@pytest.mark.parametrize(
    "source, revision, expected",
    [
        pytest.param("local", None, "Loading custom opinions from local source:"),
        pytest.param("git", "some_rev", "Check out to revision:"),
        pytest.param(
            "git", None, "Revision not defined. Main branch latest commit will be used."
        ),
        pytest.param("invalid", None, "Custom opinions source invalid not supported"),
        pytest.param(
            None,
            None,
            "No custom opinions source defined. Skipping custom opinions loading.",
        ),
    ],
)
def test_opinions_pack(caplog, temp_complete_git_repo, source, revision, expected):
    os.chdir(temp_complete_git_repo)
    with patch(
        "dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config"
    ) as mock_get_config, patch("subprocess.run") as mock_subprocess_run:
        mock_subprocess_run.return_value = None
        mock_get_config.return_value = {
            "opinions_config": {
                "ignore_opinions": ["O001"],
                "custom_opinions": {
                    "source": source,
                    "repository": "https://github.com/some/repo.git",
                    "rev": revision,
                },
            }
        }
        opinions_pack = OpinionsPack()
        opinions = opinions_pack.get_opinions()
        # There should be at least 1 opinion from opinion classes
        assert len(opinions) > 0
        # Ignored opinions
        # Check that the opinion O001 was ignored
        assert "O001" not in [opinion.code for opinion in opinions]

        # Custom opinions
        # Check log messages
        with caplog.at_level(logging.DEBUG):
            assert expected in caplog.text

        if source == "local":
            # Check that the custom opinion C001, and C002 were loaded from local source
            opinion_codes = {opinion.code for opinion in opinions}
            assert "C001" in opinion_codes
            assert "C002" in opinion_codes
            # Check that subprocess.run was called for installing the required packages (for C001)
            mock_subprocess_run.assert_called_once_with(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "some_pypi_package",
                ]
            )
            # Check that C002 didn't need any package installation
            with caplog.at_level(logging.DEBUG):
                assert "No required packages for opinion C002" in caplog.text
        elif source == "git":
            # Check that subprocess.run was called
            mock_subprocess_run.assert_called()


@pytest.mark.parametrize(
    "repository, revision, expected",
    [
        pytest.param(
            "git-repo",
            "some-sha",
            "Could not clone git repository:",
            id="Fails because subprocess.run fails and raises exception with 1",
        ),
        pytest.param(
            None,
            None,
            "Custom opinions source is git but repository is not defined.",
            id="Fails because source is git and repo is not defined exits with 1",
        ),
    ],
)
def test_opinions_pack_exit_one(caplog, repository, revision, expected):
    with patch(
        "dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config"
    ) as mock_get_config, patch(
        "subprocess.run",
        side_effect=subprocess.CalledProcessError(
            1, ["cmd", "command"], stderr=b"Some error occurred"
        ),
    ):
        mock_get_config.return_value = {
            "opinions_config": {
                "custom_opinions": {
                    "source": "git",
                    "repository": repository,
                    "revision": revision,
                },
            }
        }
        with pytest.raises(SystemExit) as excinfo:
            opinions_pack = OpinionsPack()
            opinions_pack.get_opinions()
        assert excinfo.value.code == 1
        with caplog.at_level(logging.DEBUG):
            assert expected in caplog.text
