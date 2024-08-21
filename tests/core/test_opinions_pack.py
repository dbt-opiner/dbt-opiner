import logging
import os
import sys
from unittest.mock import patch

import pytest

from dbt_opiner.opinions.opinions_pack import OpinionsPack


@pytest.mark.parametrize(
    "source, expected",
    [
        pytest.param("local", "Loading custom opinions from local source:"),
        pytest.param("git", "Loading custom opinions from git repository:"),
        pytest.param("invalid", "Custom opinions source invalid not supported"),
        pytest.param(
            None, "No custom opinions source defined. Skipping custom opinions loading."
        ),
    ],
)
def test_opinions_pack(temp_complete_git_repo, source, expected, caplog):
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
