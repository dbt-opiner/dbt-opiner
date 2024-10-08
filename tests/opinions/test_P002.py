from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dbt_opiner import file_handlers
from dbt_opiner.opinions import P002


@pytest.mark.parametrize(
    "dbt_file_content, file_type, expected_passed",
    [
        pytest.param(
            {"flags": {"send_anonymous_usage_stats": False}},
            "dbt_project.yml",
            True,
            id="Anonymous statistics are disabled in dbt_project.yml file.",
        ),
        pytest.param(
            {"config": {"send_anonymous_usage_stats": False}},
            "profiles.yml",
            True,
            id="Anonymous statistics are disabled in profiles.yml file.",
        ),
        pytest.param(
            {},
            "dbt_project.yml",
            False,
            id="Anonymous statistics are not disabled in dbt_project.yml file.",
        ),
        pytest.param(
            {},
            "profiles.yml",
            False,
            id="Anonymous statistics are not disabled in profiles.yml file.",
        ),
        pytest.param(
            {},
            "other.yml",
            None,
            id="Not profiles nor dbt_project file.",
        ),
    ],
)
def test_P002(
    tmpdir,
    dbt_file_content,
    file_type,
    expected_passed,
):
    dbt_file = Path(tmpdir) / file_type
    dbt_file.touch()
    file = file_handlers.YamlFileHandler(dbt_file)

    DbtProject = MagicMock()
    mock_dbt_project = DbtProject()

    if file_type == "dbt_project.yml":
        mock_dbt_project.dbt_project_config = dbt_file_content
    else:
        mock_dbt_project.dbt_profile = dbt_file_content

    file.parent_dbt_project = mock_dbt_project
    opinion = P002()
    result = opinion.check_opinion(file)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None
