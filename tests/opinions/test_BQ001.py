from pathlib import Path

import pytest

from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.opinions import BQ001


@pytest.mark.parametrize(
    "profiles_content, config, expected_passed",
    [
        pytest.param(
            {
                "project": {
                    "outputs": {
                        "dev": {"type": "bigquery", "maximum_bytes_billed": 100},
                        "prod": {},
                    }
                }
            },
            {"sqlglot_dialect": "bigquery"},
            True,
            id="max_bytes_billed_present",
        ),
        pytest.param(
            {"project": {"outputs": {"dev": {"type": "bigquery"}, "prod": {}}}},
            {"sqlglot_dialect": "bigquery"},
            False,
            id="max_bytes_billed_missing",
        ),
        pytest.param(
            {
                "project": {
                    "outputs": {
                        "dev": {"type": "bigquery", "maximum_bytes_billed": 100},
                        "prod": {},
                    }
                }
            },
            {
                "sqlglot_dialect": "bigquery",
                "opinions_config": {
                    "extra_opinions_config": {"BQ001": {"maximum_bytes_billed": 1}}
                },
            },
            False,
            id="max_bytes_billed_exceeded",
        ),
        pytest.param(
            {},
            {"sqlglot_dialect": "other"},
            None,
            id="not_BQ_dialect",
        ),
    ],
)
def test_yaml_BQ001(
    tmpdir,
    profiles_content,
    config,
    expected_passed,
):
    profiles = Path(tmpdir) / "profiles.yml"
    profiles.touch()
    file = YamlFileHandler(profiles)
    file._dict = profiles_content
    opinion = BQ001(config=config)
    result = opinion.check_opinion(file)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None
