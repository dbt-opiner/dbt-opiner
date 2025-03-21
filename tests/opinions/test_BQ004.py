import pytest

from dbt_opiner import file_handlers
from dbt_opiner.opinions import BQ004


@pytest.mark.parametrize(
    "dbt_project_content, config, expected_passed",
    [
        pytest.param(
            {"models": {"persist_docs": {"relation": True, "columns": True}}},
            {"sqlglot_dialect": "bigquery"},
            True,
            id="Persist docs enabled",
        ),
        pytest.param(
            {"models": {}},
            {"sqlglot_dialect": "bigquery"},
            False,
            id="Persist docs columns not present",
        ),
        pytest.param({}, {"sqlglot_dialect": "other"}, None, id="Not bigquery dialect"),
    ],
)
def test_yaml_BQ004(
    dbt_project,
    dbt_project_content,
    config,
    expected_passed,
):
    dbt_project_file = dbt_project.dbt_project_dir_path / "dbt_project.yml"
    file = file_handlers.YamlFileHandler(dbt_project_file, dbt_project)
    file._dict = dbt_project_content
    opinion = BQ004(config)
    result = opinion.check_opinion(file)
    if expected_passed is not None:
        assert result.passed == expected_passed
    else:
        assert result is None
