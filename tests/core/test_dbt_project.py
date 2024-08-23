import os
from unittest.mock import patch

from dbt_opiner.dbt import DbtProject


def test_dbt_project(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    dbt_project_path = temp_complete_git_repo / "dbt_project" / "dbt_project.yml"
    dbt_project_all_files = DbtProject(dbt_project_path, all_files=True)
    dbt_project_one_file = DbtProject(
        dbt_project_path,
        files=[(dbt_project_path.parent / "models" / "test" / "model" / "model.sql")],
    )
    # Check all files were loaded
    for key, n_files in [("sql", 1), ("yaml", 3), ("markdown", 1)]:
        assert len(dbt_project_all_files.files[key]) == n_files
    # Check only one file was loaded
    assert len(dbt_project_one_file.files["sql"]) == 1


# Test the filer files functionality
def test_dbt_project_filter_files(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    with patch(
        "dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config"
    ) as mock_get_config:
        mock_get_config.return_value = {
            "files": {
                "sql": ".*/models/not_test/.*",
            }
        }
        dbt_project_path = temp_complete_git_repo / "dbt_project" / "dbt_project.yml"
        dbt_project_all_files = DbtProject(dbt_project_path, all_files=True)
        # Check that the file was filtered
        assert dbt_project_all_files.files["sql"] == []
