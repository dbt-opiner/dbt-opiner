import os

from dbt_opiner.dbt import DbtProject


def test_dbt_project(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    dbt_project_path = temp_complete_git_repo / "dbt_project" / "dbt_project.yml"
    dbt_project_all_files = DbtProject(dbt_project_path, all_files=True)
    dbt_project_one_file = DbtProject(
        dbt_project_path,
        files=[(dbt_project_path.parent / "models" / "test" / "model.sql")],
    )
    # Check all files loaded
    for key, n_files in [("sql", 1), ("yaml", 3), ("markdown", 1)]:
        assert len(dbt_project_all_files.files[key]) == n_files
    # Check only one file loaded
    assert len(dbt_project_one_file.files["sql"]) == 1
