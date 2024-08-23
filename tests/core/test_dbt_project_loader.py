import os
from pathlib import Path

import pytest

from dbt_opiner.dbt import DbtProjectLoader


# Test project loader
@pytest.mark.parametrize(
    "all_files, changed_files_parts, expected_sql_files, expected_yaml_files, expected_md_files",
    [
        pytest.param(True, None, 1, 3, 1, id="all_files is True"),
        pytest.param(
            False,
            [["dbt_project", "models", "test", "model", "model.sql"]],
            1,
            0,
            0,
            id="One changed file, a model in a dbt project",
        ),
        pytest.param(
            False,
            [["dbt_project", "dbt_packages", "package", "macros", "macro.sql"]],
            0,
            0,
            0,
            id="One changed file, a model in a dbt package",
        ),
        pytest.param(
            False,
            [["dbt_project"]],
            1,
            3,
            1,
            id="Changed files is the dbt root directory",
        ),
    ],
)
def test_dbt_project_loader(
    temp_complete_git_repo,
    all_files,
    changed_files_parts,
    expected_sql_files,
    expected_yaml_files,
    expected_md_files,
):
    os.chdir(temp_complete_git_repo)
    changed_files = None
    if changed_files_parts:
        changed_files = [
            temp_complete_git_repo.joinpath(*file_parts)
            for file_parts in changed_files_parts
        ]
    loader = DbtProjectLoader()
    projects = loader.initialize_dbt_projects(
        all_files=all_files, changed_files=changed_files
    )
    # Check correct project initialization
    assert len(projects) == 1
    assert projects[0].dbt_project_file_path == temp_complete_git_repo.joinpath(
        "dbt_project", "dbt_project.yml"
    )
    # Check the number of files loaded
    project_loaded_files = projects[0].files
    assert len(project_loaded_files.get("sql", [])) == expected_sql_files
    assert len(project_loaded_files.get("yaml", [])) == expected_yaml_files
    assert len(project_loaded_files.get("markdown", [])) == expected_md_files


# Test value error for all_files and changed_files
@pytest.mark.parametrize(
    "all_files, changed_files",
    [
        pytest.param(
            True, [Path("some_file")], id="Both all_files and changed_files are passed"
        ),
        pytest.param(False, None, id="Neither all_files and changed_files are passed"),
    ],
)
def test_dbt_project_loader_exceptions(
    temp_complete_git_repo, all_files, changed_files
):
    os.chdir(temp_complete_git_repo)
    loader = DbtProjectLoader()
    with pytest.raises(ValueError):
        loader.initialize_dbt_projects(all_files=all_files, changed_files=changed_files)
