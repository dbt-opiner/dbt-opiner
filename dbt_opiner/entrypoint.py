from dbt_opiner.dbt_project import DbtProject
from pathlib import Path
from dbt_opiner.utils import find_dbt_project_yml, find_all_dbt_project_ymls


def process_all_files():
    dbt_projects_file_paths = find_all_dbt_project_ymls()
    dbt_projects = []
    for dbt_project_file_path in dbt_projects_file_paths:
        dbt_project = DbtProject(dbt_project_file_path=dbt_project_file_path)
        dbt_project.load_all_files()
        dbt_projects.append(dbt_project)
    for project in dbt_projects:
        print(project.files)


def process_changed_files(changed_files: list):
    file_to_project_map = {}
    for file in changed_files:
        try:
            assert file.exists()
        except AssertionError:
            raise FileNotFoundError(f"{file} does not exist")
        dbt_project_file_path = find_dbt_project_yml(file)
        if file_to_project_map.get("dbt_project_file_path"):
            file_to_project_map[dbt_project_file_path].append(file)
        else:
            file_to_project_map[dbt_project_file_path] = [file]
        print(file_to_project_map)


def main(changed_files: list = [], all_files: bool = False):
    if all_files:
        process_all_files()
    else:
        process_changed_files(changed_files)


if __name__ == "__main__":
    main(
        changed_files=[
            Path("tests/multi_repo/customers/models/dimensions/dim_customers.sql"),
        ]
    )
    main(all_files=True)
