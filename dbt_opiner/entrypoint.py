from loguru import logger
from dbt_opiner.dbt_artifacts import DbtProject
from dbt_opiner.utils import find_dbt_project_yml, find_all_dbt_project_ymls


def process_all_files():
    # Find all dbt projects
    dbt_projects_file_paths = find_all_dbt_project_ymls()
    dbt_projects = []
    # Load all dbt projects
    for dbt_project_file_path in dbt_projects_file_paths:
        dbt_project = DbtProject(dbt_project_file_path=dbt_project_file_path)
        dbt_projects.append(dbt_project)
    # Process every dbt project
    for project in dbt_projects:
        logger.info(project.dbt_manifest.manifest_dict)


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
        # Load dbt projects


def lint(changed_files: list = [], all_files: bool = False, **kwargs):
    if all_files and changed_files:
        raise ValueError("Cannot process all files and changed files at the same time")

    if all_files:
        logger.info("Processing all files")
        process_all_files()

    if changed_files:
        logger.info("Processing changed files")
        for file in changed_files:
            logger.info(file)
        # process_changed_files(changed_files)
