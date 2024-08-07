from collections import defaultdict
from pathlib import Path

from loguru import logger

from dbt_opiner.dbt_artifacts import DbtProject
from dbt_opiner.linter import Linter
from dbt_opiner.opinions.opinions_pack import OpinionsPack
from dbt_opiner.utils import find_all_dbt_project_ymls
from dbt_opiner.utils import find_dbt_project_yml


def get_dbt_projects_all_files(target: str = None, force_compile: bool = False):
    """
    Initialize all dbt projects and files in the git repository.
    """
    dbt_projects_file_paths = find_all_dbt_project_ymls()
    dbt_projects = []

    for dbt_project_file_path in dbt_projects_file_paths:
        dbt_project = DbtProject(
            dbt_project_file_path=dbt_project_file_path,
            target=target,
            force_compile=force_compile,
        )
        dbt_projects.append(dbt_project)
    return dbt_projects


def get_dbt_projects_changed_files(
    changed_files: list, target: str = None, force_compile: bool = False
):
    """
    Initialize dbt projects with the corresponding changed files.
    """
    file_to_project_map = defaultdict(list)

    for file in changed_files:
        try:
            assert file.exists()
        except AssertionError:
            raise FileNotFoundError(f"{file} does not exist")
        dbt_project_file_path = find_dbt_project_yml(file)
        if dbt_project_file_path:
            logger.debug(f"Found dbt_project.yml for file {file}")
            file_to_project_map[dbt_project_file_path].append(file)
        else:
            logger.debug(f"No dbt_project.yml found for file {file}")

    dbt_projects = []
    for dbt_project_file_path, files in file_to_project_map.items():
        dbt_project = DbtProject(
            dbt_project_file_path=dbt_project_file_path,
            files=files,
            all_files=False,
            target=target,
            force_compile=force_compile,
        )
        dbt_projects.append(dbt_project)
    return dbt_projects


def initialize_dbt_projects(
    changed_files: list = [],
    all_files: bool = False,
    target: str = None,
    force_compile: bool = False,
):
    if all_files and changed_files:
        raise ValueError("Cannot process all files and changed files at the same time")

    if all_files:
        logger.debug("Processing all files")
        dbt_projects = get_dbt_projects_all_files(
            target=target, force_compile=force_compile
        )

    if changed_files:
        logger.debug("Processing changed files")
        files = [Path(file) for file in changed_files]
        dbt_projects = get_dbt_projects_changed_files(
            files, target=target, force_compile=force_compile
        )

    return dbt_projects


def lint(
    changed_files: list[Path] = [],
    all_files: bool = False,
    target: str = None,
    force_compile: bool = False,
):
    dbt_projects = initialize_dbt_projects(
        changed_files=changed_files,
        all_files=all_files,
        target=target,
        force_compile=force_compile,
    )

    opinions_pack = OpinionsPack()
    linter = Linter(opinions_pack)

    for dbt_project in dbt_projects:
        merged_files = [
            item for sublist in dbt_project.files.values() for item in sublist
        ]
        for file in merged_files:
            linter.lint_file(file)

    linter.log_results_and_exit()
