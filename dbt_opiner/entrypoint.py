import sys
import time
from pathlib import Path

from loguru import logger

from dbt_opiner.dbt import DbtProjectLoader
from dbt_opiner.linter import Linter
from dbt_opiner.opinions.opinions_pack import OpinionsPack


def lint(
    changed_files: list[str] = [],
    all_files: bool = False,
    target: str = None,
    force_compile: bool = False,
    no_ignore: bool = False,
    output_file: str = None,
) -> None:
    """Lint the dbt project using the dbt-opiner package.

    Args:
        changed_files: List of files that have been changed. Defaults to [].
        all_files: Flag to lint all files. Defaults to False.
        target: Target to run the dbt project. Defaults to None.
        force_compile: Flag to force compile the dbt project. Defaults to False
        output_file: Output file to save the linting results. Defaults to None.
    """
    logger.info("Linting dbt projects...")
    loader = DbtProjectLoader(target, force_compile)

    dbt_projects = loader.initialize_dbt_projects(
        changed_files=changed_files, all_files=all_files
    )

    opinions_pack = OpinionsPack(no_ignore)
    linter = Linter(opinions_pack, no_ignore)

    # TODO: make it parallel?
    start = time.process_time()
    for dbt_project in dbt_projects:
        merged_files = [
            item for files_list in dbt_project.files.values() for item in files_list
        ]
        for file in merged_files:
            linter.lint_file(file)
    end = time.process_time()

    logger.info(f"Linting completed in {round(end - start, 3)} seconds")
    linter.log_results_and_exit(output_file)


def audit(
    type: str,
    format: str = "md",
    dbt_project_dir: str = None,
    target: str = None,
    force_compile: bool = False,
    no_ignore: bool = False,
    output_file: str = None,
) -> None:
    """Audit the dbt project using the dbt-opiner package.

    Args:
        type: Type of audit to log. Defaults to all.
        format: Format of the printed output md or csv. Defaults to md.
        dbt_project_dir: Directory of the dbt project to audit.
                         If not provided, all dbt projects in the git repository
                         will be audited.
        target: Target to run the dbt project. Defaults to None.
        force_compile: Flag to force compile the dbt project. Defaults to False
        no_ignore: Flag to ignore the no qa configurations. Defaults to False.
        output_file: Output file to save the linting results. Defaults to None.
    """
    logger.info("Auditing dbt projects...")
    loader = DbtProjectLoader(target, force_compile)

    if dbt_project_dir:
        if (Path(dbt_project_dir) / "dbt_project.yml").exists():
            dbt_projects = loader.initialize_dbt_projects(
                changed_files=[dbt_project_dir]
            )
        else:
            logger.critical(f"Directory {dbt_project_dir} is not a dbt project")
            sys.exit(1)
    else:
        dbt_projects = loader.initialize_dbt_projects(all_files=True)

    opinions_pack = OpinionsPack(no_ignore)
    linter = Linter(opinions_pack, no_ignore)
    for dbt_project in dbt_projects:
        merged_files = [
            item for files_list in dbt_project.files.values() for item in files_list
        ]
        for file in merged_files:
            linter.lint_file(file)

    linter.log_audit_and_exit(type=type, format=format, output_file=output_file)
