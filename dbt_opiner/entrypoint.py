import time

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
):
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

    opinions_pack = OpinionsPack()
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
