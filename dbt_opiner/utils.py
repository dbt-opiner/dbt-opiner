import os
import subprocess
from pathlib import Path

from loguru import logger


def find_git_root(path):
    current_path = Path(path).resolve()
    while current_path != current_path.parent:
        if (current_path / ".git").exists():
            logger.debug(f"git root is: {current_path}")
            return current_path
        current_path = current_path.parent
    return None


def find_dbt_project_yml(file):
    git_root_path = find_git_root(file)
    if git_root_path is None:
        return None  # Not a git repository

    current_path = Path(file).resolve()

    while current_path != git_root_path:
        if (current_path / "dbt_project.yml").exists():
            return current_path / "dbt_project.yml"
        current_path = current_path.parent

    # Check the git root directory itself
    if (git_root_path / "dbt_project.yml").exists():
        return git_root_path / "dbt_project.yml"

    return None


def find_all_dbt_project_ymls():
    git_root_path = find_git_root(
        os.getcwd()
    )  # Check how this would work in pre-commit hook
    if git_root_path is None:
        return None  # Not a git repository
    dbt_project_ymls = []
    for root, dirs, files in os.walk(git_root_path):
        # Skip .venv directories
        dirs[:] = [d for d in dirs if d != ".venv"]

        for file in files:
            if file == "dbt_project.yml":
                dbt_project_ymls.append(Path(root) / file)
    return dbt_project_ymls


def check_manifest_exists(dbt_project_file_path: Path):
    dbt_project_path = dbt_project_file_path.parent
    manifest_path = dbt_project_path / "target" / "manifest.json"
    return manifest_path.exists()


def run_dbt_command(
    dbt_project_file_path: Path,
    command: str,
    target: str = None,
    dbt_profile_path: Path = None,
):
    """Run dbt command for the given dbt project file path using subprocess"""
    # Get current working directory
    current_working_dir = os.getcwd()
    # Set working directory to the dbt project directory
    os.chdir(dbt_project_file_path.parent)

    cmd = [
        "dbt",
        command,
        "--project-dir",
        str(dbt_project_file_path.parent),
    ]

    if dbt_profile_path:
        cmd.extend(
            [
                "--profiles-dir",
                str(dbt_profile_path.parent),
            ]
        )
    if target:
        cmd.extend(
            [
                "--target",
                str(target),
            ]
        )
    logger.debug(f"Running dbt command: {cmd}")
    subprocess.run(cmd)

    # Reset working directory
    os.chdir(current_working_dir)


def compile_dbt_manifest(
    dbt_project_file_path: Path, dbt_profile_path: Path = None, target: str = None
):
    run_dbt_command(
        command="deps",
        dbt_project_file_path=dbt_project_file_path,
        dbt_profile_path=dbt_profile_path,
        target=target,
    )
    run_dbt_command(
        command="seed",
        dbt_project_file_path=dbt_project_file_path,
        dbt_profile_path=dbt_profile_path,
        target=target,
    )
    run_dbt_command(
        command="compile",
        dbt_project_file_path=dbt_project_file_path,
        dbt_profile_path=dbt_profile_path,
        target=target,
    )
