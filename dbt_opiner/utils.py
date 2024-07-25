import os
import subprocess
from loguru import logger
from pathlib import Path


def find_git_root(path):
    current_path = Path(path).resolve()
    while current_path != current_path.parent:
        if (current_path / ".git").exists():
            logger.debug("git root is: {current_path}")
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


def run_dbt_command(dbt_project_file_path: Path, dbt_profile_path: Path, command: str):
    """Run dbt command for the given dbt project file path using subprocess"""
    if dbt_profile_path:
        logger.info(
            f"Running dbt {command} for {dbt_project_file_path} with profile {dbt_profile_path}"
        )
        subprocess.run(
            [
                "dbt",
                command,
                "--project-dir",
                dbt_project_file_path.parent,
                "--profiles-dir",
                dbt_profile_path.parent,
            ]
        )
    else:
        subprocess.run(["dbt", command, "--project-dir", dbt_project_file_path.parent])
