from pathlib import Path
import os


def find_git_root(path):
    current_path = Path(path).resolve()
    while current_path != current_path.parent:
        if (current_path / ".git").exists():
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
