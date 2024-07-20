from dbt_opiner.dbt_project import DbtProject
from pathlib import Path
from dbt_opiner.config_handler import ConfigHandler
from dbt_opiner.utils import find_dbt_project_yml, find_all_dbt_project_ymls

if __name__ == "__main__":
    # If we do changed files
    changed_files = [
        Path("tests/multi_repo/customers/models/customer.sql"),
    ]
    # Find dbt project file
    dbt_project_file_path = find_dbt_project_yml(changed_files[0])

    # If we do all
    dbt_projects_file_paths = find_all_dbt_project_ymls()
    print(dbt_projects_file_paths)
    exit()

    # Load dbt project
    dbt_project = DbtProject(dbt_project_file_path=dbt_project_file_path)

    # Load configs from dbt project path
    config_path = (
        dbt_project.dbt_project_file_path.parent / ".dbt_opiner" / "config.yaml"
    )
    ConfigHandler.set_config_path(config_path)
    config_handler = ConfigHandler()

    # Load project files
    dbt_project.load_all_files()

    # Load manifest

    # Map manifest to files

    for file in dbt_project.files["sql"]:
        print(file)
