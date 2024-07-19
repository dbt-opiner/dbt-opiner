from dbt_opiner.dbt_project import DbtProject
from dbt_opiner.dbt_manifest import DbtManifest
from pathlib import Path


if __name__ == '__main__':
  dbt_project = DbtProject(dbt_project_file_path=Path('tests/multi_repo/customers/dbt_project.yml'))
  print(dbt_project.dbt_manifest.manifest_dict.keys())
