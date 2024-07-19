from pathlib import Path
from dbt_opiner.dbt_manifest import DbtManifest
class DbtProject:
  def __init__(self, dbt_project_file_path: Path)->None:
    try:
      assert dbt_project_file_path.exists()
      self.dbt_project_file_path = dbt_project_file_path
    except AssertionError:
      raise FileNotFoundError(f'{dbt_project_file_path} does not exist')
    self.dbt_manifest = DbtManifest(self.dbt_project_file_path.parent / 'target' / 'manifest.json')
    self.files = dict(sql=[], yaml=[], markdown=[])

      