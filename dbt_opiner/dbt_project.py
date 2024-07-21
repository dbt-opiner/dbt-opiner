from pathlib import Path
from dbt_opiner.dbt_manifest import DbtManifest
from dbt_opiner.file import SQLFileHandler, YamlFileHandler, MarkdownFileHandler


class DbtProject:
    def __init__(self, dbt_project_file_path: Path) -> None:
        try:
            assert dbt_project_file_path.exists()
            self.dbt_project_file_path = dbt_project_file_path
        except AssertionError:
            raise FileNotFoundError(f"{dbt_project_file_path} does not exist")
        try:
            assert (
                self.dbt_project_file_path.parent / "target" / "manifest.json"
            ).exists()
            self.dbt_manifest = DbtManifest(
                self.dbt_project_file_path.parent / "target" / "manifest.json"
            )
        except AssertionError:
            raise FileNotFoundError(
                f"{self.dbt_project_file_path.parent / 'target' / 'manifest.json'} does not exist"
            )
        self.files = dict(sql=[], yaml=[], markdown=[])

    def load_all_files(self):
        """
        Create an object for every sql, yaml, and markdown file in the dbt project
        and add it to self.files dictionary.
        """
        files = (self.dbt_project_file_path.parent / "models").rglob(
            "*"
        )  # TODO: add folder config.
        self.load_files(files)

    def load_files(self, files: list):
        """
        Load only the list of files pased. Useful for git diff files loading.
        """
        for file in files:
            if file.is_file():
                if file.suffix == ".sql":
                    self.files["sql"].append(SQLFileHandler(file))
                elif file.suffix in [".yml", ".yaml"]:
                    self.files["yaml"].append(YamlFileHandler(file))
                elif file.suffix == ".md":
                    self.files["markdown"].append(MarkdownFileHandler(file))
