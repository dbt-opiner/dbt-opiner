import json
import re
from pathlib import Path
from loguru import logger
from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.file_handlers import (
    SQLFileHandler,
    YamlFileHandler,
    MarkdownFileHandler,
)
from dbt_opiner.utils import run_dbt_command

MATCH_ALL = r".*"


class DbtProject:
    def __init__(
        self, dbt_project_file_path: Path, files: list = [], all_files: bool = True
    ) -> None:
        # Set config
        self._config = ConfigSingleton().get_config()

        # Set dbt project file
        try:
            assert dbt_project_file_path.exists()
            self.dbt_project_file_path = dbt_project_file_path
            self.dbt_project_config = YamlFileHandler(dbt_project_file_path)
        except AssertionError:
            raise FileNotFoundError(f"{dbt_project_file_path} does not exist")

        # Set profiles file
        if (dbt_project_file_path.parent / "profiles.yml").exists():
            self.dbt_profile_path = dbt_project_file_path.parent / "profiles.yml"
        else:
            self.dbt_profile_path = None
        # Load manifest
        self._load_manifest()

        # Load files
        self.files = dict(sql=[], yaml=[], markdown=[])
        if all_files:
            self._init_all_files()
        else:
            self._init_files(files)

    def _init_all_files(self):
        """
        Create an object for every sql, yaml, and markdown file in the dbt project
        and add it to self.files dictionary.
        """
        files = (self.dbt_project_file_path.parent).rglob("*")
        self._init_files(files)

    def _init_files(self, files: list):
        """
        Initialize only the list of files pased. Useful for git diff files loading.
        """
        for file in files:
            if file.is_file():
                if file.suffix == ".sql":
                    # replace with regex match
                    if self.do_file_match(
                        self._config.get("sql").get("files", MATCH_ALL), file
                    ):
                        self.files["sql"].append(SQLFileHandler(file))
                elif file.suffix in [".yml", ".yaml"]:
                    try:
                        yaml_config = self._config["yaml"]
                        file_pattern = yaml_config.get("files", MATCH_ALL)
                    except KeyError:
                        file_pattern = self._config.get("yml").get("files", MATCH_ALL)
                    if self.do_file_match(file_pattern, file):
                        self.files["yaml"].append(YamlFileHandler(file))
                elif file.suffix == ".md":
                    if self.do_file_match(
                        self._config.get("md").get("files", MATCH_ALL), file
                    ):
                        self.files["markdown"].append(MarkdownFileHandler(file))
                else:
                    logger.debug(f"{file.suffix} is not supported. Skipping.")

    def _load_manifest(self):
        try:
            assert (
                self.dbt_project_file_path.parent / "target" / "manifest.json"
            ).exists()
        except AssertionError:
            logger.info(
                f"{self.dbt_project_file_path.parent / 'target' / 'manifest.json'} does not exist. Compiling."
            )
            run_dbt_command(self.dbt_project_file_path, self.dbt_profile_path, "deps")
            run_dbt_command(self.dbt_project_file_path, self.dbt_profile_path, "seed")
            run_dbt_command(
                self.dbt_project_file_path, self.dbt_profile_path, "compile"
            )
        self.dbt_manifest = DbtManifest(
            self.dbt_project_file_path.parent / "target" / "manifest.json"
        )

    @staticmethod
    def do_file_match(
        pattern: str,
        file: Path,
    ) -> bool:
        return re.match(pattern, str(file))


class DbtManifest:
    def __init__(self, manifest_path: str) -> None:
        self.manifest_path = manifest_path
        with open(self.manifest_path, "r") as f:
            self.manifest_dict = json.load(f)
            f.close()
