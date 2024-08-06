import json
import re
from pathlib import Path
from loguru import logger
from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.exceptions import DbtNodeNotFoundExeption
from dbt_opiner.file_handlers import (
    SQLFileHandler,
    YamlFileHandler,
    MarkdownFileHandler,
)
from dbt_opiner.utils import compile_dbt_manifest

MATCH_ALL = r".*"


class DbtProject:
    def __init__(
        self,
        dbt_project_file_path: Path,
        files: list = [],
        all_files: bool = True,
        target: str = None,
        force_compile: bool = False,
    ) -> None:
        self._target = target

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
        self.dbt_profile_path = None
        if (dbt_project_file_path.parent / "profiles.yml").exists():
            self.dbt_profile_path = dbt_project_file_path.parent / "profiles.yml"
        # Load manifest
        self._load_manifest(force_compile)

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

    def _init_files(self, files: list[Path]):
        """
        Initialize only the list of files pased. Useful for git diff files loading.
        """
        for file in files:
            if file.is_file():
                # Ignore files inside the target directory
                if (
                    self.dbt_project_file_path.parent / "target"
                ).resolve() in file.resolve().parents:
                    logger.debug(f"Ignoring file inside target directory: {file}")
                    continue
                if file.suffix == ".sql":
                    if re.match(
                        self._config.get("sql").get("files", MATCH_ALL), str(file)
                    ):
                        file = SQLFileHandler(file)

                        # Check if it's a model or a macro .sql file
                        if "{%macro" in file.content.replace(" ", ""):
                            dbt_node = next(
                                (
                                    node
                                    for node in self.dbt_manifest.macros
                                    if node.original_file_path in str(file.file_path)
                                ),
                                None,
                            )

                        else:
                            # It's a model (or a test, not supported yet)
                            dbt_node = next(
                                (
                                    node
                                    for node in self.dbt_manifest.nodes
                                    if node.original_file_path in str(file.file_path)
                                ),
                                None,
                            )

                        if not dbt_node:
                            raise DbtNodeNotFoundExeption(
                                f"Node not found for {file.file_path}."
                            )

                        file.set_dbt_node(dbt_node)
                        # TODO support tests

                        self.files["sql"].append(file)
                elif file.suffix in [".yml", ".yaml"]:
                    try:
                        yaml_config = self._config["yaml"]
                        file_pattern = yaml_config.get("files", MATCH_ALL)
                    except KeyError:
                        file_pattern = self._config.get("yml").get("files", MATCH_ALL)

                    if re.match(file_pattern, str(file)):
                        # Search for the node in the manifest by the file name in patch
                        # A yml file can have more than one dbt node
                        dbt_nodes = (
                            node
                            for node in self.dbt_manifest.nodes
                            if node.docs_yml_file_path in str(file)
                        )

                        self.files["yaml"].append(YamlFileHandler(file, dbt_nodes))
                elif file.suffix == ".md":
                    if re.match(
                        self._config.get("md").get("files", MATCH_ALL), str(file)
                    ):
                        self.files["markdown"].append(MarkdownFileHandler(file))
                else:
                    logger.debug(f"{file.suffix} is not supported. Skipping.")

    def _load_manifest(self, force_compile=False):
        manifest_path = self.dbt_project_file_path.parent / "target" / "manifest.json"

        # Check if we need to compile the manifest either because of force_compile
        # or because the manifest does not exist.
        if force_compile or not manifest_path.exists():
            action = (
                "Force compiling"
                if force_compile
                else f"{manifest_path} does not exist. Compiling"
            )
            logger.info(action)
            compile_dbt_manifest(
                self.dbt_project_file_path, self.dbt_profile_path, self._target
            )

        self.dbt_manifest = DbtManifest(manifest_path)


class DbtManifest:
    def __init__(self, manifest_path: str) -> None:
        self.manifest_path = manifest_path
        with open(self.manifest_path, "r") as f:
            self.manifest_dict = json.load(f)
            f.close()
        # Only keep the values of the nodes.
        # We don't need the ket to access the nodes.
        self.nodes = [
            DbtNode(node) for node in self.manifest_dict.get("nodes").values()
        ]
        self.macros = [
            DbtNode(node) for node in self.manifest_dict.get("macros").values()
        ]


class DbtNode:
    def __init__(self, node: dict) -> None:
        self._node = node

    @property
    def schema(self):
        return self.node.get("schema")

    @property
    def alias(self):
        return self._node.get("alias")

    @property
    def type(self):
        return self._node.get("resource_type")

    @property
    def original_file_path(self):
        return self._node.get("original_file_path")

    @property
    def compiled_code(self):
        return self._node.get("compiled_code")

    @property
    def docs_yml_file_path(self):
        return Path(self._node.get("patch_path").replace("://", "/"))

    @property
    def description(self):
        return self._node.get("description")

    def __repr__(self):
        return f"DbtNode({self.alias})"

    def __str__(self):
        return f"{self._node}"

    def keys(self):
        return self._node.keys()

    def values(self):
        return self._node.values()

    def items(self):
        return self._node.items()

    def get(self, key):
        return self._node.get(key)
