import json
import os
import pathlib
import re
import subprocess
from collections import defaultdict
from typing import Any
from typing import ItemsView
from typing import KeysView
from typing import Optional
from typing import TypedDict
from typing import ValuesView

import sqlglot
import yaml
from loguru import logger
from sqlglot.optimizer import qualify
from sqlglot.optimizer import scope

from dbt_opiner import config_singleton
from dbt_opiner import file_handlers

MATCH_ALL = r".*"


class DbtProject:
    """Class to represent a dbt project and its artifacts.

    Attributes:
        name: The name of the dbt project.
        dbt_project_dir_path: The path to the dbt project directory (where dbt_project.yml file is).
        dbt_project_file_path: The path to the dbt_project.yml file.
        dbt_project_config: The configuration of the dbt project.
        dbt_profile_path: The path to the profiles.yml file.
        dbt_profile: The configuration of the dbt profile.
        dbt_manifest: The dbt manifest object.
        files: A dictionary with the files in the dbt project.

    """

    def __init__(
        self,
        dbt_project_file_path: pathlib.Path,
        files: list[pathlib.Path] = [],
        all_files: bool = False,
        target: Optional[str] = None,
        force_compile: bool = False,
    ) -> None:
        """
        Args:
            dbt_project_file_path: The path to the dbt_project.yml file.
            files: A list of files to load.
            all_files: A flag to load all files in the dbt project.
            target: The target to run the dbt command.
            force_compile: A flag to force compile the dbt manifest file
        """

        self._target = target

        # Set config
        self._config = config_singleton.ConfigSingleton().get_config()
        # Set dbt project file
        try:
            assert dbt_project_file_path.exists()
        except AssertionError:
            raise FileNotFoundError(f"{dbt_project_file_path} does not exist")

        self.dbt_project_file_path = dbt_project_file_path
        self.dbt_project_dir_path = dbt_project_file_path.parent
        self.dbt_project_config = self._load_yaml_file(dbt_project_file_path)
        self.name = self.dbt_project_config.get("name")

        # Profiles can be none if it's specified in personal folder
        # We won't take care of that because is not a very good practice
        self.dbt_profile_path = None
        self.dbt_profile: dict[str, Any] = {}
        if (dbt_project_file_path.parent / "profiles.yml").exists():
            self.dbt_profile_path = dbt_project_file_path.parent / "profiles.yml"
            self.dbt_profile = self._load_yaml_file(self.dbt_profile_path)

        # Load manifest
        self._load_manifest(force_compile)

        # TODO: Load catalog

        # Load files
        self.files: dict[str, list[file_handlers.FileHandler]] = dict(
            sql=[], yaml=[], markdown=[]
        )
        if all_files:
            self._init_all_files()
        else:
            self._init_files(files)

    def _init_all_files(self) -> None:
        """Create an object for every sql, yaml, and markdown file in the dbt project
        and add it to self.files dictionary.
        """
        files = list((self.dbt_project_file_path.parent).rglob("*"))
        self._init_files(files)

    def _init_files(self, files: list[pathlib.Path]) -> None:
        """Initialize only the list of files pased. Useful for git diff files loading.

        Args:
            files: A list of files to load.
        """
        for file in files:
            # Ignore files inside target directory
            if (
                (
                    self.dbt_project_file_path.parent
                    / self.dbt_project_config.get("target-path", "target")
                ).resolve()
                in file.resolve().parents
                # Ignore files inside dbt deps directory
                or (
                    self.dbt_project_file_path.parent
                    / self.dbt_project_config.get(
                        "packages-install-path", "dbt_packages"
                    )
                ).resolve()
                in file.resolve().parents
                # Ignore files inside .venv directory
                or (self.dbt_project_file_path.parent / ".venv").resolve()
                in file.resolve().parents
            ):
                continue

            if file.suffix == ".sql":
                if re.match(
                    self._config.get("files", {}).get("sql", MATCH_ALL), str(file)
                ):
                    sql_file = file_handlers.SqlFileHandler(
                        file_path=file, parent_dbt_project=self
                    )
                    self.files["sql"].append(sql_file)

            elif file.suffix in [".yml", ".yaml"]:
                try:
                    file_pattern = self._config.get("files", {})["yaml"]
                except KeyError:
                    file_pattern = self._config.get("files", {}).get("yml", MATCH_ALL)
                if re.match(file_pattern, str(file)):
                    yaml_file = file_handlers.YamlFileHandler(
                        file_path=file, parent_dbt_project=self
                    )
                    self.files["yaml"].append(yaml_file)

            elif file.suffix == ".md":
                file_pattern = self._config.get("files", {}).get("md", MATCH_ALL)
                if re.match(file_pattern, str(file)):
                    self.files["markdown"].append(
                        file_handlers.MarkdownFileHandler(
                            file_path=file, parent_dbt_project=self
                        )
                    )
            else:
                logger.debug(f"{file.suffix} is not supported. Skipping.")

    def _load_manifest(self, force_compile: bool = False) -> None:
        """Load the dbt manifest file.

        Args:
            force_compile: If True, compile the manifest file even if it exists.
        """
        manifest_path = self.dbt_project_file_path.parent / "target" / "manifest.json"

        # Check if we need to compile the manifest either because of force_compile
        # or because the manifest does not exist.
        if force_compile or not manifest_path.exists():
            action = (
                "Force compiling"
                if force_compile
                else f"{manifest_path} does not exist. Compiling"
            )
            logger.debug(action)
            compile_dbt_manifest(
                self.dbt_project_file_path, self.dbt_profile_path, self._target
            )

        self.dbt_manifest = DbtManifest(str(manifest_path))

    @staticmethod
    def _load_yaml_file(file_path: pathlib.Path) -> dict[str, Any]:
        """Load a yaml file.

        Args:
            file_path: The path to the yaml file.

        Returns:
            A dictionary with the yaml file content.
        """
        with open(file_path, "r") as f:
            yaml_file: dict[str, Any] = yaml.safe_load(f)
            f.close()
        return yaml_file


class DbtManifest:
    """Class to represent a dbt manifest file.

    Attributes:
        manifest_dict: The dictionary representation of the manifest file.
        nodes: A dictionary of dbt nodes (models, tests, seeds..) in the manifest.
        model_nodes: A dictionary of dbt model nodes in the manifest.
        macros: A dictionary of dbt macros in the manifest.
        sources: A dictionary of dbt sources in the manifest.
        exposures: A dictionary of dbt exposures in the manifest.
    """

    def __init__(self, manifest_path: str) -> None:
        """
        Args:
            manifest_path: The path to the dbt manifest file
        """
        self._manifest_path = manifest_path
        with open(self._manifest_path, "r") as f:
            self.manifest_dict = json.load(f)
            f.close()
        dialect = config_singleton.ConfigSingleton().get_config().get("sqlglot_dialect")
        # For now only a few elements of the manifest are defined as Attributes
        # If more are required they can be added or the manifest_dict can be used instead.

        # Create a dictionary with the keys and values of the nodes,
        # macros, sources and exposures in the manifest file
        self.nodes: dict[
            str, DbtBaseNode
        ] = {}  # nodes in manifest contains models, tests, seeds..
        self.model_nodes: dict[str, DbtModel] = {}  # only keep model nodes
        self.macros: dict[str, DbtMacro] = {}
        self.sources: dict[str, DbtSource] = {}
        self.exposures: dict[str, DbtBaseNode] = {}

        self._get_nodes()
        self._get_model_nodes(dialect)
        self._get_macros()
        self._get_sources()
        self._get_exposures()

    def _get_nodes(self) -> None:
        for key, value in self.manifest_dict.get("nodes", {}).items():
            # Note: also e.g. seeds and tests are included in the nodes dict
            self.nodes[key] = DbtBaseNode(value)

    def _get_model_nodes(self, dialect: Optional[str]) -> None:
        for key, value in self.manifest_dict.get("nodes", {}).items():
            resource_type = value.get("resource_type")
            if resource_type == "model":
                self.model_nodes[key] = DbtModel(value, dialect)

    def _get_macros(self) -> None:
        for key, value in self.manifest_dict.get("macros", {}).items():
            self.macros[key] = DbtMacro(value)

    def _get_sources(self) -> None:
        for key, value in self.manifest_dict.get("sources", {}).items():
            self.sources[key] = DbtSource(value)

    def _get_exposures(self) -> None:
        for key, value in self.manifest_dict.get("exposures", {}).items():
            self.exposures[key] = DbtBaseNode(value)


class DbtCatalog:
    """Class to represent a dbt catalog file."""

    # TODO


class DbtNodeType(TypedDict, total=False):
    schema: str
    alias: str
    resource_type: str
    original_file_path: str
    compiled_code: str
    patch_path: str
    description: str
    config: dict[str, Any]
    columns: dict[str, Any]
    depends_on: dict[str, list[str]]
    database: str


class DbtBaseNode:
    """Base class to represent a dbt node, macro or source in the manifest file.

    Each dbt node is represented as a dictionary, and this class provides
    methods to access common attributes via properties for convenience.

    Attributes:
        schema: The schema of the node.
        alias: The alias of the node.
        type: The type of the node.
        original_file_path: The path to the original file of the node.
        docs_yml_file_path: The path to the docs yml file of the node.
        config: A dictionary containing configuration options for the node.
        description: The description of the node.

    These Dict methods are also available:
        keys: Get the keys of the node.
        values: Get the values of the node.
        items: Get the items of the node.
        get: Get the value of a key in the node.
    """

    def __init__(self, node: DbtNodeType) -> None:
        """
        Args:
            node: The dictionary representation of the dbt node.
        """
        self._node = node

    @property
    def schema(self) -> str:
        return self._node.get("schema", "")

    @property
    def alias(self) -> str:
        return self._node.get("alias", "")

    @property
    def type(self) -> str:
        return self._node.get("resource_type", "")

    @property
    def original_file_path(self) -> str:
        return self._node.get("original_file_path", "")

    @property
    def docs_yml_file_path(self) -> Optional[str]:
        patch_path = self._node.get("patch_path")
        return patch_path.replace("://", "/") if patch_path is not None else None

    @property
    def description(self) -> str:
        return self._node.get("description", "")

    @property
    def config(self) -> dict[str, Any]:
        return self._node.get("config", {})

    def __repr__(self) -> str:
        return f"DbtBaseNode({self.alias})"

    def __str__(self) -> str:
        return f"{self._node}"

    def keys(self) -> KeysView[str]:
        return self._node.keys()

    def values(self) -> ValuesView[Any]:
        return self._node.values()

    def items(self) -> ItemsView[str, Any]:
        return self._node.items()

    def get(
        self, key: str, default: Optional[str | dict[str, Any] | list[Any]] = None
    ) -> Any:
        return self._node.get(key, default)


class DbtModel(DbtBaseNode):
    """Class to represent a dbt model in the manifest file.

    Inherits from DbtBaseNode and includes attributes and methods specific to model nodes,
    such as handling of SQL code and AST (Abstract Syntax Tree).

    Attributes:
        compiled_code: The compiled code of the node.
        columns: The columns of the node available in the manifest.json
        unique_key: The unique key of the node.
        depends_on: The list of dependencies of the node (macros and nodes).
        sql_code_ast: The sqlglot Abstract Syntax Tree (AST) of the compiled code.
        ast_extracted_columns: The columns extracted from the sql code AST.
    """

    def __init__(self, node: DbtNodeType, sql_dialect: Optional[str] = None) -> None:
        super().__init__(node)
        self._sql_code_ast = None
        self._sql_dialect = sql_dialect

    @property
    def compiled_code(self) -> str:
        return self._node.get("compiled_code", "")

    @property
    def columns(self) -> dict[str, Any]:
        return self._node.get("columns", {})

    @property
    def unique_key(self) -> Optional[str]:
        return self.config.get("unique_key")

    @property
    def depends_on(self) -> dict[str, list[str]]:
        return self._node.get("depends_on", {})

    @property
    def sql_code_ast(self) -> Optional[sqlglot.expressions.Select]:
        """Returns the sqlglot Abstract Syntax Tree for the compiled sql code.
        See more about AST at: https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md
        """
        if self._sql_code_ast is None and self.compiled_code:
            try:
                sqlglot.transpile(self.compiled_code, read=self._sql_dialect)
            except sqlglot.errors.ParseError as e:
                logger.error(f"Malformed SQL code:\n{e}")
            else:
                self._sql_code_ast = sqlglot.parse_one(  # type: ignore
                    self.compiled_code, dialect=self._sql_dialect
                )

        return self._sql_code_ast

    @property
    def ast_extracted_columns(self) -> list[str]:
        """Returns the columns extracted from the sql code ast."""
        if self.sql_code_ast:
            return self._extract_columns_from_ast()
        return []

    def _extract_columns_from_ast(self) -> list[str]:
        columns = []
        for column in qualify.qualify(self.sql_code_ast).selects:  # type: ignore
            # If there's a select * in the final CTE
            if column.is_star:
                root = scope.build_scope(qualify.qualify(column.parent))
                try:
                    # If the column is a `select *` from a directly referenced table
                    if root:
                        columns.append(
                            f"* from {scope.find_in_scope(root.expression, sqlglot.exp.Table).name}"  # type: ignore
                        )
                except AttributeError:
                    # If the column is a `select * from (select * from table)`
                    columns.append("* from nested query")
            else:
                columns.append(column.alias)
        return columns


class DbtSource(DbtBaseNode):
    """Class to represent a dbt source in the manifest file.

    Inherits from DbtBaseNode and provides access to source-specific properties.

    Attributes:
        database: The database where the source is located.
    """

    @property
    def database(self) -> str:
        return self._node.get("database", "")


class DbtMacro(DbtBaseNode):
    """Represents a dbt macro."""

    # Add specific properties or methods for macros if needed
    pass


class DbtProjectLoader:
    """Load dbt projects and files from a cloned git repository.

    Methods:
      initialize_dbt_projects: Initialize dbt projects with all files or only the changed ones.
    """

    def __init__(self, target: Optional[str] = None, force_compile: bool = False):
        """
        Args:
          target: The target to run dbt commands.
          force_compile: A flag to force compile the dbt manifest file.
        """
        self._target = target
        self._force_compile = force_compile

    def _get_dbt_projects_all_files(self) -> list[DbtProject]:
        """
        Initialize all dbt projects and files in the git repository.

        Returns:
          A list of dbt projects with all its files loaded.
        """
        dbt_projects_file_paths = self._find_all_dbt_project_ymls()
        dbt_projects = []
        if dbt_projects_file_paths:
            dbt_projects = [
                DbtProject(
                    dbt_project_file_path=dbt_project_file_path,
                    target=self._target,
                    force_compile=self._force_compile,
                    all_files=True,
                )
                for dbt_project_file_path in dbt_projects_file_paths
            ]

        return dbt_projects

    def _get_dbt_projects_changed_files(
        self, changed_files: list[pathlib.Path]
    ) -> list[DbtProject]:
        """Initialize dbt projects with only the corresponding changed files.

        Returns:
          A list of dbt projects with the specified files loaded.
        """
        file_to_project_map = defaultdict(list)

        for file in changed_files:
            try:
                assert file.exists()
            except AssertionError:
                raise FileNotFoundError(f"{file} does not exist")
            dbt_project_file_path = self._find_dbt_project_yml(file)
            if dbt_project_file_path:
                file_to_project_map[dbt_project_file_path].append(file)

        dbt_projects = []
        for dbt_project_file_path, files in file_to_project_map.items():
            dbt_project = DbtProject(
                dbt_project_file_path=dbt_project_file_path,
                files=files,
                all_files=False,
                target=self._target,
                force_compile=self._force_compile,
            )
            dbt_projects.append(dbt_project)
        return dbt_projects

    def initialize_dbt_projects(
        self, changed_files: list[str] = [], all_files: bool = False
    ) -> list[DbtProject]:
        """Initialize dbt projects with all files or only the changed ones.

        Args:
            changed_files: A list of changed files to process.
            all_files: A flag to process all files in the repository.

        Returns:
            A list of dbt projects with the specified files loaded.
        """
        if all_files and changed_files:
            raise ValueError(
                "Cannot process all files and changed files at the same time"
            )
        if not all_files and not changed_files:
            raise ValueError("Either all files or changed files must be specified")

        if all_files:
            logger.debug("Processing all files")
            dbt_projects = self._get_dbt_projects_all_files()

        if changed_files:
            logger.debug("Processing changed files")
            # If the parameter is a path to a directory
            # get all files in the directory and its subdirectories
            files: list[pathlib.Path] = []
            for file in changed_files:
                path = pathlib.Path(file)
                if path.is_dir():
                    files.extend(path.rglob("*"))
                else:
                    files.append(path)
            dbt_projects = self._get_dbt_projects_changed_files(files)

        return dbt_projects

    def _find_dbt_project_yml(self, file: pathlib.Path) -> Optional[pathlib.Path]:
        """Given a file path, find the dbt_project.yml file in the directory tree.
        Only traverse up the directory tree until the git root directory.

        Args:
            file: The file path to start the search from.

        Returns: If found, the path to the dbt_project.yml file, otherwise None.
        """
        git_root_path = self._find_git_root(file)
        dbt_project_yml_path = None
        if git_root_path:
            current_path = file.resolve()
            while current_path != git_root_path:
                if (current_path / "dbt_project.yml").exists():
                    # Keep going up the directory tree to find the closest dbt_project.yml
                    # to git root directory. This is to avoid finding nested dbt projects.
                    # The nested file is filtered later in DbtProject._init_files method.
                    dbt_project_yml_path = current_path / "dbt_project.yml"
                current_path = current_path.parent

            if dbt_project_yml_path:
                logger.debug(
                    f"Found dbt_project.yml for file {file} at {dbt_project_yml_path}"
                )
                return dbt_project_yml_path
            logger.debug("dbt_project.yml not found")

        return None

    def _find_all_dbt_project_ymls(self) -> Optional[list[pathlib.Path]]:
        """Find all dbt_project.yml files in a git repository.
        Ignore nested dbt_project.yml that dbt_packages have, by keeping the files
        that are closest to the git root directory.

        Returns: If found, a list of dbt_project.yml file paths, otherwise None.
        """
        git_root_path = self._find_git_root(pathlib.Path(os.getcwd()))

        if git_root_path:
            dbt_project_ymls = []
            for root, dirs, files in os.walk(git_root_path, topdown=True):
                # Skip .venv directories
                if ".venv" in root:
                    continue
                if "dbt_project.yml" in files:
                    dbt_project_ymls.append(pathlib.Path(root) / "dbt_project.yml")
                    # Clear dirs to stop os.walk from traversing down this directory
                    # to avoid finding nested dbt projects from dbt_packages
                    dirs[:] = []
                    continue
            return dbt_project_ymls
        return None

    @staticmethod
    def _find_git_root(path: pathlib.Path) -> Optional[pathlib.Path]:
        """Given a path to a file or directory, find the root of the git repository.

        Args:
          path: The path to start the search from.

        Returns: The path to the git root directory, otherwise None.
        """
        current_path = path.resolve()
        while current_path != current_path.parent:
            if (current_path / ".git").exists():
                logger.debug(f"git root is: {current_path}")
                return current_path
            current_path = current_path.parent

        logger.error("Not a git repository")
        raise FileNotFoundError("Not a git repository")


def run_dbt_command(
    command: str,
    dbt_project_file_path: pathlib.Path,
    dbt_profile_path: Optional[pathlib.Path] = None,
    target: Optional[str] = None,
    silent: bool = False,
) -> subprocess.CompletedProcess[bytes]:  # pragma: no cover
    """Run dbt command for the given dbt project file path.

    Args:
        dbt_project_file_path: The path to the dbt project file.
        command: The dbt command to run.
        target: The target to run the dbt command.
        dbt_profile_path: The path to the dbt profile file.
        silent: flag to suppress logging. Use True for logging errors.

    Returns:
        The subprocess.CompletedProcess object.
    """
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

    try:
        result = subprocess.run(cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        if not silent:
            error_message = f"{e.stdout.decode('utf-8')}\n{e.stderr.decode('utf-8')}"
            logger.error(f"Error running dbt command: \n{error_message}")
        raise e
    finally:
        # Reset working directory
        os.chdir(current_working_dir)

    return result


def compile_dbt_manifest(
    dbt_project_file_path: pathlib.Path,
    dbt_profile_path: Optional[pathlib.Path] = None,
    target: Optional[str] = None,
) -> None:  # pragma: no cover
    """Compile the dbt manifest file for the given dbt project file path.
    It tries to run compile but runs deps, seed, if just compile fails.
    Sometimes those are required to run compile.

    Args:
        dbt_project_file_path: The path to the dbt project file.
        dbt_profile_path: The path to the dbt profile file.
        target: The target to run the dbt command
    """
    # Shouldn't access private attribute, but passing all the logger context is too much
    if logger._core.handlers.get(1).levelno == 10:  # type: ignore
        r = run_dbt_command(
            command="debug",
            dbt_project_file_path=dbt_project_file_path,
            dbt_profile_path=dbt_profile_path,
            target=target,
        )
        logger.debug(f"dbt debug output:\n{r.stdout.decode()}")

    try:
        run_dbt_command(
            command="compile",
            dbt_project_file_path=dbt_project_file_path,
            dbt_profile_path=dbt_profile_path,
            target=target,
            silent=True,
        )
    except subprocess.CalledProcessError:
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
