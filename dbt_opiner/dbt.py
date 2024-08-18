import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import sqlglot
from loguru import logger
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer.scope import find_in_scope

from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.file_handlers import MarkdownFileHandler
from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler

MATCH_ALL = r".*"


class DbtProject:
    """Class to represent a dbt project and its artifacts.

    Attributes:
        dbt_project_file_path: The path to the dbt_project.yml file.
        dbt_project_config: The configuration of the dbt project.
        dbt_profile_path: The path to the profiles.yml file.
        dbt_manifest: The dbt manifest object.
        files: A dictionary with the files in the dbt project.

    """

    def __init__(
        self,
        dbt_project_file_path: Path,
        files: list = [],
        all_files: bool = True,
        target: str = None,
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

        # TODO: Load catalog

        # Load files
        self.files = dict(sql=[], yaml=[], markdown=[])
        if all_files:
            self._init_all_files()
        else:
            self._init_files(files)

    def _init_all_files(self):
        """Create an object for every sql, yaml, and markdown file in the dbt project
        and add it to self.files dictionary.
        """
        files = (self.dbt_project_file_path.parent).rglob("*")
        self._init_files(files)

    def _init_files(self, files: list[Path]):
        """Initialize only the list of files pased. Useful for git diff files loading.

        Args:
            files: A list of files to load.
        """
        for file in files:
            if file.is_file():
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
                        sql_file = SqlFileHandler(file, self.dbt_manifest, self)
                        self.files["sql"].append(sql_file)

                elif file.suffix in [".yml", ".yaml"]:
                    try:
                        file_pattern = self._config.get("files", {})["yaml"]
                    except KeyError:
                        file_pattern = self._config.get("files", {}).get(
                            "yml", MATCH_ALL
                        )
                    if re.match(file_pattern, str(file)):
                        yaml_file = YamlFileHandler(file, self.dbt_manifest, self)
                        self.files["yaml"].append(yaml_file)

                elif file.suffix == ".md":
                    file_pattern = self._config.get("files", {}).get("md", MATCH_ALL)
                    if re.match(file_pattern, str(file)):
                        self.files["markdown"].append(MarkdownFileHandler(file, self))
                else:
                    logger.debug(f"{file.suffix} is not supported. Skipping.")

    def _load_manifest(self, force_compile=False):
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

        self.dbt_manifest = DbtManifest(manifest_path)


class DbtManifest:
    """Class to represent a dbt manifest file.

    Attributes:
        manifest_dict: The dictionary representation of the manifest file.
        nodes: A list of dbt nodes in the manifest.
        macros: A list of dbt macros in the manifest.

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
        dialect = ConfigSingleton().get_config().get("sqlglot_dialect")
        # For now only a few elements of the manifest are defined as Attributes
        # If more are required they can be added or the manifest_dict can be used instead.
        self.nodes = [
            DbtNode(node, dialect) for node in self.manifest_dict.get("nodes").values()
        ]
        self.macros = [
            DbtNode(node, dialect) for node in self.manifest_dict.get("macros").values()
        ]


class DbtCatalog:
    # TODO
    pass


class DbtNode:
    """Class to represent a dbt node or a macro in the manifest file.

    The underlying structure is a dictionary but some Attributes are
    defined as properties for convenience.

    Attributes:
        schema: The schema of the node.
        alias: The alias of the node.
        type: The type of the node.
        original_file_path: The path to the original file of the node.
        compiled_code: The compiled code of the node.
        docs_yml_file_path: The path to the docs yml file of the node.
        description: The description of the node.
        columns: The columns of the node available in the manifest.json
        unique_key: The unique key of the node.
        sql_code_ast: The sqlglot Abstract Syntax Tree (AST) of the compiled code.
        ast_extracted_columns: The columns extracted from the sql code AST.

    These Dict methods are also available:
        keys: Get the keys of the node.
        values: Get the values of the node.
        items: Get the items of the node.
        get: Get the value of a key in the node.
    """

    def __init__(self, node: dict, sql_dialect: str = None) -> None:
        """
        Args:
            node: The dictionary representation of the dbt node.
        """
        self._node = node
        self._sql_code_ast = None
        self._sql_dialect = sql_dialect

    @property
    def schema(self):
        return self._node.get("schema")

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
    def docs_yml_file_path(self) -> str | None:
        if self._node.get("patch_path"):
            return self._node.get("patch_path").replace("://", "/")
        return None

    @property
    def description(self):
        return self._node.get("description")

    @property
    def columns(self):
        return self._node.get("columns")

    @property
    def unique_key(self):
        return self._node.get("config").get("unique_key")

    @property
    def sql_code_ast(self) -> sqlglot.expressions.Select:
        """Returns the sqlglot Abstract Syntax Tree for the compiled sql code.
        See more about AST at: https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md
        """
        if self._sql_code_ast is None and self.compiled_code:
            try:
                sqlglot.transpile(self.compiled_code, read=self._sql_dialect)
            except sqlglot.errors.ParseError as e:
                logger.error(f"Malformed SQL code:\n{e}")
            else:
                self._sql_code_ast = sqlglot.parse_one(
                    self.compiled_code, dialect=self._sql_dialect
                )

        return self._sql_code_ast

    @property
    def ast_extracted_columns(self):
        """Returns the columns extracted from the sql code ast."""
        if self.sql_code_ast:
            return self._extract_columns_from_ast()
        return []

    def _extract_columns_from_ast(self):
        columns = []
        for column in qualify(self.sql_code_ast).selects:
            # If there's a select * in the final CTE
            if column.is_star:
                root = build_scope(qualify(column.parent))
                try:
                    # If the column is a `select *` from a directly referenced table
                    columns.append(
                        f"* from {find_in_scope(root.expression, sqlglot.exp.Table).name}"
                    )
                except AttributeError:
                    # If the column is a `select * from (select * from table)`
                    columns.append("* from nested query")
            else:
                columns.append(column.alias)
        return columns

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

    def get(self, key, default=None):
        return self._node.get(key, default)


class DbtProjectLoader:
    """Load dbt projects and files from a cloned git repository.

    Methods:
      initialize_dbt_projects: Initialize dbt projects with all files or only the changed ones.
    """

    def __init__(self, target: str = None, force_compile: bool = False):
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

        for dbt_project_file_path in dbt_projects_file_paths:
            dbt_project = DbtProject(
                dbt_project_file_path=dbt_project_file_path,
                target=self._target,
                force_compile=self._force_compile,
            )
            dbt_projects.append(dbt_project)
        return dbt_projects

    def _get_dbt_projects_changed_files(
        self, changed_files: list[Path]
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
                logger.debug(f"Found dbt_project.yml for file {file}")
                file_to_project_map[dbt_project_file_path].append(file)
            else:
                logger.debug(f"No dbt_project.yml found for file {file}")

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
        self, changed_files: list[str] = None, all_files: bool = False
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
            files = []
            for file in changed_files:
                path = Path(file)
                if path.is_dir():
                    files.extend(path.rglob("*"))
                else:
                    files.append(path)
            dbt_projects = self._get_dbt_projects_changed_files(files)

        return dbt_projects

    def _find_dbt_project_yml(self, file: Path) -> Path | None:
        """Given a file path, find the dbt_project.yml file in the directory tree.
        Only traverse up the directory tree until the git root directory.

        Args:
            file: The file path to start the search from.

        Returns: If found, the path to the dbt_project.yml file, otherwise None.
        """
        git_root_path = self._find_git_root(file)
        if git_root_path:
            current_path = file.resolve()
            while current_path != git_root_path:
                if (current_path / "dbt_project.yml").exists():
                    return current_path / "dbt_project.yml"
                current_path = current_path.parent

            # Also check the git root directory itself
            if (git_root_path / "dbt_project.yml").exists():
                return git_root_path / "dbt_project.yml"

        logger.debug("Not a git repository")
        return None

    def _find_all_dbt_project_ymls(self) -> list[Path] | None:
        """Find all dbt_project.yml files in a git repository.
        Ignore nested dbt_project.yml that dbt_packages have, by keeping the files
        that are closest to the git root directory.

        Returns: If found, a list of dbt_project.yml file paths, otherwise None.
        """
        git_root_path = self._find_git_root(Path(os.getcwd()))

        if git_root_path:
            dbt_project_ymls = []
            for root, dirs, files in os.walk(git_root_path, topdown=True):
                # Skip .venv directories
                if ".venv" in root:
                    continue
                if "dbt_project.yml" in files:
                    dbt_project_ymls.append(Path(root) / "dbt_project.yml")
                    # Clear dirs to stop os.walk from traversing down this directory
                    # to avoid finding nested dbt projects from dbt_packages
                    dirs[:] = []
                    continue
            return dbt_project_ymls
        logger.debug("Not a git repository")
        return None

    @staticmethod
    def _find_git_root(path: Path) -> Path | None:
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
        logger.debug("Not a git repository")
        return None


def run_dbt_command(
    command: str,
    dbt_project_file_path: Path,
    dbt_profile_path: Path = None,
    target: str = None,
) -> subprocess.CompletedProcess:
    """Run dbt command for the given dbt project file path.

    Args:
        dbt_project_file_path: The path to the dbt project file.
        command: The dbt command to run.
        target: The target to run the dbt command.
        dbt_profile_path: The path to the dbt profile file.

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
        error_message = f"{e.stdout.decode('utf-8')}\n{e.stderr.decode('utf-8')}"
        logger.critical(f"Error running dbt command: \n{error_message}")
        sys.exit(1)

    # Reset working directory
    os.chdir(current_working_dir)
    return result


def compile_dbt_manifest(
    dbt_project_file_path: Path, dbt_profile_path: Path = None, target: str = None
):
    """Compile the dbt manifest file for the given dbt project file path.
    It runs deps, seed, and compile dbt commands that are required to generate the manifest file.

    Args:
        dbt_project_file_path: The path to the dbt project file.
        dbt_profile_path: The path to the dbt profile file.
        target: The target to run the dbt command
    """
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
