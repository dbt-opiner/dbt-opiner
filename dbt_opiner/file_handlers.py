import re
import sys
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from loguru import logger


if TYPE_CHECKING:
    from dbt_opiner.dbt import DbtManifest
    from dbt_opiner.dbt import DbtProject


class FileHandler(ABC):
    """Abstract class for handling files.

    Attributes:
        path: Path to the file.
        type: File extension.
        content: Raw content (as a string) of a file.
        no_qa_opinions: List of no_qa_opinions in the file content or related files
        parent_dbt_project: Parent dbt project of the file.
    """

    def __init__(
        self, file_path: Path, parent_dbt_project: "DbtProject" = None
    ) -> None:
        """
        Args:
            file_path: Path to the file.
            parent_dbt_project: Parent dbt project of the file.
        """
        try:
            assert file_path.exists()
        except AssertionError:
            raise FileNotFoundError(f"{file_path} does not exist")
        self.path = file_path
        self.type = self.path.suffix
        self._content = None
        self.no_qa_opinions = self._get_no_qa_opinions(self.content)
        self.parent_dbt_project = parent_dbt_project

    @property
    def content(self):
        """Reads the file content and returns it as a string."""
        if self._content is None:
            self._content = self._read_content()
        return self._content

    @staticmethod
    def _get_no_qa_opinions(content: str) -> list[str]:
        """Get the no_qa_opinions from a string.
        Args:
            content: String to search for no_qa_opinions.
        Returns:
            List of no_qa_opinions.
        """
        if re.search(r"noqa: dbt-opiner all", content):
            return ["all"]
        matches = re.findall(r"noqa: dbt-opiner ([\w\d, ]+)", content)
        if matches:
            return matches[0].split(", ")
        return []

    def _add_no_qa_opinions_from_other_file(self, other_file_path: str) -> None:
        """Add no_qa_opinions from another file to the current file.

        We use this to add the no_qa_opinions from SQL and YAML files that are related.

        Args:
            other_file_path: Str path to the file to get the no_qa_opinions from.
                             It's a string because dbt provides incomplete paths and we need to reconstruct them.
        """

        current_file_parts = list(self.path.resolve().parent.parts)
        other_file_parts = list(Path(other_file_path).parts)  # Comes from manifest
        index = current_file_parts.index(other_file_parts[0])
        final_path = current_file_parts[:index] + other_file_parts

        sql_file_path = Path(*final_path)
        with sql_file_path.open("r") as file:
            content = file.read()
        self.no_qa_opinions.extend(self._get_no_qa_opinions(content))

    def _read_content(self):
        try:
            with self.path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading file: {e}")

    def __repr__(self):
        return f"FileHander({self.path})"

    def __str__(self):
        return f"{self.path}"


class SqlFileHandler(FileHandler):
    """Class for handling SQL files.
    It expects the SQL file to be a model, macro, or test of a dbt project.

    Attributes:
        path: Path to the file.
        type: File extension.
        content: Raw content (as a string) of a file.
        no_qa_opinions: List of no_qa_opinions in the file content or related files
        parent_dbt_project: Parent dbt project of the file.
        dbt_node: DbtNode object associated with the SQL file.
        compiled_code: Compiled code of the dbt node
    """

    def __init__(
        self,
        file_path: Path,
        dbt_manifest: "DbtManifest" = None,
        parent_dbt_project: "DbtProject" = None,
    ) -> None:
        """
        Args:
            file_path: Path to the SQL file.
            dbt_manifest:  dbt manifest to search the DbtNode object associated with the SQL file.
            parent_dbt_project: Parent dbt project of the file.
        """
        # Trying to instantiate SqlFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".sql"
        except AssertionError:
            raise ValueError(
                f"SqlFileHandler requires a .sql file, got {file_path.suffix}"
            )
        super().__init__(file_path, parent_dbt_project)
        self.dbt_node = None

        # Add dbt node to the file handler
        # Check if it's a model or a macro .sql file
        if "{%macro" in self.content.replace(" ", ""):
            self.dbt_node = next(
                (
                    node
                    for node in dbt_manifest.macros
                    if node.original_file_path in str(self.path)
                ),
                None,
            )

        else:
            # It's a model or a test
            self.dbt_node = next(
                (
                    node
                    for node in dbt_manifest.nodes
                    if node.original_file_path in str(self.path)
                ),
                None,
            )

        if not self.dbt_node:
            logger.critical(
                (
                    f"Node not found for {self.path}. Try running dbt compile to "
                    "generate the manifest file, or make sure the file is part of a "
                    "well formed dbt project."
                )
            )
            sys.exit(1)

        # Add no_qa_opinions from the docs yml file
        if self.dbt_node.docs_yml_file_path:
            self._add_no_qa_opinions_from_other_file(self.dbt_node.docs_yml_file_path)
        # TODO: Add catalog entry to the file handler


class YamlFileHandler(FileHandler):
    """Class for handling YAML files.

    Attributes:
        path: Path to the file.
        type: File extension.
        content: Raw content (as a string) of a file.
        no_qa_opinions: List of no_qa_opinions in the file content or related files
        parent_dbt_project: Parent dbt project of the file.
        dbt_nodes: List of DbtNode objects for which the YAML file is a patch (contains docs of).

    Methods:
        to_dict: Returns the YAML file content as a dictionary.
        get: Returns the value of a key in the YAML file content.
    """

    def __init__(
        self,
        file_path: Path,
        dbt_manifest: "DbtManifest" = None,
        parent_dbt_project: "DbtProject" = None,
    ) -> None:
        """
        Args:
            file_path: Path to the YAML file.
            dbt_manifest: dbt manifest to search in which the YAML file is a patch (contains docs of).
        """
        # Trying to instantiate YamlFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".yml" or file_path.suffix == ".yaml"
        except AssertionError:
            raise ValueError(
                f"YamlFileHandler requires a .yml or .yaml file, got {file_path.suffix}"
            )
        super().__init__(file_path, parent_dbt_project)
        self._dict = None
        self.type = ".yaml"

        # Search for the node in the manifest by the file name in patch
        # A yml file can have more than one dbt node
        self.dbt_nodes = []
        if dbt_manifest:
            self.dbt_nodes = [
                node
                for node in dbt_manifest.nodes
                if str(node.docs_yml_file_path) in str(file_path)
            ]

            # If we have a dbt node, get the no_qa_opinions from the sql file
            # and append to the ignored opinions of the yaml file
            if self.dbt_nodes:
                for node in self.dbt_nodes:
                    self._add_no_qa_opinions_from_other_file(node.original_file_path)

    def to_dict(self) -> dict:
        """Returns the YAML file content as a dictionary."""
        if self._dict is None:
            try:
                self._dict = yaml.safe_load(self.content)
            except yaml.YAMLError as e:
                raise RuntimeError(f"Error parsing YAML file: {e}")
            except Exception as e:
                raise RuntimeError(f"Error reading YAML file: {e}")
        return self._dict

    def get(self, key, default=None):
        """Returns the value of a key in the YAML file content.
        Args:
            key: Key to get the value of.
            default: Default value to return if the key is not found. Defaults to None.
        """
        return self.to_dict().get(key, default)


class MarkdownFileHandler(FileHandler):
    """Class for handling Markdown files.

    Attributes:
        path: Path to the file.
        type: File extension.
        content: Raw content (as a string) of a file.
        no_qa_opinions: List of no_qa_opinions in the file content or related files
        parent_dbt_project: Parent dbt project of the file.
        content: Raw content (as a string) of a file.
    """

    def __init__(
        self, file_path: Path, parent_dbt_project: "DbtProject" = None
    ) -> None:
        """
        Args:
            file_path: Path to the Markdown file.
            parent_dbt_project: Parent dbt project of the file.
        """
        # Trying to instantiate MarkdownFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".md"
        except AssertionError:
            raise ValueError(
                f"MarkdownFileHandler requires a .md file, got {file_path.suffix}"
            )
        super().__init__(file_path, parent_dbt_project)
