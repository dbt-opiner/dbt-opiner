from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from dbt_opiner.dbt import DbtNode


class FileHandler(ABC):
    """Abstract class for handling files.

    Attributes:
        file_path: Path to the file.
        file_type: File extension.
        content: Raw content (as a string) of a file.
    """

    def __init__(self, file_path: Path):
        """
        Args:
            file_path: Path to the file.
        """
        try:
            assert file_path.exists()
        except AssertionError:
            raise FileNotFoundError(f"{file_path} does not exist")
        self.file_path = file_path
        self.file_type = self.file_path.suffix
        self._content = None

    @property
    def content(self):
        """Reads the file content and returns it as a string."""
        if self._content is None:
            self._content = self._read_content()
        return self._content

    def _read_content(self):
        try:
            with self.file_path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading file: {e}")

    def __repr__(self):
        return f"FileHander({self.file_path})"

    def __str__(self):
        return f"{self.file_path}"


class SQLFileHandler(FileHandler):
    """Class for handling SQL files.
    It expects the SQL file to be a model, macro, or test of a dbt project.

    Attributes:
        dbt_node: DbtNode object associated with the SQL file.
        compiled_code: Compiled code of the dbt node

    Methods:
        set_dbt_node: Sets the dbt_node attribute.
    """

    def __init__(self, file_path: Path, dbt_node: "DbtNode" = None) -> None:
        """
        Args:
            file_path: Path to the SQL file.
            dbt_node: DbtNode object associated with the SQL file. Defaults to None.
                      Can be later set with the set_dbt_node method.
        """
        # Trying to instantiate SQLFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".sql"
        except AssertionError:
            raise ValueError(
                f"SQLFileHandler requires a .sql file, got {file_path.suffix}"
            )
        self.dbt_node = dbt_node
        super().__init__(file_path)

    @property
    def compiled_code(self) -> str:
        """Returns the compiled code of the dbt node associated with the SQL file."""
        if self.dbt_node:
            return self.dbt_node.compiled_code
        return None

    # TODO: add sqlglot parsing of compiled code

    def set_dbt_node(self, dbt_node):
        """Sets the dbt_node attribute.

        Args:
            dbt_node: DbtNode object associated with the SQL file.
        """
        self.dbt_node = dbt_node


class YamlFileHandler(FileHandler):
    """Class for handling YAML files.

    Attributes:
        dbt_nodes: List of DbtNode objects for which the YAML file is a patch (contains docs of).

    Methods:
        to_dict: Returns the YAML file content as a dictionary.
        get: Returns the value of a key in the YAML file content.
    """

    def __init__(self, file_path: Path, dbt_nodes: list = None) -> None:
        """
        Args:
            file_path: Path to the YAML file.
            dbt_nodes: List of DbtNode objects for which the YAML file is a patch (contains docs of). Defaults to None.
        """
        # Trying to instantiate YamlFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".yml" or file_path.suffix == ".yaml"
        except AssertionError:
            raise ValueError(
                f"YamlFileHandler requires a .yml or .yaml file, got {file_path.suffix}"
            )
        self.dbt_nodes = dbt_nodes
        self._dict = None
        super().__init__(file_path)
        self.file_type = ".yaml"

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
        content: Raw content (as a string) of a file.
    """

    def __init__(self, file_path: Path):
        # Trying to instantiate MarkdownFileHandler with another extension should fail
        try:
            assert file_path.suffix == ".md"
        except AssertionError:
            raise ValueError(
                f"MarkdownFileHandler requires a .md file, got {file_path.suffix}"
            )
        super().__init__(file_path)
