from abc import ABC, abstractmethod
import yaml
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_opiner.dbt_artifacts import DbtNode


class FileHandler(ABC):
    def __init__(self, file_path: Path):
        try:
            assert file_path.exists()
        except AssertionError:
            raise FileNotFoundError(f"{file_path} does not exist")
        self.file_path = file_path
        self.file_type = self.file_path.suffix
        self._content = None

    @property
    def content(self):
        if self._content is None:
            self._content = self._read_content()
        return self._content

    @abstractmethod
    def _read_content(self):
        pass

    def __repr__(self):
        return f"FileHander({self.file_path})"

    def __str__(self):
        return f"{self.file_path}"


class SQLFileHandler(FileHandler):
    # Add reading the node in manifest
    def __init__(self, file_path: Path, dbt_node: "DbtNode" = None):
        self.dbt_node = dbt_node
        super().__init__(file_path)

    @property
    def compiled_code(self):
        if self.dbt_node:
            return self.dbt_node.compiled_code
        return None

    def set_dbt_node(self, dbt_node):
        self.dbt_node = dbt_node

    def _read_content(self) -> str:
        try:
            with self.file_path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading SQL file: {e}")


class YamlFileHandler(FileHandler):
    # Add reading the node in manifest
    def __init__(self, file_path: Path, dbt_nodes: list = None):
        self.dbt_nodes = dbt_nodes
        super().__init__(file_path)

    def _read_content(self) -> dict:
        with self.file_path.open("r") as file:
            return file.read()

    def to_dict(self):
        try:
            yaml.safe_load(self.content)
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error reading YAML file: {e}")


class MarkdownFileHandler(FileHandler):
    def _read_content(self) -> str:
        try:
            with self.file_path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading Markdown file: {e}")
