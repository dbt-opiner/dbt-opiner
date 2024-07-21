from abc import ABC, abstractmethod
from pathlib import Path
import yaml


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

    def __str__(self):
        return f"{self.file_path}"


class SQLFileHandler(FileHandler):
    # Read SQL file but also parse with sqlglot
    # Also, search for compiled SQL from manifest
    def _read_content(self):
        try:
            with self.file_path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading SQL file: {e}")


class YamlFileHandler(FileHandler):
    def _read_content(self):
        try:
            with self.file_path.open("r") as file:
                return yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error reading YAML file: {e}")


class MarkdownFileHandler(FileHandler):
    def _read_content(self):
        try:
            with self.file_path.open("r") as file:
                return file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading Markdown file: {e}")
