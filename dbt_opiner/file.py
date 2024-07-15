from abc import ABC, abstractmethod
from enum import Enum
import yaml

class FileType(Enum):
    SQL = "sql"
    YAML = "yaml"
    YML = "yaml"
    MARKDOWN = "md"

class IncorrectFileExtensionError(Exception):
    def __init__(self, expected_extension, actual_extension):
        super().__init__(f"Incorrect file extension: expected .{expected_extension}, got .{actual_extension}")

class FileHandlerMixin(ABC):
    def __init__(self, file_path):
        self.file_path = file_path

    def has_correct_extension(self, file_type: FileType):
        return self.file_path.endswith(f".{file_type.value}")
    
    @abstractmethod
    def read(self):
        pass


class SQLFileHandler(FileHandlerMixin):
    # Read SQL file but also parse with sqlglot
    def read(self):
        self.has_correct_extension(FileType.SQL)
        with open(self.file_path, 'r') as file:
            return file.read()

class YAMLFileHandler(FileHandlerMixin):
    def read(self):
        self.has_correct_extension(FileType.YAML)
        with open(self.file_path, 'r') as file:
            return yaml.safe_load(file)

class MarkdownFileHandler(FileHandlerMixin):
    def read(self):
        self.has_correct_extension(FileType.MARKDOWN)
        with open(self.file_path, 'r') as file:
            return file.read()
