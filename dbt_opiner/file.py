from abc import ABC, abstractmethod
from enum import Enum
import yaml

class FileType(Enum):
    SQL = "sql"
    YAML = "yaml"
    YML = "yml"
    MARKDOWN = "md"

class IncorrectFileExtensionError(Exception):
    def __init__(self, expected_extension, actual_extension):
        super().__init__(f"Incorrect file extension: expected .{expected_extension}, got .{actual_extension}")

class FileHandler(ABC):
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_type = self.file_path.split('.')[-1]
        self.content = None
    
    @property
    def content(self):
        if self.content is None:
            self.content = self.read()
        return self.content()
    
    def has_correct_extension(self, file_type: FileType):
        return self.file_type == file_type.value
    
    @abstractmethod
    def _read_content(self):
        pass


class SQLFileHandler(FileHandler):
    # Read SQL file but also parse with sqlglot
    # Also, search for compiled SQL from manifest
    def _read_content(self):
        try:  
          assert self.has_correct_extension(FileType.SQL)
        except AssertionError:
          raise IncorrectFileExtensionError(FileType.SQL, self.file_path.split('.')[-1])
        with open(self.file_path, 'r') as file:
            return file.read()

class YAMLFileHandler(FileHandler):
    def _read_content(self):
        try:
          assert self.has_correct_extension(FileType.YML) or self.has_correct_extension(FileType.YAML)
        except AssertionError:
          raise IncorrectFileExtensionError(FileType.YML, self.file_path.split('.')[-1])
        with open(self.file_path, 'r') as file:
            return yaml.safe_load(file)

class MarkdownFileHandler(FileHandler):
    def _read_content(self):
        try:
          assert self.has_correct_extension(FileType.MARKDOWN)
        except AssertionError:
          raise IncorrectFileExtensionError(FileType.MARKDOWN, self.file_path.split('.')[-1])
        with open(self.file_path, 'r') as file:
            return file.read()
