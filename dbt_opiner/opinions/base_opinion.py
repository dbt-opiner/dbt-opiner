from abc import ABC
from abc import abstractmethod

from dbt_opiner.file_handlers import MarkdownFileHandler
from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity


class BaseOpinion(ABC):
    """The base class for an opinion.

    Args:
        code: The identifier for this opinion, used in inclusion
            or exclusion.
        description: A human readable description of what this
            opinion does. It will be displayed when any violations are found.
        severity: The severity of the opinion. It can be one of should or must.
            Should is a suggestion, must is an obligation
        applies_to_file_type: The file type that this opinion applies to (sql, yml, etc.)
        applies_to_node_type: The node type that this opinion applies to (model, marco, etc.)
        config: The configuration for this opinion.
    """

    def __init__(
        self,
        code: str,
        description: str,
        severity: OpinionSeverity,
        applies_to_file_type: str,
        applies_to_node_type: str,
        config: dict = None,
    ) -> None:
        self.code = code
        self.description = description
        self.severity = severity
        self.applies_to_file_type = applies_to_file_type
        self.applies_to_node_type = applies_to_node_type
        self._config = config  # In this case we inject the config to the opinion instead of relying on the singleton.

    def check_opinion(
        self, file: SQLFileHandler | YamlFileHandler | MarkdownFileHandler
    ) -> LintResult:
        """The method that will be called to evaluate the opinion.

        Args:
            file: The the file to evaluate.

        Returns:
            bool: True if the opinion is met, False otherwise.
        """
        if file.file_type == self.applies_to_file_type:
            return self._eval(file)
        else:
            return None

    @abstractmethod
    def _eval(
        self, file: SQLFileHandler | YamlFileHandler | MarkdownFileHandler
    ) -> LintResult:
        """
        The method that contains all the logic of the opinon evaluation.

        Args:
            file: The the file to evaluate.

        Returns:
            bool: True if the opinion is met, False otherwise.
        """
        pass
