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
    """

    # To install required packages for custom opinions this must be specified in children classes
    # It is done like this because:
    #  - there are different ways of defining packages in python projects
    #  - if an opinion is ignored and not loaded, we don't want to install the packages
    required_packages: list[str] = []

    def __init__(
        self,
        code: str,
        description: str,
        severity: OpinionSeverity,
    ) -> None:
        self.code = code
        self.description = description
        self.severity = severity

    def check_opinion(
        self, file: SQLFileHandler | YamlFileHandler | MarkdownFileHandler
    ) -> LintResult:
        """The method that will be called to evaluate the opinion.

        Args:
            file: The the file to evaluate.

        Returns:
            A LintResult with the evaluation result of the opinion.
        """
        # Public interface that provides better encapsulation, flexibility,
        # consistency, extensibility, and ease of testing.
        return self._eval(file)

    @abstractmethod
    def _eval(
        self, file: SQLFileHandler | YamlFileHandler | MarkdownFileHandler
    ) -> LintResult:
        """
        The method that will contain all the logic of the opinon evaluation.
        Should be implemented in the child class.

        Args:
            file: The the file to evaluate.

        Returns:
            A LintResult with the evaluation result of the opinion.
        """
        pass
