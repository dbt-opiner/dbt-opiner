import abc
from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter


class BaseOpinion(abc.ABC):
    """The base class for an opinion.

    Attributes:
        code: The identifier for this opinion, used in inclusion
            or exclusion.
        description: A human readable description of what this
            opinion does. It will be displayed when any violations are found.
        severity: The severity of the opinion. It can be one of should or must.
            Should is a suggestion, must is an obligation
        tags: List of tags to identify group of opinions
    """

    # To install required dependencies packages for custom opinions this must be specified in children classes
    # It is done like this because:
    #  - there are different ways of installing packages in python projects
    #  - if an opinion is ignored and not loaded, we don't want to install the packages
    required_dependencies: list[str] = []

    def __init__(
        self,
        code: str,
        description: str,
        severity: linter.OpinionSeverity,
        config: dict[str, Any] = {},
        tags: list[str] = [],
    ) -> None:
        """
        Args:
        code: The identifier for this opinion, used in inclusion
            or exclusion.
        description: A human readable description of what this
            opinion does. It will be displayed when any violations are found.
        severity: The severity of the opinion. It can be one of should or must.
            Should is a suggestion, must is an obligation
        config: Configuration dict with optional extra configuration for the opinion.
        tags: List of tags to identify group of opinions
        """
        self.code = code
        self.description = description
        self.severity = severity
        self.tags = tags
        self._config = config

    def check_opinion(
        self,
        file: file_handlers.FileHandler,
    ) -> Optional[linter.LintResult | list[linter.LintResult]]:
        """The method that will be called to evaluate the opinion.

        Args:
            file: The the file to evaluate.

        Returns:
            A single ListResult with the evaluation result of the opinion.
            A list if the result evaluates more than one dbt node.
        """
        # Public interface that provides better encapsulation, flexibility,
        # consistency, extensibility, and ease of testing.
        result = self._eval(file)

        # Add opinion tags to the result and check that the result is a linter.LintResult
        if isinstance(result, linter.LintResult):
            result.tags = self.tags
            return result

        if isinstance(result, list):
            for res in result:
                if not isinstance(res, linter.LintResult):
                    return None
                res.tags = self.tags or ["not tagged"]
            return result
        return None

    @abc.abstractmethod
    def _eval(
        self,
        file: file_handlers.FileHandler,
    ) -> Optional[linter.LintResult | list[linter.LintResult]]:
        """
        The method that will contain all the logic of the opinon evaluation.
        Should be implemented in the child class.

        Args:
            file: The the file to evaluate.

        Returns:
            A single ListResult with the evaluation result of the opinion.
            A list if the result evaluates more than one dbt node.
        """
