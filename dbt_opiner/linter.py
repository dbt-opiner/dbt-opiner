import re
import sys
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from loguru import logger

from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.file_handlers import FileHandler

if TYPE_CHECKING:
    from dbt_opiner.opinions.opinions_pack import OpinionsPack


class OpinionSeverity(Enum):
    """Enum class to represent the severity of an opinion.

    Attributes:
        MUST: The opinion must be followed.
        SHOULD: The opinion should be followed.
    """

    MUST = (1, "must")
    SHOULD = (2, "should")

    def __init__(self, num: int, text: str) -> None:
        """
        Args:
            num: The numerical value of the severity.
            text: The text value of the severity.
        """
        self.num = num
        self.text = text

    @property
    def value(self) -> str:
        """Returns the text value of the severity."""
        return self.text


@dataclass
class LintResult:
    """Dataclass to hold the result of a linting operation.

    Attributes:
        file: The file handler that was linted.
        opinion_code: The code of the opinion that was checked.
        passed: True if the opinion passed, False otherwise.
        severity: The severity of the opinion.
        message: The message of the opinion check.
    """

    file: FileHandler
    opinion_code: str
    passed: bool
    severity: OpinionSeverity
    message: str

    def __gt__(self, other):
        """Compare two LintResult objects by severity and opinion code."""
        if self.severity.num != other.severity.num:
            return self.severity.num > other.severity.num
        else:
            return self.opinion_code > other.opinion_code


class Linter:
    """Perform linting operations on dbt project files and log the results.

    Methods:
        lint_file: Lint a file with the loaded opinions.
        get_lint_results: Get the lint results sorted by severity and opinion code.
        log_results_and_exit: Log the lint results and exit with the appropriate code.

    """

    def __init__(self, opinions_pack: "OpinionsPack", no_ignore: bool = False) -> None:
        """
        Args:
            opinions_pack: OpinionsPack object containing the opinions to be checked.
            no_ignore: If True, ignore all the no qa configs.
        """
        self._lint_results = []
        self._no_ignore = no_ignore
        self._config = ConfigSingleton().get_config()
        self.opinions = opinions_pack.get_opinions()

    def lint_file(self, file: FileHandler) -> None:
        """Lint a file with the loaded opinions and add the result to the lint results.

        Args:
            file: The file handler to be linted.
        """
        logger.debug(f"Linting file {file.path}")

        for opinion in self.opinions:
            if not self._no_ignore:
                # Check file no_qa
                if opinion.code in file.no_qa_opinions or "all" in file.no_qa_opinions:
                    logger.debug(f"Skipping opinion {opinion.code} because of noqa")
                    continue
                # Check opinions_config>ignore_files
                ignore_files = (
                    self._config.get("opinions_config", {})
                    .get("ignore_files", {})
                    .get(opinion.code)
                )
                if ignore_files:
                    if re.match(ignore_files, str(file)):
                        logger.debug(f"Skipping opinion {opinion.code} because of noqa")
                        continue

            logger.debug(f"Checking opinion {opinion.code}")

            lint_result = opinion.check_opinion(file)
            if lint_result:
                logger.debug(f"Lint Result: {lint_result}")

                # yaml files that can have multiple dbt nodes
                # so sometimes lint results are a list for the same yml file but different nodes
                if isinstance(lint_result, list):
                    for result in lint_result:
                        self._lint_results.append((result, file.path))
                else:
                    self._lint_results.append((lint_result, file.path))

    def get_lint_results(self) -> list[tuple[FileHandler, LintResult]]:
        """Returns a tuple of file and lint results sorted by severity and
        opinion code (alphabetically).
        Files can be in different order and not necessarily together if
        more than one opinion for the file fails.
        """
        return sorted(self._lint_results, key=lambda x: x[0])

    def log_results_and_exit(self) -> None:
        """Log the results of the linting and exit with the appropriate code."""
        # Change logger setup to make messages more clear
        original_logger_config = logger._core.handlers.copy().get(1)
        logger.remove()
        logger_id = logger.add(
            original_logger_config._sink,
            level=original_logger_config._levelno,
            colorize=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}\n{message}</level>",
        )

        exit_code = 0

        for result, file_path in self.get_lint_results():
            message = f"{result.opinion_code} | {result.message}\n{file_path}"
            if not result.passed:
                if result.severity == OpinionSeverity.MUST:
                    exit_code = 1
                    logger.error(message)
                if result.severity == OpinionSeverity.SHOULD:
                    logger.warning(message)
            if result.passed:
                logger.debug(message)

        logger.remove(logger_id)
        logger.add(sys.stdout, level=original_logger_config._levelno)
        logger.debug(f"Exit with code: {exit_code}")
        sys.exit(exit_code)
