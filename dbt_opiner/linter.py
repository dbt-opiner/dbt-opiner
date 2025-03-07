import io
import re
import sys
from collections import defaultdict
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

from dbt_opiner import config_singleton
from dbt_opiner import file_handlers

if TYPE_CHECKING:
    from dbt_opiner.opinions.opinions_pack import OpinionsPack  # pragma: no cover


class OpinionSeverity(Enum):
    """Enum class to represent the severity of an opinion.

    Attributes:
        MUST: The opinion must be followed.
        SHOULD: The opinion should be followed.
    """

    MUST = (2, "must")
    SHOULD = (1, "should")

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

    file: file_handlers.FileHandler
    opinion_code: str
    passed: bool
    severity: OpinionSeverity
    message: str
    tags: Optional[list[str]] = None

    def __gt__(self, other: "LintResult") -> bool:
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
        self._lint_results: list[LintResult] = []
        self._no_ignore = no_ignore
        self._config = config_singleton.ConfigSingleton().get_config()
        self.opinions = opinions_pack.get_opinions()

    def lint_file(
        self,
        file: (file_handlers.FileHandler),
    ) -> None:
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
                    if re.match(ignore_files, str(file.path)):
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
                        self._lint_results.append(result)
                else:
                    self._lint_results.append(lint_result)

    def get_lint_results(self, deduplicate: bool = False) -> list[LintResult]:
        """Returns list of lint results sorted by severity and
        opinion code (alphabetically).
        Files can be in different order and not necessarily together if
        more than one opinion for the file fails.
        Args:
            deduplicate: If True, remove duplicated results from the lint results.
        Returns:
            A list with sorted lint results
        """
        if deduplicate:
            return sorted(self._deduplicate_results())

        return sorted(self._lint_results)
        # TODO: add option to organize results by opinion tags.

    def log_results_and_exit(self, output_file: Optional[str] = None) -> None:
        """Log the results of the linting and exit with the appropriate code.
        Args:
          output_file: The file to write the lint results to.
        """

        exit_code = 0
        message_lines = ["# ✨ Dbt-opiner lint results\n"]
        for result in self.get_lint_results(deduplicate=True):
            message = f"{result.opinion_code} | `{result.file.path}` {result.message}\n"
            if not result.passed:
                if result.severity == OpinionSeverity.MUST:
                    exit_code = 1
                    logger.error(message)
                    message_lines.append(f"- ❌ {message}")
                if result.severity == OpinionSeverity.SHOULD:
                    logger.warning(message)
                    message_lines.append(f"- ⚠️ {message}")
            if result.passed:
                logger.debug(message)
        if exit_code == 0:
            logger.info("All opinions passed!")
            message_lines.append("✅ All opinions passed!")

        if output_file:
            with open(output_file, "w") as f:
                f.writelines(message_lines)
        logger.debug(f"Exit with code: {exit_code}")
        sys.exit(exit_code)

    def log_audit_and_exit(
        self, type: str, format: str, output_file: Optional[str] = None
    ) -> None:
        """Log the audit results and exit.
        Args:
            type: The type of audit to perform. Can be "all", "general", "by_tag", or "detailed".
            output_file: The file to write the audit results to.
        """
        # Change logger setup to make messages more clear
        # Get exiting logger config
        original_logger_config = next(iter(logger._core.handlers.copy().values()))  # type: ignore
        logger.remove()

        # Add file sink if specified
        if output_file:
            logger.add(
                output_file,
                level=original_logger_config._levelno,
                colorize=False,
                format="{message}\n",
            )

        logger.add(
            original_logger_config._sink,
            level=original_logger_config._levelno,
            format="{message}\n",
        )

        audit_results = self._audit()

        def dataframe_to_string(df: pd.DataFrame, format_type: str) -> str:
            buffer = io.StringIO()
            if format_type == "md":
                markdown_str: str = df.to_markdown(index=False)
                return markdown_str
            elif format_type == "csv":
                df.to_csv(buffer, index=False)
                csv_str: str = buffer.getvalue().strip()
                return csv_str
            else:
                raise ValueError(f"Unsupported format: {format_type}")

        if type == "all":
            for name, df in audit_results.items():
                title = name.title().replace("_", " ")
                logger.info(f"# {title}\n{dataframe_to_string(df, format)}\n\n")
        if type == "general":
            logger.info(
                f"{dataframe_to_string(audit_results['general_statistics'], format)}"
            )
        if type == "by_tag":
            logger.info(
                f"{dataframe_to_string(audit_results['statistics_by_tag'], format)}"
            )
        if type == "detailed":
            logger.info(
                f"{dataframe_to_string(audit_results['detailed_results'], format)}"
            )

        logger.remove()
        logger.add(sys.stdout, level=original_logger_config._levelno)
        sys.exit(0)

    def _deduplicate_results(self) -> list[LintResult]:
        """Remove duplicated results from the lint results.

        A duplicated result is result for the same opinion
        for a .yaml file that is also evaluated in a .sql file
        Keep only the .yaml file result since it's where the changes need to be made.
        """
        deduplicated_results = []
        for result in self._lint_results:
            if isinstance(result.file, file_handlers.SqlFileHandler):
                node_yaml_file = result.file.dbt_node.docs_yml_file_path
                opinion = result.opinion_code
                if any(
                    result.file.type == ".yaml"
                    and str(result.file.path) == str(node_yaml_file)
                    and result.opinion_code == opinion
                    for result in self._lint_results
                ):
                    continue
            deduplicated_results.append(result)

        return deduplicated_results

    def _audit(self) -> dict[str, pd.DataFrame]:
        """Create a series of dataframes with data about the linting results."""
        audit_dict = defaultdict(list)

        for result in self.get_lint_results(deduplicate=True):
            audit_dict["dbt_project_name"].append(result.file.parent_dbt_project.name)
            audit_dict["file_name"].append(str(result.file.path))
            audit_dict["opinion_code"].append(result.opinion_code)
            audit_dict["severity"].append(result.severity.value)
            audit_dict["tags"].append(result.tags)
            audit_dict["passed"].append(result.passed)

        audit_df = pd.DataFrame(audit_dict)

        general_statistics = audit_df.groupby(
            ["dbt_project_name", "severity"], as_index=False
        ).agg(
            total_evaluated=("opinion_code", "count"),
            passed=("passed", lambda x: x.sum()),
            failed=("passed", lambda x: x.count() - x.sum()),
        )
        general_statistics["percentage_passed"] = (
            general_statistics["passed"] / general_statistics["total_evaluated"]
        ) * 100

        statistics_by_tag = (
            audit_df.explode("tags")
            .groupby(["dbt_project_name", "severity", "tags"], as_index=False)
            .agg(
                total_evaluated=("opinion_code", "count"),
                passed=("passed", lambda x: x.sum()),
                failed=("passed", lambda x: x.count() - x.sum()),
            )
        )
        statistics_by_tag["percentage_passed"] = (
            statistics_by_tag["passed"] / statistics_by_tag["total_evaluated"]
        ) * 100

        return OrderedDict(
            [
                ("general_statistics", general_statistics),
                ("statistics_by_tag", statistics_by_tag),
                ("detailed_results", audit_df),
            ]
        )
