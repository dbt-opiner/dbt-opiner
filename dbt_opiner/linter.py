import sys
from enum import Enum
import re
from dataclasses import dataclass
from collections import defaultdict
from loguru import logger
from dbt_opiner.file_handlers import FileHandler
# from dbt_opiner.opinions.opinions_pack import OpinionsPack


class OpinionSeverity(Enum):
    MUST = (1, "must")
    SHOULD = (2, "should")

    def __init__(self, num, text):
        self.num = num
        self.text = text

    @property
    def value(self):
        return self.text


@dataclass
class LintResult:
    file: FileHandler
    opinion_code: str
    passed: bool
    severity: OpinionSeverity
    message: str

    def __gt__(self, other):
        if self.severity.num != other.severity.num:
            return self.severity.num > other.severity.num
        else:
            return self.opinion_code > other.opinion_code


class Linter:
    def __init__(self, opinions_pack):
        self.lint_results = defaultdict(list)
        self.opinions_pack = opinions_pack  # TODO make it the class iterable

    def lint_file(self, file: FileHandler):
        logger.debug(f"Linting file {file.file_path}")
        for opinion in self.opinions_pack.get_opinions(
            file.file_type, file.dbt_node.type
        ):
            if self._check_noqa(file, opinion.code):
                continue
            logger.debug(f"Checking opinion {opinion.code}")
            lint_result = opinion.check_opinion(file)
            if lint_result:
                logger.debug(f"Lint Result: {lint_result}")
                self.lint_results[file.file_path].append(lint_result)

    def log_failed_results_and_exit(self):
        exit_code = 0
        for file_path, results in self.lint_results.items():
            for result in sorted(results):
                if not result.passed:
                    exit_code = 1
                    logger.error(f"{file_path}: {result.message}")

        logger.debug(f"Exit with code: {exit_code}")
        sys.exit(exit_code)

    @staticmethod
    def _check_noqa(file: FileHandler, opinion_code):
        if re.search(rf"noqa: dbt-opiner [0-9]*,? ?{opinion_code}", file.content):
            return True
        if re.search(r"noqa: dbt-opiner all", file.content):
            return True
        return False
