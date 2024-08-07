from loguru import logger

from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O002(BaseOpinion):
    def __init__(self, config: dict = None) -> None:
        super().__init__(
            code="O002",
            description="Model description must have keywords.",
            severity=OpinionSeverity.MUST,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
            config=config,
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        try:
            keywords = (
                self._config.get("sql").get("opinions_config").get("O002_keywords")
            )
        except AttributeError:
            keywords = None

        if file.dbt_node.description and keywords:
            logger.debug(f"Checking model description for keywords: {keywords}")
            passed = True
            for keyword in keywords:
                if keyword not in file.dbt_node.description:
                    passed = False
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=passed,
                severity=self.severity,
                message=f"Model description {self.severity.value} have keywords: {keywords}",
            )

        logger.debug("Model has no description or keywords are not defined.")
