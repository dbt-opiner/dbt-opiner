from loguru import logger

from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O002(BaseOpinion):
    def __init__(self, config: dict = None, **kwargs) -> None:
        super().__init__(
            code="O002",
            description="Model description must have keywords.",
            severity=OpinionSeverity.MUST,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
        )
        self._config = config

    def _eval(self, file: SQLFileHandler) -> LintResult:
        try:
            keywords = (
                self._config.get("sql", {}).get("opinions_config").get("O002_keywords")
            )
        except AttributeError:
            keywords = None

        if file.dbt_node.description and keywords:
            logger.debug(f"Checking model description for keywords: {keywords}")
            missing_keywords = []

            for keyword in keywords:
                if keyword.lower() not in file.dbt_node.description.lower():
                    missing_keywords.append(keyword)

            if len(missing_keywords) > 0:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=f"Model description {self.severity.value} have keywords: {", ".join(missing_keywords)}",
                )
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="Model description has all required keywords.",
            )

        logger.debug("Model has no description or keywords are not defined.")
