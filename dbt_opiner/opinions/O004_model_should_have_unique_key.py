from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O004(BaseOpinion):
    def __init__(self, config: dict = None) -> None:
        super().__init__(
            code="O004",
            description="Model should have unique key.",
            severity=OpinionSeverity.SHOULD,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
            config=config,
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        if file.dbt_node.unique_key:
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="Model has a unique key.",
            )
        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Model {self.severity.value} have a unique key.",
        )
