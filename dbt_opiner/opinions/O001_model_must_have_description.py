from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O001(BaseOpinion):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O001",
            description="Model must have a description.",
            severity=OpinionSeverity.MUST,
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        # Check type of file and model.
        if file.type not in [".sql"]:
            return None  # TODO: add yaml check support
        if file.dbt_node.type != "model":
            return None

        if file.dbt_node.description:
            if len(file.dbt_node.description) > 0:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="Model has a description.",
                )
        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Model {self.severity.value} have a description.",
        )
