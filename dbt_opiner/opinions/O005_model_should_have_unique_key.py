from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O005(BaseOpinion):
    """Models should have a unique key defined in the config block of the model.
    This is useful to enforce the uniqueness of the model and
    to make the granularity of the model explicit.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O005",
            description="Model should have unique key.",
            severity=OpinionSeverity.SHOULD,
            tags=["dbt config", "models"],
        )

    def _eval(self, file: SqlFileHandler) -> LintResult:
        # Check type of file and model.
        if file.type != ".sql" or file.dbt_node.type != "model":
            return None

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
            message=f"Model {self.severity.value} have a unique key defined in the config block.",
        )
