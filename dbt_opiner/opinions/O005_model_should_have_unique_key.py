from dbt_opiner import file_handlers
from dbt_opiner import linter
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
            severity=linter.OpinionSeverity.SHOULD,
            tags=["dbt config", "models"],
        )

    def _eval(self, file: file_handlers.SqlFileHandler) -> linter.LintResult | None:
        # Check type of file and model.
        if file.type == ".sql" and file.dbt_node.type == "model":
            if file.dbt_node.unique_key:
                return linter.LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="Model has a unique key.",
                )
            return linter.LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=f"Model {self.severity.value} have a unique key defined in the config block.",
            )
