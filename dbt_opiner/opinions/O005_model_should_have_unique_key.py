from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class O005(base_opinion.BaseOpinion):
    """Models should have a unique key defined in the config block of the model.
    This is useful to enforce the uniqueness of the model and
    to make the granularity of the model explicit.
    """

    def __init__(self, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="O005",
            description="Model should have unique key.",
            severity=linter.OpinionSeverity.SHOULD,
            tags=["dbt config", "models"],
        )

    def _eval(self, file: file_handlers.FileHandler) -> Optional[linter.LintResult]:
        if isinstance(file, file_handlers.SqlFileHandler):
            if isinstance(file.dbt_node, DbtModel):
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
        return None
