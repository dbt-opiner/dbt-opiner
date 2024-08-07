from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O004(BaseOpinion):
    def __init__(self):
        super().__init__(
            code="O004",
            description="Model should have unique key.",
            severity=OpinionSeverity.SHOULD,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        pass
