from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O003(BaseOpinion):
    def __init__(self, config: dict = None) -> None:
        super().__init__(
            code="O003",
            description="All columns must have a description.",
            severity=OpinionSeverity.MUST,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
            config=config,
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        pass
