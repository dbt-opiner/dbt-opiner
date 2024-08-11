from dbt_opiner.file_handlers import SQLFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O003(BaseOpinion):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O003",
            description="All columns must have a description.",
            severity=OpinionSeverity.MUST,
        )

    def _eval(self, file: SQLFileHandler) -> LintResult:
        # Check type of file and model.
        if file.type not in [".sql"]:
            return None  # TODO: add yaml check support
        if file.dbt_node.type != "model":
            return None

        # TODO: add yaml check support
        # If you change the yaml and remove the description, this should fail.

        descriptionless_columns = []
        for key, value in file.dbt_node.columns.items():
            if not value.get("description") or len(value.get("description")) == 0:
                descriptionless_columns.append(key)

        if len(descriptionless_columns) > 0:
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=f"Columns: {", ".join(descriptionless_columns)} {self.severity.value} have a description.",
            )

        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=True,
            severity=self.severity,
            message="All columns have a description.",
        )
