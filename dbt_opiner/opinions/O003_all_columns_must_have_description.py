from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O003(BaseOpinion):
    """All columns in the model should have a description.

    Descriptions are important for documentation and understanding the purpose
    of the columns. A good description desambiguates the content of a column
    and helps making data more obvious.

    This opinion has some caveats. The only way of really knowning the
    columns of a model is by running the model and checking the columns in
    the catalog.json. However we don't want to depend on the execution of
    the model.

    This opinion checks if:
      - all defined columns have a description in the manifests.json.
      - the columns extracted by the sqlglot parser have a description in
        the manifest.json.

    If the model is constructed in a way that not all columns are extracted by
    the sqlglot parser, this opinion will omit those columns from the check.
    Rule O004 will check against this condition and will fail if
    unresolved select are found.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O003",
            description="All columns must have a description.",
            severity=OpinionSeverity.MUST,
        )

    def _eval(self, file: SqlFileHandler) -> LintResult:
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
