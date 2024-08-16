from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O003(BaseOpinion):
    """All columns in the model should have a description. Empty descriptions are not allowed.

    Descriptions are important for documentation and understanding the purpose
    of the columns. A good description desambiguates the content of a column
    and helps making data more obvious.

    This opinion has some caveats. The only way of really knowning the
    columns of a model is by running the model and checking the columns in
    the database or catalog.json. However we don't want to depend on the execution of
    the model.

    This opinion checks if:
      - all defined columns in yaml files have a non empty description in the manifests.json.
      - the columns extracted by the sqlglot parser have a description in
        the manifest.json.

    If the model is constructed in a way that not all columns are extracted by
    the sqlglot parser, this opinion will omit those columns from the check.
    Rule O004 will check against this condition and will fail if
    unresolved `select *` are found.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O003",
            description="All columns must have a description.",
            severity=OpinionSeverity.MUST,
            tags=["metadata", "models"],
        )

    def _eval(self, file: SqlFileHandler | YamlFileHandler) -> list[LintResult]:
        nodes = []
        if file.type == ".sql" and file.dbt_node.type == "model":
            nodes = [file.dbt_node]
        if file.type == ".yaml":
            nodes = [node for node in file.dbt_nodes if node.type == "model"]

        results = []

        for node in nodes:
            # Check if model has columns.
            if not node.columns:
                results.append(
                    LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"Model {node.alias} {self.severity.value} have column descriptions.",
                    )
                )
                continue

            # If it has columns, description shouldn't be empty
            descriptionless_columns = []
            for key, value in node.columns.items():
                if not value.get("description") or len(value.get("description")) == 0:
                    descriptionless_columns.append(key)

            # Also, check ast_extracted_columns that doesn't exist in the columns keys
            for column in node.ast_extracted_columns:
                # Don't include unresolved select *
                if column not in node.columns.keys() and "*" not in column:
                    descriptionless_columns.append(column)

            if len(descriptionless_columns) > 0:
                results.append(
                    LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"Column(s): {descriptionless_columns} in model {node.alias} {self.severity.value} have a description.",
                    )
                )
            else:
                results.append(
                    LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"All columns in model {node.alias} have a description.",
                    )
                )

        return results
