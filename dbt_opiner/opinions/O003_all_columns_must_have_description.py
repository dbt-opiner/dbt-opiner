from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class O003(base_opinion.BaseOpinion):
    """All columns in the model should have a description. Empty descriptions are not allowed.

    Descriptions are important for documentation and understanding the purpose
    of the columns. A good description disambiguates the content of a column
    and helps make data more obvious.

    This opinion has some caveats. The only way of really knowing the
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

    def __init__(self, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="O003",
            description="All columns must have a description.",
            severity=linter.OpinionSeverity.MUST,
            tags=["metadata", "models"],
        )

    def _eval(
        self, file: file_handlers.FileHandler
    ) -> Optional[list[linter.LintResult]]:
        nodes = []
        if isinstance(file, file_handlers.SqlFileHandler):
            if isinstance(file.dbt_node, DbtModel):
                nodes = [file.dbt_node]
        if isinstance(file, file_handlers.YamlFileHandler):
            nodes = [node for node in file.dbt_nodes if isinstance(node, DbtModel)]

        results = []

        for node in nodes:
            # Check if model has columns.
            if not node.columns:
                results.append(
                    linter.LintResult(
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
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"Column(s): {descriptionless_columns} in model {node.alias} {self.severity.value} have a description.",
                    )
                )
            else:
                results.append(
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"All columns in model {node.alias} have a description.",
                    )
                )

        return results
