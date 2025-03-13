from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class O007(base_opinion.BaseOpinion):
    """YAML files must not have columns that are not present in the SQL model.

    This opinion ensures that any columns defined in the YAML files actually exist
    within the SQL code of the models. Unnecessary columns in YAML files can cause
    confusion and maintenance issues.
    """

    def __init__(self, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="O007",
            description="YAML files must not have unnecessary columns.",
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
            yaml_columns = set(node.columns.keys())
            actual_columns = set(node.ast_extracted_columns)

            unnecessary_columns = yaml_columns - actual_columns

            if unnecessary_columns:
                results.append(
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"Unnecessary column(s) defined in YAML for model {node.alias}: {unnecessary_columns}.",
                    )
                )
            else:
                results.append(
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"No unnecessary columns found in YAML for model {node.alias}.",
                    )
                )

        return results
