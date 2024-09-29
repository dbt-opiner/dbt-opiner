from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class BQ002(BaseOpinion):
    """Models materialized as tables in BigQuery should have clustering defined.

    Clustering is a feature in BigQuery that allows you to group your data based
    on the contents of one or more columns in the table.
    This can help improve query performance, reduce costs, and optimize your data
    for analysis.
    A table with clustering also optimizes "limit 1" queries, as it can skip scanning.

    This opinion checks if models materialized as tables in BigQuery have clustering defined.
    """

    def __init__(self, config: dict = None, **kwargs):
        super().__init__(
            code="BQ002",
            description="Models materialized as tables should have clustering defined.",
            severity=OpinionSeverity.SHOULD,
            config=config,
            tags=["models", "bigquery"],
        )

    def _eval(self, file: SqlFileHandler) -> LintResult | None:
        if (
            file.type != ".sql"
            or file.dbt_node.type != "model"
            or file.dbt_node.config.get("materialized") not in ("table", "incremental")
            or self._config.get("sqlglot_dialect") != "bigquery"
        ):
            return None

        if file.dbt_node.config.get("cluster_by"):
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="Model has clustering defined.",
            )

        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Model {self.severity.value} have clustering defined.",
        )
