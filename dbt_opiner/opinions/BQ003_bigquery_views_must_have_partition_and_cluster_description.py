from typing import Any

from loguru import logger

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.opinions import base_opinion


class BQ003(base_opinion.BaseOpinion):
    """Views must have documented the partition and cluster of underlying tables.

    Views that select underlying tables must have a description that explains
    the partition and clustering that will impact view usage performance.
    Since BigQuery does not show the partition and clustering information for views,
    it is important to document this information in the view description in dbt.

    This opinion checks if the description of the view has the keywords 'partition' and 'cluster'.
    The check is case insensitive.
    """

    def __init__(self, config: dict[str, Any], **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="BQ003",
            description="Views must have documented the partition and cluster of underlying tables.",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["bigquery", "metadata", "models"],
        )

    def _eval(
        self, file: file_handlers.FileHandler | file_handlers.YamlFileHandler
    ) -> list[linter.LintResult]:
        if self._config.get("sqlglot_dialect") != "bigquery":
            return []

        keywords = ["partition", "cluster"]
        nodes = []
        if isinstance(file, file_handlers.SqlFileHandler):
            if (
                file.dbt_node.type == "model"
                and file.dbt_node.config.get("materialized") == "view"
            ):
                nodes = [file.dbt_node]

        if isinstance(file, file_handlers.YamlFileHandler):
            nodes = [
                node
                for node in file.dbt_nodes
                if node.type == "model" and node.config.get("materialized") == "view"
            ]

        results = []
        for node in nodes:
            if node.description:
                missing_keywords = []
                for keyword in keywords:
                    if keyword.lower() not in node.description.lower():
                        missing_keywords.append(keyword)

                if len(missing_keywords) > 0:
                    result = linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"View {node.alias} description {self.severity.value} have keywords: {missing_keywords}",
                    )
                else:
                    result = linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"View {node.alias} description has all required keywords.",
                    )
                results.append(result)
            else:
                logger.debug(f"Model {node.alias} has no description.")
        return results
