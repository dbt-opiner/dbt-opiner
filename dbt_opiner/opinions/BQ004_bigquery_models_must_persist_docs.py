from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.opinions import base_opinion


class BQ004(base_opinion.BaseOpinion):
    """The persist_docs option for models must be enabled.

    The persist_docs option for models must be enabled to ensure that the documentation
    is shown in the BigQuery console.

    Add

    ```yaml
    models:
      +persist_docs:
        relation: true
        columns: true
    ```
    To your dbt_project.yml file to enable this option.
    """

    def __init__(self, config: dict[str, Any], **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="BQ004",
            description="The persist_docs option for models must be enabled",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["bigquery", "dbt_config"],
        )

    def _eval(self, file: file_handlers.FileHandler) -> Optional[linter.LintResult]:
        if isinstance(file, file_handlers.YamlFileHandler):
            if (
                self._config.get("sqlglot_dialect") != "bigquery"
                or file.path.name != "dbt_project.yml"
            ):
                return None

            if file.get("models", {}).get("persist_docs", {}).get(
                "relation"
            ) and file.get("models", {}).get("persist_docs", {}).get("columns"):
                return linter.LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="The persist_docs option for models is enabled",
                )

            return linter.LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=f"The persist_docs option for models {self.severity.value} be enabled",
            )
        return None
