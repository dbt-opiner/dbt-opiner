from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class BQ004(BaseOpinion):
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

    def __init__(self, config: dict, **kwargs) -> None:
        super().__init__(
            code="BQ004",
            description="The persist_docs option for models must be enabled",
            severity=OpinionSeverity.MUST,
            config=config,
            tags=["bigquery", "dbt_config"],
        )

    def _eval(self, file: YamlFileHandler) -> LintResult | None:
        if (
            self._config.get("sqlglot_dialect") != "bigquery"
            or file.path.name != "dbt_project.yml"
        ):
            return None

        if file.get("models", {}).get("persist_docs", {}).get("relation") and file.get(
            "models", {}
        ).get("persist_docs", {}).get("columns"):
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="The persist_docs option for models is enabled",
            )

        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"The persist_docs option for models {self.severity.value} be enabled",
        )
