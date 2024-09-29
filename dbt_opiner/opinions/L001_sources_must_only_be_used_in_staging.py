from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class L001(BaseOpinion):
    """Sources must only be used in staging layer.

    Staging models are the entrypoint for raw data in dbt projects, and it is the
    only place were we can use the source macro.
    See more [here](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)

    This opinion checks if the source macro is only used in staging models.

    Extra configuration:
    Sometimes when dbt is run in CI all models end up in the same schema.
    By specifying a node alias prefix we can still enforce this rule.
    You can specify these under the `opinions_config>extra_opinions_config>L001` key in your `.dbt-opiner.yaml` file.
    - staging_schema: schema name for staging tables (default: staging)
    - staging_prefix: prefix for staging tables (default: stg_)
    """

    def __init__(self, config: dict, **kwargs) -> None:
        super().__init__(
            code="L001",
            description="Sources must only be used in staging.",
            severity=OpinionSeverity.MUST,
            tags=["lineage", "models"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("L001", {})
        )

    def _eval(self, file: SqlFileHandler) -> LintResult | None:
        staging_schema_name = self._opinions_config.get("staging_schema", "staging")
        staging_prefix = self._opinions_config.get("staging_prefix", "stg_")

        if (
            file.type == ".sql"
            and file.dbt_node.type == "model"
            and not file.dbt_node.alias.startswith(staging_prefix)
            and not file.dbt_node.schema == staging_schema_name
        ):
            # We check the raw sql content for the source macro.
            content = file.content.replace(" ", "").replace("\n", "")

            if "{{source(" in content:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=f"The source macro {self.severity.value} only be used in staging layer.",
                )

            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message=f"The source macro is not used in model {file.dbt_node.alias}.",
            )
