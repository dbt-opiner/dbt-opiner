from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.opinions import base_opinion


class P002(base_opinion.BaseOpinion):
    """Dbt project must not send anonymous statistics.

    Sending anonymous statistics is enabled by default (opt-out).
    Although is a good way to help the dbt team improve the product, for privacy
    reasons we recommend to disable this feature.

    This opinion checks if the `send_anonymous_usage_stats` flag is set to false
    in the `dbt_project.yml` file.
    Previous to dbt 1.8 this flag was set in profiles.yml.
    This opinion will also check if it's present there.
    """

    def __init__(self, config: dict[str, Any] = {}, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="P002",
            description="Dbt project must not send anonymous statistics.",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["privacy", "dbt_config"],
        )

    def _eval(self, file: file_handlers.FileHandler) -> Optional[linter.LintResult]:
        if file.path.name not in ("profiles.yml", "dbt_project.yml"):
            return None

        if (
            file.parent_dbt_project.dbt_profile.get("config", {}).get(
                "send_anonymous_usage_stats"
            )
            is False
        ):
            return linter.LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="Anonymous statistics are disabled in profiles.yml file.",
            )

        if (
            file.parent_dbt_project.dbt_project_config.get("flags", {}).get(
                "send_anonymous_usage_stats"
            )
            is False
        ):
            return linter.LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message="Anonymous statistics are disabled in dbt_project.yml file.",
            )

        return linter.LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Dbt project {self.severity.value} not send anonymous statistics.",
        )
