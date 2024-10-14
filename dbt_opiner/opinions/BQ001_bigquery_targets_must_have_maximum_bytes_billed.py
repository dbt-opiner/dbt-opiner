from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.opinions import base_opinion


class BQ001(base_opinion.BaseOpinion):
    """Bigquery targets used for development and testing must have maximum_bytes_billed
    set to prevent unexpected costs.

    This opinion checks if the `maximum_bytes_billed` parameter is set in the target.
    An optional list of target names to ignore can be specified in the configuration. By default,
    it ignores the `prod` and `production` targets. To disable this and check all targets,
    set the `ignore_targets` configuration to an empty list.
    Also, an optional `maximum_bytes_billed` parameter can be set to specify the maximum
    number of bytes billed allowed. By default it is not checked.

    Extra configuration:
    You can specify these under the `opinions_config>extra_opinions_config>BQ001` key in your `.dbt-opiner.yaml` file.
      - ignore_targets: list of target names to ignore (default: ['prod', 'production'])
      - maximum_bytes_billed: maximum bytes billed allowed (optional)
    """

    def __init__(self, config: dict[str, Any] = {}, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="BQ001",
            description="Bigquery targets used for development and testing must have maximum_bytes_billed.",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["bigquery", "dbt_config"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("BQ001", {})
        )

    def _eval(self, file: file_handlers.FileHandler) -> Optional[linter.LintResult]:
        if isinstance(file, file_handlers.YamlFileHandler):
            if (
                file.path.name != "profiles.yml"
                or self._config.get("sqlglot_dialect") != "bigquery"
            ):
                return None

            ignored_targets = self._opinions_config.get(
                "ignore_targets", ["prod", "production"]
            )
            max_bytes_billed = self._opinions_config.get("maximum_bytes_billed")
            content = file.to_dict()
            error_targets: dict[str, list[str]] = {
                "missing": [],
                "exceeded": [],
                "bq_targets": [],
            }
            for project in content.values():
                for target_name, target in project.get("outputs", {}).items():
                    if target.get("type") == "bigquery":
                        error_targets["bq_targets"].append(target_name)
                        if target_name not in ignored_targets:
                            if not target.get("maximum_bytes_billed"):
                                error_targets["missing"].append(target_name)
                            if (
                                max_bytes_billed
                                and target.get("maximum_bytes_billed")
                                > max_bytes_billed
                            ):
                                error_targets["exceeded"].append(target_name)

            if error_targets.get("missing") or error_targets.get("exceeded"):
                message = f"Bigquery targets used for development and testing {self.severity.value} have maximum_bytes_billed."
                if error_targets.get("exceeded"):
                    message += f"Exceeded for targets: {error_targets['exceeded']}.\n"
                if error_targets.get("missing"):
                    message += f"Missing for targets: {error_targets['missing']}.\n"

                return linter.LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=message,
                )

            # Avoid returning true if there are no bq targets
            if error_targets.get("bq_targets"):
                return linter.LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="Bigquery targets used for development and testing have maximum_bytes_billed.",
                )
        return None
