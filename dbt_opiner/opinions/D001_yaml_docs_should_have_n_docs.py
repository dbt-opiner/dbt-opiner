from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class D001(BaseOpinion):
    """Yaml files used for documentation should have a limited number of models or sources.

    Although dbt allows to put multiple nodes inside the same yaml file,
    having a limited ammount of nodes per yaml file makes it easier to find the documentation
    for a specific element and keeps the files short.

    This opinion checks if each yaml file containing models or sources contains more than a specified number (default 1).
    You can specify these under the `opinions_config>extra_opinions_config>D001` key in your `.dbt-opiner.yaml` file.
        - max_n_allowed: number of docs allowed per yaml file
    """

    def __init__(self, config: dict = None, **kwargs) -> None:
        super().__init__(
            code="D001",
            description="Yaml files used for documentation should have a limited number of models or sources.",
            severity=OpinionSeverity.SHOULD,
            config=config,
            tags=["metadata"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("D001", {})
        )

    def _eval(self, file: YamlFileHandler) -> LintResult | None:
        max_n_allowed = self._opinions_config.get("max_n_allowed", 1)
        if file.type == ".yaml":
            if len(file.dbt_nodes) > max_n_allowed:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=f"Yaml file {file.path} {self.severity.value} have more than {max_n_allowed} nodes.",
                )
            else:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message=f"Yaml file {file.path} has {len(file.dbt_nodes)} nodes.",
                )
