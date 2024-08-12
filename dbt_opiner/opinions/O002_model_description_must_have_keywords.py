from loguru import logger

from dbt_opiner.dbt import DbtNode
from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O002(BaseOpinion):
    """Models descriptions must have keywords.

    Keywords help standarizing the description of the models,
    and ensure that all the important information is present in the description.
    Make sure the description of the model has all the required keywords.

    The keywords can be set in the dbt-opiner.yaml configuration like this:
    ```yaml
    sql:
      opinions_config:
        O002_keywords:
          - summary
          - granularity
    ```

    Keywords are case insensitive.
    """

    def __init__(self, config: dict = None, **kwargs) -> None:
        super().__init__(
            code="O002",
            description="Model description must have keywords.",
            severity=OpinionSeverity.MUST,
        )
        self._config = config

    def _eval(
        self, file: SqlFileHandler | YamlFileHandler
    ) -> LintResult | list[LintResult]:
        # Check type of file and model.
        try:
            keywords = (
                self._config.get("sql", {}).get("opinions_config").get("O002_keywords")
            )
        except AttributeError:
            keywords = None
        if keywords:
            logger.debug(f"Checking model descriptions for keywords: {keywords}")
            if (
                file.type == ".sql"
                and file.dbt_node.type == "model"
                and file.dbt_node.description
            ):
                return self._check_keywords(file.dbt_node, file, keywords)
            if file.type == ".yaml":
                results = []
                for node in file.dbt_nodes:
                    if node.type == "model" and node.description:
                        results.append(self._check_keywords(node, file, keywords))
                        continue
                    logger.debug(f"Model {node.alias} has no description.")
                if results:
                    return results
        logger.debug("Model has no description or keywords are not defined.")
        return None

    def _check_keywords(
        self, node: DbtNode, file: SqlFileHandler | YamlFileHandler, keywords: list[str]
    ) -> LintResult:
        missing_keywords = []
        for keyword in keywords:
            if keyword.lower() not in node.description.lower():
                missing_keywords.append(keyword)

        if len(missing_keywords) > 0:
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=f"Model {node.alias} description {self.severity.value} have keywords: {", ".join(missing_keywords)}",
            )
        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=True,
            severity=self.severity,
            message=f"Model {node.alias} description has all required keywords.",
        )
