from loguru import logger

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
            config=config,
            tags=["metadata", "models"],
        )

    def _eval(self, file: SqlFileHandler | YamlFileHandler) -> list[LintResult]:
        # Check type of file and model.
        keywords = (
            self._config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("O002_keywords")
        )

        if keywords:
            logger.debug(f"Checking model descriptions for keywords: {keywords}")
            nodes = []
            if file.type == ".sql" and file.dbt_node.type == "model":
                nodes = [file.dbt_node]

            if file.type == ".yaml":
                nodes = [node for node in file.dbt_nodes if node.type == "model"]

            results = []
            for node in nodes:
                if node.description:
                    missing_keywords = []
                    for keyword in keywords:
                        if keyword.lower() not in node.description.lower():
                            missing_keywords.append(keyword)

                    if len(missing_keywords) > 0:
                        result = LintResult(
                            file=file,
                            opinion_code=self.code,
                            passed=False,
                            severity=self.severity,
                            message=f"Model {node.alias} description {self.severity.value} have keywords: {missing_keywords}",
                        )
                    else:
                        result = LintResult(
                            file=file,
                            opinion_code=self.code,
                            passed=True,
                            severity=self.severity,
                            message=f"Model {node.alias} description has all required keywords.",
                        )
                    results.append(result)
                else:
                    logger.debug(f"Model {node.alias} has no description.")
            return results

        logger.debug("Keywords are not defined.")
        return None
