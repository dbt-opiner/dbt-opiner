from typing import Any
from typing import Optional

from loguru import logger

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class O002(base_opinion.BaseOpinion):
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

    def __init__(self, config: dict[str, Any] = {}, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="O002",
            description="Model description must have keywords.",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["metadata", "models"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("O002", {})
        )

    def _eval(
        self, file: file_handlers.FileHandler
    ) -> Optional[list[linter.LintResult]]:
        # Check type of file and model.
        keywords = self._opinions_config.get("keywords", [])

        if keywords:
            logger.debug(f"Checking model descriptions for keywords: {keywords}")
            nodes = []
            if isinstance(file, file_handlers.SqlFileHandler):
                if isinstance(file.dbt_node, DbtModel):
                    nodes = [file.dbt_node]
            if isinstance(file, file_handlers.YamlFileHandler):
                nodes = [node for node in file.dbt_nodes if isinstance(node, DbtModel)]

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
                            message=f"Model {node.alias} description {self.severity.value} have keywords: {missing_keywords}",
                        )
                    else:
                        result = linter.LintResult(
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
