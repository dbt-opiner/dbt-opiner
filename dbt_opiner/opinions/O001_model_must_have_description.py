from typing import Any
from typing import Optional

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class O001(base_opinion.BaseOpinion):
    """Models must have descriptions. Empty descriptions are not allowed.

    Descriptions are important for documentation and understanding the purpose of the model.
    A good description makes data more obvious.

    Include a description for the model in a yaml file or config block.
    """

    def __init__(self, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="O001",
            description="Model must have a description.",
            severity=linter.OpinionSeverity.MUST,
            tags=["metadata", "models"],
        )

    def _eval(
        self, file: file_handlers.FileHandler
    ) -> Optional[list[linter.LintResult]]:
        nodes = []
        if isinstance(file, file_handlers.SqlFileHandler):
            if isinstance(file.dbt_node, DbtModel):
                nodes = [file.dbt_node]
        if isinstance(file, file_handlers.YamlFileHandler):
            nodes = [node for node in file.dbt_nodes if isinstance(node, DbtModel)]

        results = []
        for node in nodes:
            if node.description:
                if len(node.description) > 0:
                    result = linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"Model {node.alias} has a description.",
                    )
            else:
                result = linter.LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=f"Model {node.alias} {self.severity.value} have a description.",
                )
            results.append(result)

        return results
