from dbt_opiner.dbt import DbtNode
from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O001(BaseOpinion):
    """Models must have descriptions. Empty descriptions are not allowed.

    Descriptions are important for documentation and understanding the purpose of the model.
    A good description makes data more obvious.

    Include a description for the model in a yaml file or config block.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O001",
            description="Model must have a description.",
            severity=OpinionSeverity.MUST,
        )

    def _eval(
        self, file: SqlFileHandler | YamlFileHandler
    ) -> LintResult | list[LintResult]:
        if file.type == ".sql" and file.dbt_node.type == "model":
            return self._check_description(file.dbt_node, file)

        if file.type == ".yaml":
            results = []
            for node in file.dbt_nodes:
                if node.type == "model":
                    results.append(self._check_description(node, file))
            return results
        return None

    def _check_description(
        self, node: DbtNode, file: SqlFileHandler | YamlFileHandler
    ) -> LintResult:
        if node.description:
            if len(node.description) > 0:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message=f"Model {node.alias} has a description.",
                )
        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Model {node.alias} {self.severity.value} have a description.",
        )
