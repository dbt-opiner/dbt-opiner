from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.file_handlers import YamlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O001(BaseOpinion):
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
            if file.dbt_node.description:
                if len(file.dbt_node.description) > 0:
                    return LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message="Model has a description.",
                    )
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=f"Model {self.severity.value} have a description.",
            )

        if file.type == ".yaml":
            results = []
            for node in file.dbt_nodes:
                if node.type == "model":
                    if node.get("description"):
                        if len(node.get("description")) > 0:
                            results.append(
                                LintResult(
                                    file=file,
                                    opinion_code=self.code,
                                    passed=True,
                                    severity=self.severity,
                                    message=f"Model {node.alias} has a description.",
                                )
                            )
                            continue
                    results.append(
                        LintResult(
                            file=file,
                            opinion_code=self.code,
                            passed=False,
                            severity=self.severity,
                            message=f"Model {node.alias} {self.severity.value} have a description.",
                        )
                    )
            return results

        return None
