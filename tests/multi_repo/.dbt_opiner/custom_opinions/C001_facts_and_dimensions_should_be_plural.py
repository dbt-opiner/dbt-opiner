# Write a custom rule for dbt_opiner
from dbt_opiner.opinions.base_opinion import BaseOpinion
from dbt_opiner.linter import LintResult, OpinionSeverity


class C001(BaseOpinion):
    def __init__(self):
        super().__init__(
            code="C001",
            description="Facts and dimensions should be plural.",
            severity=OpinionSeverity.SHOULD,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
        )

    def _eval(self, file):
        #        print(file.dbt_node)
        #        exit()
        if file.dbt_node.description:
            if len(file.dbt_node.description) > 0:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="Facts and dimensions are plural.",
                )
        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=False,
            severity=self.severity,
            message=f"Facts and dimensions {self.severity.value} be plural.",
        )
