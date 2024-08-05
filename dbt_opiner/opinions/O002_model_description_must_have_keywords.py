from dbt_opiner.opinions.base_opinion import BaseOpinion
from dbt_opiner.linter import OpinionSeverity


class O002(BaseOpinion):
    def __init__(self):
        super().__init__(
            code="O002",
            description="Model description must have keywords.",
            severity=OpinionSeverity.MUST,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
        )

    def _eval(self, file):
        pass
