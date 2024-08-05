from dbt_opiner.opinions.base_opinion import BaseOpinion
from dbt_opiner.linter import OpinionSeverity


class O004(BaseOpinion):
    def __init__(self):
        super().__init__(
            code="O004",
            description="Model should have unique key.",
            severity=OpinionSeverity.SHOULD,
            applies_to_file_type=".sql",
            applies_to_node_type="model",
        )

    def _eval(self, file):
        pass
