from dbt_opiner import linter
from dbt_opiner.opinions.base_opinion import BaseOpinion


class BadOpinion(BaseOpinion):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="bad",
            description="",
            severity=linter.OpinionSeverity.MUST,
            tags=[],
        )

    def _eval(self, file):
        return ["bad result"]


def test_opinion_doesnt_return_lintresult(mock_sqlfilehandler):
    opinion = BadOpinion()
    results = opinion.check_opinion(mock_sqlfilehandler)
    assert results is None
