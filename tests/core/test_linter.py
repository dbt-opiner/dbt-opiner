from dbt_opiner.dbt import DbtNode
from dbt_opiner.linter import Linter
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.opinions_pack import OpinionsPack


def test_get_lint_results(mock_sqlfilehandler, mock_yamlfilehandler):
    linter = Linter(OpinionsPack())
    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    sql_file = mock_sqlfilehandler
    sql_file.dbt_node = DbtNode(
        {
            "resource_type": "model",
            "description": "Some description",
            "patch_path": "test.yaml",
        }
    )

    lint_result_1 = LintResult(
        yaml_file, "C001", False, OpinionSeverity.SHOULD, "message"
    )
    lint_result_2 = LintResult(
        sql_file, "C001", False, OpinionSeverity.SHOULD, "message"
    )

    linter._lint_results = [lint_result_1, lint_result_2]
    # Test get_lint_results without deduplication
    assert linter.get_lint_results(False) == [lint_result_1, lint_result_2]
    # Test get_lint_results with deduplication
    assert linter.get_lint_results(True) == [lint_result_1]
