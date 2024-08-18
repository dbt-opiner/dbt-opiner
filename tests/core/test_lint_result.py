from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity


def test_lint_result(mock_yamlfilehandler):
    file = mock_yamlfilehandler
    opinion_code = "O001"
    passed = True
    severity = OpinionSeverity.MUST
    message = "Model model_name has a description."

    result = LintResult(
        file=file,
        opinion_code=opinion_code,
        passed=passed,
        severity=severity,
        message=message,
    )

    assert result.file == file
    assert result.opinion_code == opinion_code
    assert result.passed == passed
    assert result.severity == severity
    assert result.message == message
