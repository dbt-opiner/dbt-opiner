from dbt_opiner import linter


def test_lint_result(mock_yamlfilehandler):
    file = mock_yamlfilehandler
    opinion_code = "O001"
    passed = True
    severity = linter.OpinionSeverity.MUST
    message = "Model model_name has a description."

    result = linter.LintResult(
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


def test_lint_result_sorting(mock_yamlfilehandler, mock_sqlfilehandler):
    result1 = linter.LintResult(
        file=mock_yamlfilehandler,
        opinion_code="C001",
        passed=True,
        severity=linter.OpinionSeverity.MUST,
        message="message",
    )
    result2 = linter.LintResult(
        file=mock_sqlfilehandler,
        opinion_code="C001",
        passed=True,
        severity=linter.OpinionSeverity.MUST,
        message="message",
    )

    assert not (
        result1 > result2
    )  # They are not the same, but they are not greater than each other

    result3 = linter.LintResult(
        file=mock_sqlfilehandler,
        opinion_code="C002",
        passed=True,
        severity=linter.OpinionSeverity.MUST,
        message="message",
    )

    assert result3 > result2  # result3 has a greater opinion code than result2

    result4 = linter.LintResult(
        file=mock_sqlfilehandler,
        opinion_code="C004",
        passed=True,
        severity=linter.OpinionSeverity.SHOULD,
        message="message",
    )

    assert (
        result3 > result4
    )  # result3 has a greater severity than result4 (ignore opinion code)
