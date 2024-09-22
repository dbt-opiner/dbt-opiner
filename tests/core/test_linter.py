import logging
import os
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from loguru import logger

from dbt_opiner.dbt import DbtNode
from dbt_opiner.dbt import DbtProject
from dbt_opiner.linter import Linter
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions import O001
from dbt_opiner.opinions.opinions_pack import OpinionsPack


@pytest.fixture
@patch("dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config")
def base_linter(get_config_mock, temp_empty_git_repo):
    os.chdir(temp_empty_git_repo)
    get_config_mock.return_value = {}
    linter = Linter(OpinionsPack())
    return linter


@pytest.fixture
def linter_with_results(base_linter, mock_yamlfilehandler):
    mock_dbt_project = Mock(spec=DbtProject)
    mock_dbt_project.name = "test_project"

    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    yaml_file.parent_dbt_project = mock_dbt_project

    lint_result_1 = LintResult(
        yaml_file, "C001", False, OpinionSeverity.SHOULD, "message", ["tag1"]
    )
    lint_result_2 = LintResult(
        yaml_file, "C002", False, OpinionSeverity.MUST, "message", ["tag1", "tag2"]
    )
    lint_result_3 = LintResult(yaml_file, "C003", True, OpinionSeverity.MUST, "message")

    base_linter._lint_results = [lint_result_1, lint_result_2, lint_result_3]

    return base_linter


def test_noqa_opinion_in_file(base_linter, mock_yamlfilehandler):
    base_linter.opinions = [
        O001(),
    ]
    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    yaml_file.no_qa_opinions = "O001"
    base_linter.lint_file(yaml_file)
    assert base_linter.get_lint_results() == []


def test_noqa_opinion_in_config(base_linter, mock_yamlfilehandler):
    base_linter.opinions = [O001()]
    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    base_linter.lint_file(yaml_file)
    assert base_linter.get_lint_results() == []


def test_get_lint_results(base_linter, mock_sqlfilehandler, mock_yamlfilehandler):
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

    base_linter._lint_results = [lint_result_1, lint_result_2]
    # Test get_lint_results without deduplication
    assert base_linter.get_lint_results(False) == [lint_result_1, lint_result_2]
    # Test get_lint_results with deduplication
    assert base_linter.get_lint_results(True) == [lint_result_1]


@pytest.mark.parametrize(
    "result_type, expected",
    [
        pytest.param(
            "all",
            ["General Statistics", "Statistics By Tag", "Detailed Results"],
            id="all",
        ),
        pytest.param(
            "general",
            [
                "dbt_project_name",
                "severity",
                "total_evaluated",
                "passed",
                "failed",
                "percentage_passed",
            ],
            id="general",
        ),
        pytest.param("by_tag", ["tags"], id="by_tag"),
        pytest.param("detailed", ["file_name", "opinion_code", "tags"], id="detailed"),
    ],
)
def test_audit(
    linter_with_results,
    caplog,
    result_type,
    expected,
):
    # Patch logger.remove to prevent weird loguru error
    with patch("sys.exit") as mock_exit, patch.object(
        logger, "remove", lambda *args, **kwargs: None
    ):
        linter_with_results.log_audit_and_exit(
            type=result_type, format="md", output_file="audit.md"
        )

        with caplog.at_level(logging.INFO):
            for expectation in expected:
                assert expectation in caplog.text

        # Check output file was created
        assert os.path.exists("audit.md")
        # Check that sys.exit was called with 0
        mock_exit.assert_called_once_with(0)


def test_log_results_and_exit(linter_with_results, caplog):
    with patch("sys.exit") as mock_exit, patch.object(
        logger, "remove", lambda *args, **kwargs: None
    ):
        linter_with_results.log_results_and_exit(output_file="results.txt")
        expectations = [
            "C001 | message",
            "C002 | message",
            "C003 | message",
            "WARNING",
            "ERROR",
            "DEBUG",
        ]
        with caplog.at_level(logging.DEBUG):
            for expectation in expectations:
                assert expectation in caplog.text

        assert os.path.exists("results.txt")
        # Check that sys.exit was called with 1 because there are failed results
        mock_exit.assert_called_once_with(1)
