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


@patch("dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config")
def test_no_qa_opinion_in_file(
    get_config_mock, temp_empty_git_repo, mock_yamlfilehandler
):
    os.chdir(temp_empty_git_repo)
    get_config_mock.return_value = {}
    linter = Linter(OpinionsPack())
    linter.opinions = [
        O001(),
    ]
    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    yaml_file.no_qa_opinions = "O001"
    linter.lint_file(yaml_file)
    assert linter.get_lint_results() == []


@patch("dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config")
def test_no_qa_opinion_in_config(
    get_config_mock, temp_empty_git_repo, mock_yamlfilehandler
):
    os.chdir(temp_empty_git_repo)
    get_config_mock.return_value = {
        "opinions_config": {"ignore_files": {"O001": ".*test.*"}}
    }
    linter = Linter(OpinionsPack())
    linter.opinions = [O001()]
    yaml_file = mock_yamlfilehandler
    yaml_file.path = "test.yaml"
    linter.lint_file(yaml_file)
    assert linter.get_lint_results() == []


@patch("dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config")
def test_get_lint_results(
    get_config_mock, temp_empty_git_repo, mock_sqlfilehandler, mock_yamlfilehandler
):
    os.chdir(temp_empty_git_repo)
    get_config_mock.return_value = {}
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


@patch("dbt_opiner.opinions.opinions_pack.ConfigSingleton.get_config")
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
    get_config_mock,
    temp_empty_git_repo,
    caplog,
    mock_yamlfilehandler,
    result_type,
    expected,
):
    os.chdir(temp_empty_git_repo)
    get_config_mock.return_value = {}
    linter = Linter(OpinionsPack())
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
    lint_result_3 = LintResult(
        yaml_file, "C003", False, OpinionSeverity.MUST, "message"
    )

    linter._lint_results = [lint_result_1, lint_result_2, lint_result_3]

    # Patch logger.remove to prevent weird loguru error
    with patch("sys.exit") as mock_exit, patch.object(
        logger, "remove", lambda *args, **kwargs: None
    ):
        linter.log_audit_and_exit(type=result_type, format="md")

        with caplog.at_level(logging.INFO):
            for expectation in expected:
                assert expectation in caplog.text

        # Check that sys.exit was called with 0
        mock_exit.assert_called_once_with(0)
