import os

import pytest
from click.testing import CliRunner

from dbt_opiner.cli import main


@pytest.fixture
def runner():
    return CliRunner()


def test_help_option(runner):
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--version" in result.output
    assert "lint" in result.output


def test_lint_option(runner):
    result = runner.invoke(main, ["lint", "--help"])
    assert result.exit_code == 0
    assert "-a, --all-files" in result.output
    assert "-f, --files" in result.output
    assert "--target" in result.output
    assert "--force-compile" in result.output
    assert "--no-ignore" in result.output
    assert "-o, --output-file" in result.output


def test_missing_options(runner):
    result = runner.invoke(main, ["lint"])
    assert result.exit_code == 2
    assert "Either --files or --all_files options must be provided" in result.output


def test_linter_run_all_files(runner, temp_complete_git_repo):
    os.chdir(temp_complete_git_repo / "dbt_project")
    result = runner.invoke(main, ["lint", "-a", "--log-level", "DEBUG"])
    assert result.exit_code == 0
    assert "Linting completed in" in result.output
    assert "Linting dbt projects" in result.output
    assert "Linting file" in result.output


def test_linter_run_changed_files(runner, temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    result = runner.invoke(
        main,
        [
            "lint",
            "-f",
            "dbt_project/models/test/model/model.sql",
            "--log-level",
            "DEBUG",
        ],
    )
    assert result.exit_code == 0
    assert "Linting file dbt_project/models/test/model/model.sql" in result.output


def test_audit_all(runner, temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    result = runner.invoke(
        main,
        [
            "audit",
            "--log-level",
            "DEBUG",
        ],
    )
    assert result.exit_code == 0
    assert "Auditing dbt projects" in result.output
    assert (
        "dbt_project_name|severity|total_evaluated|passed|failed|percentage_passed".replace(
            " ", ""
        )
        in result.output.replace(" ", "")
    )


def test_audit_project_error(runner, temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    result = runner.invoke(
        main,
        [
            "audit",
            "--dbt_project_dir",
            "dbt_opiner",
            "--log-level",
            "DEBUG",
        ],
    )
    assert result.exit_code == 1
    assert "is not a dbt project" in result.output


@pytest.mark.parametrize(
    "format, expected",
    [
        pytest.param(
            "md",
            "dbt_project_name|severity|total_evaluated|passed|failed|percentage_passed",
            id="md",
        ),
        pytest.param(
            "csv",
            "dbt_project_name,severity,total_evaluated,passed,failed,percentage_passed",
            id="csv",
        ),
    ],
)
def test_audit_project(runner, temp_complete_git_repo, format, expected):
    os.chdir(temp_complete_git_repo)
    result = runner.invoke(
        main,
        [
            "audit",
            "--format",
            format,
            "--dbt_project_dir",
            "dbt_project",
            "--log-level",
            "DEBUG",
        ],
    )
    assert result.exit_code == 0
    assert expected in result.output.replace(" ", "")
