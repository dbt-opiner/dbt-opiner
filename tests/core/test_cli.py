from click.testing import CliRunner

from dbt_opiner.cli import main


def test_help_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--version" in result.output
    assert "lint" in result.output


def test_lint_option():
    runner = CliRunner()
    result = runner.invoke(main, ["lint", "--help"])
    assert result.exit_code == 0
    assert "-a, --all-files" in result.output
    assert "-f, --files" in result.output
    assert "--target" in result.output
    assert "--force-compile" in result.output
    assert "--no-ignore" in result.output
    assert "-o, --output-file" in result.output


def test_missing_options():
    runner = CliRunner()
    result = runner.invoke(main, ["lint"])
    assert result.exit_code == 2
    assert "Either --files or --all_files options must be provided" in result.output
