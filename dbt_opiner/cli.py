import inspect
import os
import sys
import typing as t

import click
from loguru import logger

from dbt_opiner import entrypoint


class ChoiceTuple(click.Choice):
    """
    Required for MultiOption to work
    """

    name = "CHOICE_TUPLE"

    def convert(self, value, param, ctx):
        if not isinstance(value, str):
            for value_item in value:
                super().convert(value_item, param, ctx)
        else:
            super().convert(value, param, ctx)

        return value


class MultiOption(click.Option):
    """
    Taken from https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/cli/options.py
    """

    def __init__(self, *args, **kwargs) -> None:
        self.save_other_options = kwargs.pop("save_other_options", True)
        nargs = kwargs.pop("nargs", -1)
        assert nargs == -1, "nargs, if set, must be -1 not {}".format(nargs)
        super(MultiOption, self).__init__(*args, **kwargs)
        # this makes mypy happy, setting these to None causes mypy failures
        self._previous_parser_process = lambda *args, **kwargs: None
        self._eat_all_parser = lambda *args, **kwargs: None

        # validate that multiple=True
        multiple = kwargs.pop("multiple", None)
        msg = f"MultiOption named `{self.name}` must have multiple=True (rather than {multiple})"
        assert multiple, msg

        # validate that type=tuple or type=ChoiceTuple
        option_type = kwargs.pop("type", None)
        msg = f"MultiOption named `{self.name}` must be tuple or ChoiceTuple (rather than {option_type})"
        if inspect.isclass(option_type):
            assert issubclass(option_type, tuple), msg
        else:
            assert isinstance(option_type, ChoiceTuple), msg

    def add_to_parser(self, parser: click.parser.OptionParser, ctx: click.Context):
        def parser_process(value: str, state: click.parser.ParsingState):
            # method to hook to the parser.process
            done = False
            value_list = str.split(value, " ")
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value_list.append(state.rargs.pop(0))
            else:
                # grab everything remaining
                value_list += state.rargs
                state.rargs[:] = []
            value_tuple = tuple(value_list)
            # call the actual process
            self._previous_parser_process(value_tuple, state)

        retval = super(MultiOption, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval

    def type_cast_value(self, ctx: click.Context, value: t.Any) -> t.Any:
        def flatten(data):
            if isinstance(data, tuple):
                for x in data:
                    yield from flatten(x)
            else:
                yield data

        # there will be nested tuples to flatten when multiple=True
        value = super(MultiOption, self).type_cast_value(ctx, value)
        if value:
            value = tuple(flatten(value))
        return value


@click.group(
    help="Tool to lint dbt projects and keep them on rails.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(
    message="dbt-opiner version: %(version)s",
)
def main():
    pass


@main.command(help="Lint files")
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
    help="Set log level",
)
@click.option("-a", "--all-files", is_flag=True, help="Process all files")
@click.option(
    "-f",
    "--files",
    cls=MultiOption,
    type=tuple,
    multiple=True,
    help="""Files to process.
            Can also be directory paths.
            Multiple values can be provided separated by space e.g.: -f file_1.sql file_2.sql""",
)
@click.option("--target", type=str, help="DBT Target to compile manifest")
@click.option(
    "--force-compile",
    is_flag=True,
    help="Compile dbt project manifest even if it exists",
)
@click.option(
    "--no-ignore",
    is_flag=True,
    help="Ignore all no-qa configurations",
)
@click.option(
    "-o",
    "--output-file",
    type=str,
    help="If specified, a file to capture the lint results",
)
@click.pass_context
def lint(
    ctx, log_level, files, all_files, target, force_compile, no_ignore, output_file
):
    if not files and not all_files:
        raise click.BadParameter(
            "Either --files or --all_files options must be provided"
        )

    # Try to set a target from an environment variable
    # This is useful when things should run in CI
    if target is None:
        target = os.getenv("DBT_TARGET")

    # Set log level
    logger.remove()
    logger.add(sys.stdout, level=log_level.upper())
    ctx.ensure_object(dict)
    ctx.obj["log_level"] = log_level

    # Run linter
    entrypoint.lint(files, all_files, target, force_compile, no_ignore, output_file)
