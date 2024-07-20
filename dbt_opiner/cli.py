import click


@click.command(
    help="""
         """,
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main():
    click.echo("Hello!")
