# Development Guide

## Setup
Run `make dev` to initialize the multi_repo dbt projects.
These projects can be used to test stuff with the `dbt-opiner` cli.
For example, you can run: `dbt-opiner --log-level DEBUG lint -a` to lint all the projects and files and see the full debug output.

## Testing
We use pytest for unit testing. To run the tests, run `pytest`.
Functional tests TBD.

## Some Style Rules
Docstrings follow the [Google Python Style Guide](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings).
