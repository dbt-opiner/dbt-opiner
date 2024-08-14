# Contributing to dbt-opiner

This is a humble project, and any help is welcome. If you want to contribute, please read the following guidelines.

## Report bugs and ask for features using Github's issues
We use GitHub issues to track public bugs and feature requests.
Report a bug or make a feature requrest by opening a new issue.

Great Bug Reports tend to have:
 - A quick summary and/or background
 - Steps to reproduce
    - Be specific!
    - Give sample code if you can
- What you expected would happen
- What actually happens
-  Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)
-  References (e.g. documentation pages related to the issue)

## Submitting code changes
Pull requests are the best way to propose changes to the codebase. We actively welcome your pull requests:

Please keep PR's small and do your best to follow the conventions of the project. If you have a feature that requires a lot of code changes, please create an issue before making a PR and wait for our comments. This will increase the chances of your PR getting in.

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If required, update the documentation.
4. Ensure the tests and linter checks pass.
5. Issue that pull request and wait for it to be reviewed by a maintainer or contributor!

Note: make sure to follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) guidelines when creating a PR.

## Development Guide

### Setup
We use poetry to manage dependencies. We recommend using pyenv to manage python versions.
After installing both, you can run

```bash
pyenv local 3.12.2
poetry env use 3.12.2
poetry install
```

Run `make dev` to initialize the multi_repo dbt projects.
These projects can be used to test stuff with the `dbt-opiner` cli.
For example, you can run: `poetry run dbt-opiner --log-level DEBUG lint -a` to lint all the projects and files and see the full debug output.

### Testing
We use pytest for unit testing. To run the tests, run `pytest`.
Functional tests are TBD.

### Some Style Rules
Docstrings follow the [Google Python Style Guide](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings).
