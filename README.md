[![PyPI - Version](https://img.shields.io/pypi/v/dbt-opiner)](https://pypi.org/project/dbt-opiner/)
[![PyPi Python Versions](https://img.shields.io/pypi/pyversions/dbt-opiner.svg?style=flat-square)](https://pypi.org/project/dbt-opiner/)

# dbt-opiner
Tool for keeping dbt standards aligned across dbt projects.

**Disclaimer: This is an early stage project. Check the [License](https://github.com/dbt-opiner/dbt-opiner/blob/main/LICENSE) for more information.**

# Table of Contents
1. [Installation and usage](#installation-and-usage)
    1. [CLI](#cli)
    2. [Pre-commit hook](#pre-commit-hook)
    3. [Usage in CI pipelines](#usage-in-ci-pipelines)
    4. [Important notes and additional configurations](#important-notes-and-additional-configurations)
2. [Opinions](#opinions)
    1. [Ignoring opinions (noqa)](#ignoring-opinions-noqa)
    2. [Default opinions](#default-opinions)
        1. [O001 model must have a description](#o001-model-must-have-a-description)
        2. [O002 model description must have keywords](#o002-model-description-must-have-keywords)
        3. [O003 all columns must have description](#o003-all-columns-must-have-description)
        4. [O004 All columns in model must be explicitly named at least once](#o004-all-columns-in-model-must-be-explicitly-named-at-least-once)
        5. [O005 model should have unique key](#o005-model-should-have-unique-key)
    3. [Adding custom opinions](#adding-custom-opinions)
3. [Why?](#why)
4. [Contributing](#contributing)

## Installation and usage

### CLI
It can be installed as a python package (with pip `pip install dbt-opiner`, poetry `poetry add dbt-opiner`, etc.) and used as a cli. Run `dbt-opiner -h` to see the available commands and options.

### Pre-commit hook
Also, it can be used as a pre-commit hook. To use it as a pre-commit hook, add the following to your `.pre-commit-config.yaml` file:

```yaml
repos:
  - repo: https://github.com/dbt-opiner/dbt-opiner
    rev: 0.1.0 # Tag or commit sha
    hooks:
      - id: dbt-opiner-lint
        args: [-f]
        additional_dependencies: [dbt-duckdb == 1.8.2] # Add the dbt connector you are using.
```

**Make sure to list in additional dependencies the dbt connector you are using. In this case, duckdb.**

### Usage in CI pipelines
The tool can be used in CI pipelines. It will return a non-zero exit code if any opinion with severity `Must` is not met.

An environment variable `DBT_TARGET` can be set to specify the target to use when compiling the dbt manifest. If not set, the default target will be used. The target can also be set using the `--target` option.

Check [this github action example](https://github.com/dbt-opiner/demo-multi-dbt-project/blob/main/.github/workflows/run_dbt_opiner.yaml) where a CI run is implemented.


### Important notes and additional configurations
The tool `expects all the linted files to belong to a git repository`: it won't work with files that are not part of a git repository.

Extra configs can be set using a `dbt-opiner.yaml` file, that should be anywhere in the repository.
If more than one `dbt-opiner.yaml` file is found, the tool will use the one in the highest path of the repository. If file is not provided, empty configuration will be used and default behaviour will be applied.

The `dbt-opiner.yaml` file should have the following structure:

```yaml
sqlglot_dialect: duckdb # The dialect to use when parsing the sql files with sqlglot.

opinions_config: # Extra config for opinions. Check the opinions documentation for more info.
  ignore_opinions: # To ignore some opinions list the opinion codes. Optional.
    - O001
  ignore_files:
    # The opinion is ignored in all the files that match the regex.
    # Use the opinion code as key and a regex as value.
    - O002: ".*/models/dimensions/.*"
  extra_opinions_config:
    O002_keywords:
      - summary
      - granularity
  custom_opinions: # To set custom opinions. Optional.
    source: git # git or local. If local, it will load opinions from "the directory where this config file is"/custom_opinion/ directory.
    repository: https://github.com/dbt-opiner/dbt-opiner-custom-opinions.git # Only required if the source is git. TODO: Add support for private repositories.
    rev: 0.1.0 # Tag or commit sha, not branches. Revision is optional but encouraged. If not provided default main branch will be used.

files: #Regex to match the files to lint. Optional.
  sql: ".*/(models|macros|tests)/.*" #
  #yaml:
  #md:

```

Check this repo as an example: [demo-multi-dbt-project](https://github.com/dbt-opiner/demo-multi-dbt-project/blob/main/.dbt_opiner/.dbt-opiner.yaml)

## Opinions
The opinions are defined in the `opinions` directory. They apply to certain dbt nodes (models, macros, or tests) and/or type of files (yaml, sql, md). The opinions have a code, a description, a severity, and a configuration.
The severity levels are: `Must` (it's mandatory) and `Should` (it's highly recommended).
The configuration is optional and can be used to set extra parameters for the opinion.

### Ignoring opinions (noqa)
Opinions can be ignored at the global level, at the node level, or by regex matching file paths.
To ignore an opinion at the global level, add the opinion code to the `ignore_opinions` list in the `dbt-opiner.yaml` file.
To ignore an opinion at the node (model, macro, or test) level, add a comment with the format: `noqa: dbt-opiner OXXX` at the beginning of the sql or yaml file, where `OXXX` is the opinion code. Use a comma separated list if you want to ignore more than one opinion (e.g. `noqa: dbt-opiner O001, O002`). You can also ignore all opinions in a node by using `noqa: dbt-opiner all`.
Note that if multiple nodes are defined in the same yaml file, the noqa comment will apply to all the nodes defined in that file.
To ignore opinions for certaing regex matching file paths, add the opinion code as key and a regex as value to the `ignore_files` list in the `dbt-opiner.yaml` file.

TODO: add link to example repo.

### Default opinions

#### **O001 model must have a description** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O001_model_must_have_description.py)]
Applies to: dbt models when either sql or yaml files are changed.
Models must have descriptions. Empty descriptions are not allowed.

Descriptions are important for documentation and understanding the purpose of the model.
A good description makes data more obvious.
Include a description for the model in a yaml file or config block.

#### **O002 model description must have keywords** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O002_model_description_must_have_keywords.py)]
Applies to: dbt models when either sql or yaml files are changed.
Models descriptions must have keywords.

Keywords help standarizing the description of the models,
and ensure that all the important information is present in the description.
Make sure the description of the model has all the required keywords.

The keywords can be set in the configuration like this:
```yaml
opinions_config:
  O002_keywords:
    - summary
    - granularity
```
Keywords are case insensitive.

#### **O003 all columns must have description** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O003_all_columns_must_have_description.py)]
Applies to: dbt models when either sql or yaml files are changed
All columns in the model should have a description. Empty descriptions are not allowed.

Descriptions are important for documentation and understanding the purpose
of the columns. A good description desambiguates the content of a column
and helps making data more obvious.

This opinion has some caveats. The only way of really knowning the
columns of a model is by running the model and checking the columns in
the database or catalog.json. However we don't want to depend on the execution of
the model.

This opinion checks if:
  - all defined columns in yaml files have a non empty description in the manifests.json.
  - the columns extracted by the sqlglot parser have a description in
    the manifest.json.

If the model is constructed in a way that not all columns are extracted by
the sqlglot parser, this opinion will omit those columns from the check.
Rule O004 will check against this condition and will fail if
unresolved `select *` are found.

#### **O004 All columns in model must be explicitly named at least once** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O004_final_columns_in_model_must_be_explicitly_named_at_least_once.py)]
Applies to: dbt models when sql files are changed
The final columns of the model must be explicitly named at least once.

This makes it easier to understand what columns are being selected at the end of the model,
without needing to check the lineage and the sources columns.

Ideally, all the columns should be named in the final select statement or CTE.
This rule doesn't check for that condition, but it checks if there is any
unresolved `select *` statement in the model.

** For example **

- Good:
with customers as (select * from {{ref('customers')}}),
orders as (select * from {{ref('orders')}}),
joined as (
    select
        customers.customer_id,
        customers.customer_name,
        orders.order_id,
        orders.order_date
    from customers
    join orders on customers.customer_id = orders.customer_id
)
select * from joined

- Also good:
with customers as (select customer_id, customer_name from {{ref('customers')}}),
orders as (select * from {{ref('orders')}}),
joined as (
    select
        customers.*, -- These are named in the CTE
        orders.order_id,
        orders.order_date
    from customers
    join orders on customers.customer_id = orders.customer_id
)
select * from joined

- Bad:
with customers as (select * from {{ref('customers')}}),
orders as (select * from {{ref('orders')}}),
joined as (
    select
        customers.*, -- We can't know the final columns that this model uses without checking dim_customers table
        orders.order_id,
        orders.order_date
    from customers
    join orders on customers.customer_id = orders.customer_id
)
select * from joined

#### **O005 model should have unique key** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O005_model_should_have_unique_key.py)]
Applies to: dbt models when sql files are changed
Models should have a unique key defined in the config block of the model.
This is useful to enforce the uniqueness of the model and to make the granularity of the model explicit.

### Adding custom opinions
If you want to add your own opinions, you can do so by creating new opinion classes in a custom_opinion directory in the same directory where the `.dbt-opiner.yaml` file is located or in a github repository (see for example: this [repo](https://github.com/dbt-opiner/dbt-opiner-custom-opinions/tree/main))

The opinion class should inherit from `dbt_opiner.opinions.BaseOpinion` and implement the `_eval` method. This method should check for file and node types (to avoid running the opinion in the wrong files or nodes) and evaluate the opinion. It can return a single or a list of `dbt_opiner.linter.LintResult`

If the custom opinion is in a repository and requires extra dependencies, define a class variable `required_dependencies` with the required dependencies in a list (e.g. ["numpy==2.0.1", "pandas==2.0"]).

The `_eval` method will receive a file handler to lint. Familiarize with these file handlers in the [source code](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/file_handlers.py). These file handlers also contain dbt manifest information that can be accessed in the evaluation of the opinion.

The custom opinion can use the configuration set in the `.dbt-opiner.yaml` file. The config dictionary is injected when the class is instantiated. To access it, define a `__init__` method with a `config` parameter (see for example [[this](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O002_model_description_must_have_keywords.py)])

All the configurations for [ignoring opinions (noqa)](#ignoring-opinions-noqa) will also apply to the custom opinions. Make sure you don't create conflicting opinion codes. As a best practice, use a prefix for the opinion codes that are specific to your organization (e.g. `C001`).

We use `loguru` for logging. We encourage to use the same for the custom opinions. See how to use it, [here](https://github.com/Delgan/loguru).

## Why?
Inspired in Benn Stancil's [blog post](https://benn.substack.com/p/the-rise-of-the-analytics-pretendgineer) where he says:
>My suspicion is that dbt [...] needs something that aggressively imposes [...] opinions on its users. It needs dbt on Rails: A framework that builds a project’s scaffolding, and tells us how to expand it—and not through education, but through functional requirements. Show where to put which bits of logic. Prevent me from doing circular things. Blare warnings at me when I use macros incorrectly. Force me to rigorously define “production.”

Far for being a dbt framework, this less ambitious tool aims to help enforcing standards and best practices in dbt projects at scale, as part of the federated data governance principle of a data mesh architecture.

Although other similar tools exist, they fall short in some aspects:
  - [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) doesn't work well with multi projects repositories and doesn't provide a way to define custom opinions.
  - [dbt-score](https://dbt-score.picnic.tech/) doesn't work as a pre-commit hook and it's oriented to check mainly the metadata of the nodes.

dbt-opiner tries to fill this gap. It can be used as a cli, as a pre-commit hook, and in CI pipelines, and it allows to define custom opinions that check multiple aspects of dbt projects such as:
  - Naming conventions
  - Data and unit tests checks
  - Documentation checks
  - Dependency checks
  - Security/Privacy checks
  - etc.

dbt-opiner is designed to be extensible and easy to configure, so it can be adapted to the specific needs of each organization.

This tool doesn't replace linters and formaters for sql code ([SQLFluff](https://github.com/sqlfluff/sqlfluff/), [sqlftm](https://github.com/tconbeer/sqlfmt)) that are highly encouraged.

## Contributing
Check the [CONTRIBUTING.md](https://github.com/dbt-opiner/dbt-opiner/blob/main/CONTRIBUTING.md) file for more information.
