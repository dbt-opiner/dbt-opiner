# dbt-opiner
Tool for keeping dbt standards aligned across dbt projects.

## Installation and usage

### CLI
It can be installed as a python package (with pip, poetry, etc.) and used as a cli. Run `dbt-opiner -h` to see the available commands and parameters.

### Pre-commit hook
Also, it can be used as a pre-commit hook. To use it as a pre-commit hook, add the following to your `.pre-commit-config.yaml` file:

```yaml
repos:
  - repo: https://github.com/dbt-opiner/dbt-opiner
    rev: 0.1.0
    hooks:
      - id: dbt-opiner-lint
        args: [-f]
        additional_dependencies: [dbt-duckdb == 1.8.2]
```

Make sure to list in additional dependencies the dbt connector you are using. In this case, duckdb.

### Usage in CI pipelines
The tool can be used in CI pipelines. It will return a non-zero exit code if any opinion with severity `Must` is not met.

An environment variable `DBT_TARGET` can be set to specify the target to use when compiling the dbt manifest. If not set, the default target will be used.


### Important notes and additional configurations
The tool `expects all the linted files to belong to a git repository`: it won't work with files that are not part of a git repository.

Extra configs can be set using a `dbt-opiner.yaml` file, that should be anywhere in the repository.
If more than one `dbt-opiner.yaml` file is found, the tool will use the one in the highest path of the repository. If file is not provided, empty configuration will be used and default behaviour will be applied.

The `dbt-opiner.yaml` file should have the following structure:

```yaml
global:
  ignore_opinions: # To ignore some opinions list the opinion codes. Optional.
    - O001
  custom_opinions: # To set custom opinions. Optional.
    source: git # git or local. If local, it will load opinions from "the directory where this config file is"/custom_opinion/ directory.
    repository: https://github.com/dbt-opiner/dbt-opiner-custom-opinions.git # Only required if the source is git. TODO: Add support for private repositories.
    rev: 0.1.0 # Tag or commit sha, not branches. Revision is optional but encouraged. If not provided default main branch will be used.

sql:
  files: ".*/(models|macros|tests)/.*" # Regex to match the sql files to lint. Optional.
  opinions_config: # Extra config for opinions. Check the opinions documentation for more info.
    O002_keywords:
      - summary
      - granularity

# Configuration for other file types has the same structure as the sql configuration.
# yaml:
#  files:
#  opinions_config:

# md:
#  files:
#  opinions_config:

```

TODO: add link to example repo.

## Opinions
The opinions are defined in the `opinions` directory. They apply to certain dbt nodes (models, macros, or tests) and/or type of files (yaml, sql, md). The opinions have a code, a description, a severity, and a configuration.
The severity levels are: `Must` (it's mandatory) and `Should` (it's highly recommended).
The configuration is optional and can be used to set extra parameters for the opinion.

### Ignoring opinions (noqa)
Opinions can be ignored at the global level or at the node level.
To ignore an opinion at the global level, add the opinion code to the `ignore_opinions` list in the `dbt-opiner.yaml` file.
To ignore an opinion at the node (model, macro, or test) level, add a comment with the format: `noqa: dbt-opiner OXXX` at the beginning of the sql or yaml file, where `OXXX` is the opinion code. Use a comma separated list if you want to ignore more than one opinion (e.g. `noqa: dbt-opiner O001, O002`). You can also ignore all opinions in a node by using `noqa: dbt-opiner all`.
Note that if multiple nodes are defined in the same yaml file, the noqa comment will apply to all the nodes defined in that file.

TODO: add link to example repo.

### Default opinions

#### **O001 model must have a description** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O001_model_must_have_description.py)]
Applies to: dbt models when either sql or yaml files are changed
Models description is mandatory. Empty descriptions are not allowed.

#### **O002 model description must have keywords** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O002_model_description_must_have_keywords.py)]
Applies to: dbt models when either sql or yaml files are changed
Models description must have certain keywords. The keywords can be set in the configuration (see above).
This is useful if we want to enforce a certain structure in the descriptions. For example:

```
Summary:
  This model contains all the customers of jaffle_shop
Granularity:
  one row per user_id
```

Keywords are not case sensitive.

#### **O003 all columns must have description** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O003_all_columns_must_have_description.py)]
Applies to: dbt models when either sql or yaml files are changed
All columns must have a description. Empty descriptions are not allowed.

#### **O004 model should have unique key** [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O004_model_should_have_unique_key.py)]
Applies to: dbt models when sql files are changed
Models should have a unique key defined in the config block of the model. This is useful to enforce the uniqueness of the model and to make the granularity of the model explicit.

### Adding custom opinions
TODO

## Why?
TODO
