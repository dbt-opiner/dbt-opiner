[![PyPI - Version](https://img.shields.io/pypi/v/dbt-opiner)](https://pypi.org/project/dbt-opiner/)
[![PyPi Python Versions](https://img.shields.io/pypi/pyversions/dbt-opiner.svg?style=flat-square)](https://pypi.org/project/dbt-opiner/)
[![Coverage Status](https://coveralls.io/repos/github/dbt-opiner/dbt-opiner/badge.svg?branch=main)](https://coveralls.io/github/dbt-opiner/dbt-opiner?branch=main)

# dbt-opiner
Tool for keeping dbt standards aligned across dbt projects.

<img src="https://raw.githubusercontent.com/dbt-opiner/dbt-opiner/main/docs/demo.gif" width="742" height="342">

**Or use it in a CI run! check out [this](https://github.com/dbt-opiner/demo-multi-dbt-project/pull/2) or [this](https://github.com/dbt-opiner/demo-multi-dbt-project/pull/4) demo PRs**

# Table of Contents
1. [Installation and usage](#installation-and-usage)
    1. [CLI](#cli)
        1. [Lint](#lint)
        2. [Audit](#audit)
    2. [Pre-commit hook](#pre-commit-hook)
    3. [Usage in CI pipelines](#usage-in-ci-pipelines)
    4. [Important notes and additional configurations](#important-notes-and-additional-configurations)
        1. [Load configuration from a github repository](#load-configuration-from-a-github-repository)
2. [Opinions](#opinions)
    1. [Model metadata and configuration opinions](#model-metadata-and-configuration-opinions)
        1. [O001 Model must have a description](#O001-model-must-have-a-description-source)
        2. [O002 Model description must have keywords](#O002-model-description-must-have-keywords-source)
        3. [O003 All columns must have description](#O003-all-columns-must-have-description-source)
        4. [O004 All columns in model must be explicitly named at least once](#O004-all-columns-in-model-must-be-explicitly-named-at-least-once-source)
        5. [O005 Model should have unique key](#O005-model-should-have-unique-key-source)
        6. [O006 Models names must start with a prefix](#O006-models-names-must-start-with-a-prefix)
    2. [Lineage opinions](#lineage-opinions)
        1. [L001 Sources must only be used in staging layer](#L001-sources-must-only-be-used-in-staging-layer-source)
        2. [L002 layer x must not select from layer y](#L002-layer-x-must-not-select-from-layer-y-source)
    3. [Documentation files opinions](#documentation-files-opinions)
        1. [D001 yaml files should have n docs](#D001-yaml-files-should-have-n-docs-source)
    4. [Privacy opinions](#privacy-opinions)
        1. [P001 Columns that contain Personal Identifiable Information (PII) must be tagged in the yaml file](#P001-Columns-that-contain-Personal-Identifiable-Information-PII-must-be-tagged-in-the-yaml-file-source)
        2. [P002 dbt project must not send anonymous statistics](#P002-Dbt-project-must-not-send-anon-stats-source)
    5. [BigQuery opinions](#bigquery-opinions)
        1. [BQ001 Bigquery targets used for development and testing must have maximum_bytes_billed](#BQ001-Bigquery-targets-used-for-development-and-testing-must-have-maximum_bytes_billed-source)
        2. [BQ002 Models materialized as tables in BigQuery should have clustering defined](#BQ002-Models-materialized-as-tables-in-BigQuery-should-have-clustering-defined-source)
        3. [BQ003 Views must have documented the partition and cluster of underlying tables](#BQ003-Views-must-have-documented-the-partition-and-cluster-of-underlying-tables-source)
        4. [BQ004 The persist_docs option for models must be enabled](#BQ004-The-persist_docs-option-for-models-must-be-enabled-source)
    6. [Adding custom opinions](#adding-custom-opinions)
    7. [Ignoring opinions (noqa)](#ignoring-opinions-noqa)
3. [Why?](#why)
4. [Contributing](#contributing)

## Installation and usage

### CLI
It can be installed as a python package (with pip `pip install dbt-opiner`, poetry `poetry add dbt-opiner`, etc.) and used as a cli. Run `dbt-opiner -h` to see the available commands and options.
#### Lint
`dbt-opiner lint [ARGS]` will run the linter on the changed files or dbt projects and return a non-zero exit code if any opinion with severity `Must` is not met. It will also return a summary of the opinions that failed.

#### Audit
`dbt-opiner audit [ARGS]` will run the linter on full dbt project(s) and log a summary of the opinions that failed and passed. It's customizable to log with different levels of detail and aggregation. It is especially useful to check for the quality of the dbt project(s).

### Pre-commit hook
Also, it can be used as a pre-commit hook. To use it as a pre-commit hook, add the following to your `.pre-commit-config.yaml` file:

```yaml
repos:
  - repo: https://github.com/dbt-opiner/dbt-opiner
    rev: 0.1.0 # Tag or commit sha
    hooks:
      - id: dbt-opiner-lint
        args: ["--force-compile", "-f"] # -f is mandatory. We recommend to use --force-compile to force the compilation of the dbt manifest in each run.
        additional_dependencies: [dbt-duckdb == 1.8.2] # Add the dbt connector you are using.
```

**Make sure to list in additional dependencies the dbt connector you are using. In this case, duckdb.**

### Usage in CI pipelines
The tool can be used in CI pipelines. It will return a non-zero exit code if any opinion with severity `Must` is not met.

An environment variable `DBT_TARGET` can be set to specify the target to use when compiling the dbt manifest. If not set, the default target will be used. The target can also be set using the `--target` option.

Check [this github action example](https://github.com/dbt-opiner/demo-multi-dbt-project/blob/main/.github/workflows/run_dbt_opiner.yaml) where a CI run is implemented. Or see it in action in [this PR](https://github.com/dbt-opiner/demo-multi-dbt-project/pull/1).


### Important notes and additional configurations
The tool `expects all the linted files to belong to a git repository`: it won't work with files that are not part of a git repository.

Extra configs can be set using a `.dbt-opiner.yaml` file, that should be anywhere in the repository.
If more than one `.dbt-opiner.yaml` file is found, the tool will use the one in the highest path of the repository. If the file is not provided, an empty configuration will be used and default behavior will be applied.

The `.dbt-opiner.yaml` file should have the following structure:

```yaml
sqlglot_dialect: duckdb # The dialect to use when parsing the sql files with sqlglot.

shared_config: # Load config from a repository.
  repository: https://github.com/dbt-opiner/dbt-opiner-custom-opinions.git
  rev: 0.1.0 # Tag or commit sha, not branches. Revision is optional but encouraged. If not provided default main branch will be used.
  overwrite: false # If true, all configurations will be overwritten by the shared configuration.
  # If false, only new configurations will be added.
  # Empty defaults to true.

opinions_config: # Extra config for opinions. Check the opinions documentation for more info.
  ignore_opinions: "O001" # To ignore some opinions list the opinion codes. Optional.
  ignore_files:
    # The opinion is ignored in all the files that match the regex.
    # Use the opinion code as key and a regex as value.
    - O002: ".*/models/dimensions/.*"
  extra_opinions_config:
    O002:
      keywords:
      - summary
      - granularity
  custom_opinions: # To set custom opinions. Optional.
    source: git # git or local. If local, it will load opinions from "the directory where this config file is"/custom_opinion/ directory.
    repository: https://github.com/dbt-opiner/dbt-opiner-custom-opinions.git # Only required if the source is git.
    rev: 0.1.0 # Tag or commit sha, not branches. Revision is optional but encouraged. If not provided default main branch will be used.

files: #Regex to match the files to lint. Optional.
  sql: ".*/(models|macros|tests)/.*"
  yaml: ".*/(models|macros|tests)/.*"
  md: ".*/(docs)/.*"

```

Check this repo as an example: [demo-multi-dbt-project](https://github.com/dbt-opiner/demo-multi-dbt-project/blob/main/.dbt_opiner/.dbt-opiner.yaml).

The configuration file also accepts simple environment variables filling. The only supported environment variables are string types and must be set with `${{ env_var_name }}` syntax. 

#### Load configuration from a github repository
Guided by the federated governance principle of the Data Mesh architecture, the tool allows to load the configuration from a github repository. This is useful to adhere to a common set of standards and practices and share the same configuration across multiple dbt projects. The configuration can be loaded from a public or private repository. The repository should have a `.dbt-opiner.yaml` file with the configuration.
**Warning** Only use trusted repositories to load the configuration to prevent any security issues.

## Opinions
The opinions are defined in the `opinions` directory. They apply to certain dbt nodes (models, macros, or tests) and/or type of files (yaml, sql, md). The opinions have a code, a description, a severity, and a configuration.
The severity levels are: `Must` (it's mandatory) and `Should` (it's highly recommended).
The configuration is optional and can be used to set extra parameters for the opinion.

### Model metadata and configuration opinions

#### O001 Model must have a description [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O001_model_must_have_description.py)]

Applies to: dbt models when either sql or yaml files are changed.  
Models must have descriptions. Empty descriptions are not allowed.

Descriptions are important for documentation and understanding the purpose of the model.
A good description makes data more obvious.
Include a description for the model in a yaml file or config block.

---

#### O002 Model description must have keywords [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O002_model_description_must_have_keywords.py)]

Applies to: dbt models when either sql or yaml files are changed.  
Models descriptions must have keywords.

Keywords help standarizing the description of the models,
and ensure that all the important information is present in the description.
Make sure the description of the model has all the required keywords.

The keywords can be set in the configuration like this:
```yaml
opinions_config:
  O002:
    keywords:
      - summary
      - granularity
```
Keywords are case insensitive.

---

#### O003 All columns must have description [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O003_all_columns_must_have_description.py)]

Applies to: dbt models when either sql or yaml files are changed.  
All columns in the model should have a description. Empty descriptions are not allowed.

Descriptions are important for documentation and understanding the purpose
of the columns. A good description disambiguates the content of a column
and helps make data more obvious.

This opinion has some caveats. The only way of really knowing the
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

---

#### O004 All columns in model must be explicitly named at least once [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O004_final_columns_in_model_must_be_explicitly_named_at_least_once.py)]

Applies to: dbt models when sql files are changed.  
The final columns of the model must be explicitly named at least once.

This makes it easier to understand what columns are being selected at the end of the model,
without needing to check the lineage and the sources columns.

Ideally, all the columns should be named in the final select statement or CTE.
This rule doesn't check for that condition, but it checks if there is any
unresolved `select *` statement in the model.

**For example**

- Good:
```sql
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
```

- Also good:
```sql
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
```
- Bad:
```sql
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
```

---

#### O005 Model should have unique key [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O005_model_should_have_unique_key.py)]

Applies to: dbt models when sql files are changed.  
Models should have a unique key defined in the config block of the model.  
This is useful to enforce the uniqueness of the model and to make the granularity of the model explicit.

---

#### O006 Models names must start with a prefix [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O006_models_names_must_start_with_a_prefix.py)]
Applies to: dbt models when sql files are changed.  
Models must start with a prefix that specifies the layer of the model.  

Creating a consistent pattern of file naming is crucial in dbt.  
File names must be unique and correspond to the name of the model when selected and created in the warehouse.

We recommend putting as much clear information into the file name as possible, including a prefix for the layer the model exists in, important grouping information, and specific information about the entity or transformation in the model.

See file names secion: https://docs.getdbt.com/best-practices/how-we-structure/2-staging#staging-files-and-folders

Extra configuration:  
You can specify these under `opinions_config>extra_opinions_config>O006` key in your `.dbt-opiner.yaml` file.
  - accepted_prefixes: list of prefixes that are accepted. If not specified, the opinion will use:
    ['base', 'stg', 'int', 'fct', 'dim', 'mrt', 'agg']

Note:  
Layers can be excluded using a regex pattern under the `ignore_files>O006` key in your `.dbt-opiner.yaml` file.

---
---

### Lineage opinions
#### L001 Sources must only be used in staging layer [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/L001_sources_must_only_be_used_in_staging.py)]
Sources must only be used in staging layer.

Staging models are the entrypoint for raw data in dbt projects, and it is the
only place were we can use the source macro.
See more [here](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)

This opinion checks if the __source macro__ is only used in staging models.

Extra configuration:
Sometimes when dbt is run in CI all models end up in the same schema.
By specifying a node alias prefix we can still enforce this rule.
You can specify these under the `opinions_config>extra_opinions_config>L001` key in your `.dbt-opiner.yaml` file.
- staging_schema: schema name for staging tables (default: staging)
- staging_prefix: prefix for staging tables (default: stg_)

---

#### L002 layer x must not select from layer y [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/L002_layer_x_must_not_select_from_layer_y.py)]
Applies to: dbt models when sql files are changed.

Layer directionality must be respected.

Maintaining a good lineage crucial for any dbt project, and layer directionality is a key part of it.
If the layer directionality is not respected, it can lead to circular dependencies between layers and make the data model harder to understand and to schedule.

This opinion checks if the layer directionality is respected.

For example:
- layer `stg` should not select from a layer `fct` or `mrt`.
- layer `fct` should not select from a layer `mrt`.
- layer `mrt` should not select from a layer `stg`.

Required extra configuration:
You must specify these under the `opinions_config>extra_opinions_config>L002` key in your `.dbt-opiner.yaml` file.
``` yaml
  - layer_pairs: #list of forbidden layer pairs
    - "staging,stg selects from facts,fct"
    - "staging,stg selects from marts,mrt"
    - "facts,fct selects from marts,mrt"

```

The first value is the schema layer name and the second the prefix.
If by some case your models are all in the same schema (for example in CI) and that schema is not found, a check by prefixes will be used. **Prefixes can't be ommited**. If you don't use prefixes, use a generic one like `foo`.

If no pairs are specified, the opinion will be skipped.

---
---
### Documentation files opinions

#### yaml files should have n docs [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/D001_yaml_docs_should_have_n_docs.py)]

Applies to yaml files that contain dbt nodes documentation. 

Yaml files used for documentation should have a limited number of models or sources.

Although dbt allows to put multiple nodes inside the same yaml file, having a limited ammount of nodes per yaml file makes it easier to find the documentation for a specific element and keeps the files short.

This opinion checks if each yaml file containing models or sources contains more than a specified number(default 1).  
You can specify these under the `opinions_config>extra_opinions_config>D001` key in your `.dbt-opiner.yaml` file.
    - max_n_allowed: number of docs allowed per yaml file

---
---

### Privacy opinions

#### P001 Columns that contain Personal Identifiable Information (PII) must be tagged in the yaml file [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/P001_pii_columns_must_have_tags.py)]

Applies to: dbt models when either sql or yaml files are changed.  
Columns that contain Personal Identifiable Information (PII) must be tagged in the yaml file.

A common practise in data engineering is to tag columns that contain PII. This allows to easily identify which columns contain sensitive information and to apply the necessary security measures (e.g. masking, access control, etc.).

This opinion will check the existence in the model of any PII column name from a dictionary and verify if it is tagged in the yaml file.

In BigQuery is a common practise to tag columns using policy tags instead of regular dbt tags. Make sure that `policy_tag = True` extra configuration is set if this applies to your case.

Required extra configuration:
You must specify these under the `extra_opinions_config>P001` key in your `.dbt-opiner.yaml` file.
- pii_columns: dictionary of column names + list of pii tags. For example:
``` yaml
pii_columns:
  email: ["pii_email", "pii_external"]
```
- policy_tag: bool (default: false) if true, the opinion will check for
              the presence of a policy tag instead of tags.  

If no pii_columns are specified, the opinion will be skipped.

---

#### P002 dbt project must not send anonymous statistics [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/P002_project_must_not_send_anon_stats.py)] 

Applies to: dbt_project.yml and profiles.yml files. 
Sending anonymous statistics is enabled by default (opt-out). Although is a good way to help the dbt team improve the product, for privacy reasons we recommend to disable this feature.

This opinion checks if the `send_anonymous_usage_stats` flag is set to `False` in the `dbt_project.yml` file.
Previous to dbt 1.8 this flag was set in profiles.yml. This opinion will also check if it's present there.

---
---

### BigQuery Opinions
Opinions with the code starting in BQ are specific to BigQuery. They will only evaluate files if the sqlglot dialect is set to `bigquery` in `.dbt-opiner.yaml` file.

#### BQ001 Bigquery targets used for development and testing must have maximum_bytes_billed [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/BQ001_bigquery_targets_must_have_maximum_bytes_billed.py)] 
Applies to: profiles.yml file changes.

Bigquery targets used for development and testing must have maximum_bytes_billed set to prevent unexpected costs.

This opinion checks if the `maximum_bytes_billed` parameter is set in the target.
An optional list of target names to ignore can be specified in the configuration. By default, it ignores the `prod` and `production` targets. To disable this and check all targets, set the `ignore_targets` configuration to an empty list.
Also, an optional `maximum_bytes_billed` parameter can be set to specify the maximum number of bytes billed allowed. By default it is not checked.

Extra configuration:
You can specify these under the `opinions_config>extra_opinions_config>BQ001` key in your `.dbt-opiner.yaml` file.
  - ignore_targets: list of target names to ignore (default: ['prod', 'production'])
  - maximum_bytes_billed: maximum bytes billed allowed (optional)

---

#### BQ002 Models materialized as tables in BigQuery should have clustering defined [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/BQ002_bigquery_tables_should_have_clustering.py)]

Applies to: dbt models when sql files are changed. 

Models materialized as tables in BigQuery should have clustering defined.

Clustering is a feature in BigQuery that allows you to group your data based
on the contents of one or more columns in the table.
This can help improve query performance, reduce costs, and optimize your data
for analysis.
A table with clustering also optimizes "limit 1" queries, as it can skip scanning.

This opinion checks if models materialized as tables in BigQuery have clustering defined.

---

#### BQ003 Views must have documented the partition and cluster of underlying tables [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/BQ003_bigquery_views_must_have_partition_and_cluster_description.py)]

Applies to: dbt models when either sql or yaml files are changed.
Views must have documented the partition and cluster of underlying tables.  

Views that select underlying tables must have a description that explains
the partition and clustering that will impact view usage performance.
Since BigQuery does not show the partition and clustering information for views,
it is important to document this information in the view description in dbt.  

This opinion checks if the description of the view has the keywords 'partition' and 'cluster'.  
The check is case insensitive.

---

#### BQ004 The persist_docs option for models must be enabled [[source](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/BQ004_bigquery_models_must_persist_docs.py)]

Applies to: dbt_project.yml file changes  

The persist_docs option for models must be enabled to ensure that the documentation
is shown in the BigQuery console.  

Add

```yaml
models:
  +persist_docs:
    relation: true
    columns: true
```
To your dbt_project.yml file to enable this option.  

---
---

### Adding custom opinions
> Opinions are like assholes, everyone has one. [_Popular wisdom_](https://en.wiktionary.org/wiki/opinions_are_like_assholes)

If you want to add your own opinions, you can do so by creating new opinion classes in a custom_opinion directory in the same directory where the `.dbt-opiner.yaml` file is located or in a github repository (see for example: this [repo](https://github.com/dbt-opiner/dbt-opiner-custom-opinions/tree/main))

The opinion class should inherit from `dbt_opiner.opinions.BaseOpinion` and implement the `_eval` method. This method should check for file and node types (to avoid running the opinion in the wrong files or nodes) and evaluate the opinion. It can return a single or a list of `dbt_opiner.linter.LintResult`

If the custom opinion is in a repository and requires extra dependencies, define a class variable `required_dependencies` with the required dependencies in a list (e.g. ["numpy==2.0.1", "pandas==2.0"]).

The `_eval` method will receive a file handler to lint. Familiarize with these file handlers in the [source code](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/file_handlers.py). In general, the file handlers contain the file raw content, dbt node(s) with manifest metadata, and the parent dbt project to which it belong. All these are useful to create and evaluate opinions.

The custom opinion can use the configuration set in the `.dbt-opiner.yaml` file. The config dictionary is injected when the class is instantiated. To access it, define a `__init__` method with a `config` parameter (see for example [this](https://github.com/dbt-opiner/dbt-opiner/blob/main/dbt_opiner/opinions/O002_model_description_must_have_keywords.py)])

All the configurations for [ignoring opinions (noqa)](#ignoring-opinions-noqa) will also apply to the custom opinions. Make sure you don't create conflicting opinion codes. As a best practice, use a prefix for the opinion code specific to your organization (e.g. `C001`).

We use `loguru` for logging. We encourage to use the same for the custom opinions. See how to use it, [here](https://github.com/Delgan/loguru).

**Warning** Only use trusted repositories to load opinions to prevent any security issues.

### Ignoring opinions (noqa)
> Strong opinions, weakly held. [_Paul Saffo_](https://bobsutton.typepad.com/my_weblog/2006/07/strong_opinions.html)

Opinions can be ignored at the global level, at the node level, or by regex matching file paths.
To ignore an opinion at the global level, add the opinion code to the `ignore_opinions` list in the `.dbt-opiner.yaml` file.
To ignore an opinion at the node (model, macro, or test) level, add a comment with the format: `noqa: dbt-opiner OXXX` at the beginning of the sql or yaml file, where `OXXX` is the opinion code. Use a comma separated list if you want to ignore more than one opinion (e.g. `noqa: dbt-opiner O001, O002`). You can also ignore all opinions in a node by using `noqa: dbt-opiner all`.
Note that if multiple nodes are defined in the same yaml file, the noqa comment will apply to all the nodes defined in that file.
To ignore opinions for certaing regex matching file paths, add the opinion code as key and a regex as value to the `ignore_files` list in the `.dbt-opiner.yaml` file.

## Why?
Inspired by Benn Stancil's [blog post](https://benn.substack.com/p/the-rise-of-the-analytics-pretendgineer) where he says:
>My suspicion is that dbt [...] needs something that aggressively imposes [...] opinions on its users. It needs dbt on Rails: A framework that builds a project’s scaffolding, and tells us how to expand it—and not through education, but through functional requirements. Show where to put which bits of logic. Prevent me from doing circular things. Blare warnings at me when I use macros incorrectly. Force me to rigorously define “production.”

Far from being a dbt framework, this less ambitious tool aims to help enforce standards and best practices in dbt projects at scale, as part of the data mesh architecture’s federated governance principle.

Although other similar tools exist, they fall short in some aspects:
  - [dbt-project-evaluator](https://dbt-labs.github.io/dbt-project-evaluator/latest/) doesn't work well as a pre-commit hook and in a multi project repo. Also it's a nightmare of jinja macros and dbt_project configs that need to be set up in every exisiting dbt project that you want to evaluate.
  - [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) doesn't work well with multi projects repositories and doesn't provide a way to define custom opinions (with different levels of severity).
  - [dbt-score](https://dbt-score.picnic.tech/) doesn't work as a pre-commit hook and it's oriented to check mainly the metadata of the nodes. Other checks like one model per yml file or explicit column selection are not easy to integrate.

dbt-opiner tries to fill this gap.
It can be used as a CLI, pre-commit hook, and in CI pipelines. It allows the definition of custom opinions for checking multiple aspects of dbt projects such as:
  - Naming conventions
  - Data and unit tests checks
  - Documentation checks
  - Dependency checks
  - Security/Privacy checks
  - etc.

dbt-opiner is designed to be extensible and easy to configure so that it can be adapted to the specific needs of each organization.
Furthermore, the _opinion_ approach allows to suggest and educate the users about why something is a must (or why it is just suggested).

This tool doesn't replace linters and formaters for sql code ([SQLFluff](https://github.com/sqlfluff/sqlfluff/), [sqlftm](https://github.com/tconbeer/sqlfmt)) that are highly encouraged.

## Contributing
Check the [CONTRIBUTING.md](https://github.com/dbt-opiner/dbt-opiner/blob/main/CONTRIBUTING.md) file for more information.
