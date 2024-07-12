# dbt opiner tool

## Why?

In a data mesh architecture each domain implements their data products in a self-serve independent way. However there's a need for a Federated Computational Governance that ensures compliance and standardization across domains while allowing for domain-specific flexibility.

dbt is a wide-use tool for ELT (Extract, Load, Transform) processes that can be used to implement data products in a data mesh architecture. dbt is very flexible (not opinionated) which makes it good for any type of organization and data modelling technique, but this opens the door to a lack of standardization and compliance across multiple projects.

Some tools already exist to tackle some of these aspects, for example:
- Linters and formaters for sql code ([SQLFluff](https://github.com/sqlfluff/sqlfluff/), [sqlftm](https://github.com/tconbeer/sqlfmt))
- Wide variety of checks for dbt projects: [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint)
- Metadata linting: [dbt-score](https://dbt-score.picnic.tech/)
- PR review tool: [recce](https://github.com/datarecce/recce)

These tools are not integrated and require manual work to set up. Moreover, the opinions or rules about how a dbt project should be are implicit and often not easily configurable.

## What
This project aims to provide a unified tool that integrates all these features and provides an easy way to configure and define new opinions. The tool should be able to:
- Lint SQL and YAML files
- Check dbt projects for best practices. 
  - Best practices are defined with default opinions that can be enabled or disabled. 
  - Opinions can be defined by the user in a custom folder (that can be fetched from a git repository)
  - Opinions have different severity: enforced, recommended, optional
  - Best practices include:
    - Naming conventions
    - Metadata checks
    - Documentation checks
    - Dependency checks
    - Performance checks
    - Security/Privacy checks
    - etc.
- Generates a report with the results of the checks and suggestions for improvement in PR CI steps.

## How
TBD