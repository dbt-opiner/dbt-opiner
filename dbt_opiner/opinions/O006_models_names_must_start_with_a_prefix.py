from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O006(BaseOpinion):
    """Models must start with a prefix that specifies the layer of the model.

    Creating a consistent pattern of file naming is crucial in dbt.
    File names must be unique and correspond to the name of the model when
    selected and created in the warehouse.

    We recommend putting as much clear information into the file name as possible,
    including a prefix for the layer the model exists in,
    important grouping information, and specific information about the entity or
    transformation in the model.

    See file names secion: https://docs.getdbt.com/best-practices/how-we-structure/2-staging#staging-files-and-folders

    Extra configuration:
    You can specify these under `opinions_config>extra_opinions_config>O006` key in your `.dbt-opiner.yaml` file.
      - accepted_prefixes: list of prefixes that are accepted. If not specified, the opinion will use:
        ['base', 'stg', 'int', 'fct', 'dim', 'mrt', 'agg']

    Note:
    Layers can be excluded using a regex pattern under the `ignore_files>O006` key in your `.dbt-opiner.yaml` file.
    """

    def __init__(self, config: dict, **kwargs) -> None:
        super().__init__(
            code="O006",
            description="Models must start with a prefix.",
            severity=OpinionSeverity.MUST,
            config=config,
            tags=["naming conventions", "models"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("O006", {})
        )

    def _eval(self, file: SqlFileHandler) -> LintResult | None:
        if file.type == ".sql" and file.dbt_node.type == "model":
            accepted_prefixes = self._opinions_config.get(
                "accepted_prefixes", ["base", "stg", "int", "fct", "dim", "mrt", "agg"]
            )

            if file.dbt_node.alias.split("_")[0] in accepted_prefixes:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=True,
                    severity=self.severity,
                    message="Model starts with a valid prefix.",
                )
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=(
                    f"Model {file.dbt_node.alias} {self.severity.value} start with a prefix that specifies the layer of the model. "
                    f"Accepted prefixes are: {accepted_prefixes}."
                ),
            )
