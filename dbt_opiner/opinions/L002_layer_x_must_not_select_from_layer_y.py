from collections import defaultdict

from loguru import logger

from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class L002(BaseOpinion):
    """Layer directionality must be respected.

    Maintaining a good lineage crucial for any dbt project, and
    layer directionality is a key part of it.
    If the layer directionality is not respected, it can lead to
    circular dependencies between layers and make the data model harder to understand and to schedule.

    This opinion checks if the layer directionality is respected.

    For example:
    - layer `stg` should not select from a layer `fct` or `mrt`.
    - layer `fct` should not select from a layer `mrt`.
    - layer `mrt` should not select from a layer `stg`.

    This rule requires extra configuration that state the forbidden layer pairs.
    You can specify these under the `opinions_config>extra_opinions_config>L002` key in your `.dbt-opiner.yaml` file.
      - layer_pairs: #list of forbidden layer pairs
        - "staging,stg selects from facts,fct"
        - "staging,stg selects from marts,mrt"
        - "facts,fct selects from marts,mrt"
        ... etc.
    The first value is the schema layer name and the second the prefix.
    If in CI run all models end up in the same schema a check by prefixes is used.
    You can omit the prefix if it doesn't apply to your case.
    """

    def __init__(self, config: dict, **kwargs) -> None:
        super().__init__(
            code="L002",
            description="Layer directionality must be respected.",
            severity=OpinionSeverity.MUST,
            config=config,
            tags=["lineage", "models"],
        )

        self._opinions_config = (
            self._config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("L002", {})
        )

        self._skip = not self._opinions_config.get("layer_pairs")
        if self._skip:
            logger.warning("No layer pairs configured for L002. Skipping.")

        self._schema_lineage_restrictions, self._prefix_lineage_restrictions = (
            self._get_select_restrictions()
        )

    def _eval(self, file: SqlFileHandler) -> LintResult | None:
        if self._skip:
            return None

        if file.dbt_node.type != "model" or file.type != ".sql":
            return None

        selected_models = [
            file.parent_dbt_project.dbt_manifest.nodes.get(model)
            for model in file.dbt_node.get("depends_on", {}).get("nodes", [])
        ]

        # If there are no selected models, there's nothing to check
        if not selected_models:
            return None

        node_prefix = file.dbt_node.alias.split("_")[0]
        forbidden_selects = []

        # Check for schema restrictions
        if self._schema_lineage_restrictions.get(file.dbt_node.schema):
            restricted_schemas = [
                layer.schema
                for layer in self._schema_lineage_restrictions.get(file.dbt_node.schema)
            ]
            for selected_model in selected_models:
                if selected_model.schema in restricted_schemas:
                    forbidden_selects.append(selected_model.alias)
        # If schema is not found, check for prefix restrictions
        elif self._prefix_lineage_restrictions.get(node_prefix):
            restricted_prefixes = [
                layer.prefix
                for layer in self._prefix_lineage_restrictions.get(node_prefix)
            ]
            for selected_model in selected_models:
                if selected_model.alias.split("_")[0] in restricted_prefixes:
                    forbidden_selects.append(selected_model.alias)
        # If no restrictions are found for this layer, return None
        else:
            return None

        if forbidden_selects:
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=False,
                severity=self.severity,
                message=(
                    f"Layer directionality {self.severity.value} be respected. "
                    f"Model {file.dbt_node.alias} selects from {', '.join(forbidden_selects)}."
                ),
            )

        return LintResult(
            file=file,
            opinion_code=self.code,
            passed=True,
            severity=self.severity,
            message=("Layer directionality is respected."),
        )

    def _get_select_restrictions(self) -> tuple[dict, dict]:
        def _verify_comma_split(pair_string: str) -> bool:
            return len(pair_string.split(",")) == 2

        schema_lineage_restrictions = defaultdict(list)
        prefix_lineage_restrictions = defaultdict(list)

        for pair in self._opinions_config.get("layer_pairs", []):
            if " selects from " not in pair:
                logger.warning(
                    f"Invalid layer pair configuration: {pair}. "
                    "Layer pairs must be in the format 'schema1,prefix1 selects from schema2,prefix2'."
                )
                continue

            restriction_pair = pair.split(" selects from ")

            if len(restriction_pair) != 2:
                logger.warning(
                    f"Invalid layer pair configuration: {pair}. "
                    "Layer pairs must be in the format 'schema1,prefix1 selects from schema2,prefix2'."
                )
                continue

            if not _verify_comma_split(restriction_pair[0]) or not _verify_comma_split(
                restriction_pair[1]
            ):
                logger.warning(
                    f"Invalid layer pair configuration: {pair}. "
                    "Layer pairs must be in the format 'schema1,prefix1 selects from schema2,prefix2'."
                )
                continue

            selector_layer = [x.strip() for x in restriction_pair[0].split(",")]
            schema_lineage_restrictions[selector_layer[0]].append(
                Layer(restriction_pair[1])
            )
            prefix_lineage_restrictions[selector_layer[1]].append(
                Layer(restriction_pair[1])
            )

        return schema_lineage_restrictions, prefix_lineage_restrictions


class Layer:
    def __init__(self, pair_string: str) -> None:
        self.schema, self.prefix = [x.strip() for x in pair_string.split(",")]

    def __str__(self) -> str:
        return f"{self.schema} ({self.prefix})"
