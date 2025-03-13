from typing import Any

from loguru import logger

from dbt_opiner import file_handlers
from dbt_opiner import linter
from dbt_opiner.dbt import DbtModel
from dbt_opiner.opinions import base_opinion


class P001(base_opinion.BaseOpinion):
    """Columns that contain Personal Identifiable Information (PII) should be tagged in the yaml file.

    A common practise in data engineering is to tag columns that contain PII.
    This allows to easily identify which columns contain sensitive information and
    to apply the necessary security measures (e.g. masking, access control, etc.).

    This opinion will check the existence in the model of any PII
    column name from a dictionary and verify if it is tagged in the yaml file.

    In BigQuery is a common practise to tag columns using policy tags instead of regular dbt tags.
    Make sure that `policy_tag = True` extra configuration is set if this applies to your case.

    Required extra configuration:
    You must specify these under the `opinions_config>extra_opinions_config>P001` key in your `.dbt-opiner.yaml` file.
    - pii_columns: dictionary of column names + list of pii tags.
                   for example:
                   ``` yaml
                    pii_columns:
                      email: ["pii_email", "pii_external"]
                  ```
    - policy_tag: bool (default: false) if true, the opinion will check for
                 the presence of a policy tag instead of tags.
    If no pii_columns are specified, the opinion will be skipped.
    """

    def __init__(self, config: dict[str, Any] = {}, **kwargs: dict[str, Any]) -> None:
        super().__init__(
            code="P001",
            description="Columns that contain Personal Identifiable Information (PII) must be tagged in the yaml file.",
            severity=linter.OpinionSeverity.MUST,
            config=config,
            tags=["privacy", "models"],
        )
        self._opinions_config = (
            config.get("opinions_config", {})
            .get("extra_opinions_config", {})
            .get("P001", {})
        )

        self._skip = not self._opinions_config.get("pii_columns")
        if self._skip:
            logger.warning("No pii_columns configured for P001. Skipping.")

    def _eval(self, file: file_handlers.FileHandler) -> list[linter.LintResult]:
        if self._skip:
            return []

        if self._opinions_config.get("policy_tag"):
            tag_type = "policy_tags"
        else:
            tag_type = "tags"

        nodes = []
        if isinstance(file, file_handlers.SqlFileHandler):
            if isinstance(file.dbt_node, DbtModel):
                nodes = [file.dbt_node]
        if isinstance(file, file_handlers.YamlFileHandler):
            nodes = [node for node in file.dbt_nodes if isinstance(node, DbtModel)]

        results = []

        for node in nodes:
            # Check if model has columns.
            # If there are no columns O003 will fail
            if not node.columns:
                logger.debug(f"Model {node.alias} has no columns defined.")
                continue

            # If it has columns, pii columns should be tagged
            untagged_pii_columns = []
            for key, value in node.columns.items():
                config_pii_tag = self._opinions_config.get("pii_columns").get(key, [])
                node_tag = value.get(tag_type, [])
                # Check if all config pii tags exists in node tag
                if all(tag in node_tag for tag in config_pii_tag):
                    continue
                else:
                    untagged_pii_columns.append(key)

            # Also, check ast_extracted_columns that doesn't exist in the columns keys
            for column in node.ast_extracted_columns:
                # Don't include unresolved select *
                if column not in node.columns.keys() and "*" not in column:
                    if self._opinions_config.get("pii_columns").get(column):
                        untagged_pii_columns.append(column)

            if len(untagged_pii_columns) > 0:
                results.append(
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=False,
                        severity=self.severity,
                        message=f"Column(s): {untagged_pii_columns} in model {node.alias} {self.severity.value} have a PII {tag_type}.",
                    )
                )
            else:
                results.append(
                    linter.LintResult(
                        file=file,
                        opinion_code=self.code,
                        passed=True,
                        severity=self.severity,
                        message=f"All columns in model {node.alias} have PII {tag_type}.",
                    )
                )
        return results
