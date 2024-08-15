from dbt_opiner.file_handlers import SqlFileHandler
from dbt_opiner.linter import LintResult
from dbt_opiner.linter import OpinionSeverity
from dbt_opiner.opinions.base_opinion import BaseOpinion


class O004(BaseOpinion):
    """
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
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            code="O004",
            description="The final columns of the model must be explicitly named at least once.",
            severity=OpinionSeverity.MUST,
            tags=["sql style", "models"],
        )

    def _eval(self, file: SqlFileHandler) -> LintResult:
        if file.type == ".sql" and file.dbt_node.type == "model":
            not_qualified_stars = [
                star for star in file.dbt_node.ast_extracted_columns if "*" in star
            ]
            if not_qualified_stars:
                return LintResult(
                    file=file,
                    opinion_code=self.code,
                    passed=False,
                    severity=self.severity,
                    message=f"The final columns in model {file.dbt_node.alias} {self.severity.value} be explicitly named at least once. Unresolved select * statement(s): {not_qualified_stars}",
                )
            return LintResult(
                file=file,
                opinion_code=self.code,
                passed=True,
                severity=self.severity,
                message=f"The final columns in model {file.dbt_node.alias} are explicitly named at least once.",
            )
