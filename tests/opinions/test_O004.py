import pytest

from dbt_opiner.dbt import DbtNode
from dbt_opiner.opinions import O004


@pytest.mark.parametrize(
    "mock_sqlfilehandler, expected_passed",
    [
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "compiled_code": """
                      with customers as (select customer_id, customer_name from customers),
                          orders as (select * from orders),
                          joined as (
                              select
                                  customers.*, -- These are named in the CTE
                                  orders.order_id,
                                  orders.order_date
                              from customers
                              join orders on customers.customer_id = orders.customer_id
                          )
                          select * from joined""",
                    }
                )
            ),
            True,
            id="select * customers are named in the CTE",
        ),
        pytest.param(
            (
                DbtNode(
                    {
                        "resource_type": "model",
                        "compiled_code": """
                      with customers as (select * from customers),
                          orders as (select * from orders),
                          joined as (
                              select
                                  customers.customer_id,
                                  customers.customer_name,
                                  orders.order_id,
                                  orders.order_date
                              from customers
                              join orders on customers.customer_id = orders.customer_id
                          )
                          select * from joined""",
                    }
                )
            ),
            True,
            id="select * customers are named in the join CTE",
        ),
        pytest.param(
            DbtNode(
                {
                    "resource_type": "model",
                    "compiled_code": """
                      with customers as (select * from customers),
                      orders as (select * from orders),
                      joined as (
                          select
                              customers.*, -- We can't know the final columns that this model uses without checking dim_customers table
                              orders.order_id,
                              orders.order_date
                          from customers
                          join orders on customers.customer_id = orders.customer_id
                      )
                      select * from joined""",
                }
            ),
            False,
            id="select * customers can't be resolved",
        ),
    ],
    indirect=["mock_sqlfilehandler"],
)
def test_sql_C004(mock_sqlfilehandler, expected_passed):
    opinion = O004()
    result = opinion.check_opinion(mock_sqlfilehandler)
    assert result.passed == expected_passed