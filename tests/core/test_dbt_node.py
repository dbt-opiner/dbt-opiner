import pytest

from dbt_opiner import dbt


# test ast_extracted_columns() method for DbtNode
@pytest.mark.parametrize(
    "node_dict, expected_columns",
    [
        pytest.param(
            {"compiled_code": """select * from dim_customers"""},
            ["* from dim_customers"],
            id="no cte, select *",
        ),
        pytest.param(
            {"compiled_code": """select id, name from dim_customers"""},
            ["id", "name"],
            id="no cte, select columns",
        ),
        pytest.param(
            {"compiled_code": """select id as customer_id, name from dim_customers"""},
            ["customer_id", "name"],
            id="no cte, select columns one with alias",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with customers as (
                      select id, name from dim_cusomters
                  ),
                  orders as (
                    select customer_id, product_name, price  from orders
                  )
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id"""
            },
            ["id", "name", "product_name", "price", "is_expensive"],
            id="two ctes no select *",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with customers as (
                      select * from dim_customers
                  ),
                  orders as (
                    select customer_id, product_name, price  from orders
                  )
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id"""
            },
            ["* from customers", "product_name", "price", "is_expensive"],
            id="two ctes, one select *",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with dim_customers as (select * from dim_customers),
                  customers as (
                      select id, name from dim_customers
                  ),
                  orders as (
                    select customer_id, product_name, price  from orders
                  )
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id"""
            },
            ["id", "name", "product_name", "price", "is_expensive"],
            id="CTE with select * and then explicit columns",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with dim_customers as (select id, name from dim_customers),
                  customers as ( select * from dim_customers),
                  orders as ( select customer_id, product_name, price  from orders)
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id"""
            },
            ["id", "name", "product_name", "price", "is_expensive"],
            id="CTE with explicit columns and then select *",
        ),
        pytest.param(
            {"compiled_code": """select * from (select id from dim_customers)"""},
            ["id"],
            id="Nested query with qualified column in nested",
        ),
        pytest.param(
            {"compiled_code": """select id from (select * from dim_customers)"""},
            ["id"],
            id="Nested query with qualified column outside",
        ),
        pytest.param(
            {"compiled_code": """select * from (select * from dim_customers)"""},
            ["* from nested query"],
            id="Nested query both *",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with customers as (with orders as (select * from orders),
                  customers as (select id, name from dim_customers)
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                  from customers
                  left join orders on customers.id = orders.customer_id)
                  select * from customers"""
            },
            ["id", "name", "product_name", "price"],
            id="Nested CTEs with correct select *",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with stuff as (with orders as (select * from orders),
                  customers as (select * from dim_customers)
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                  from customers
                  left join orders on customers.id = orders.customer_id)
                  select * from stuff"""
            },
            ["* from stuff"],
            id="Nested CTEs with incorrect select *",
        ),  # TODO: fix, should be [* from customers, product_name, price]
        pytest.param(
            {
                "compiled_code": """
                  with customers as (select id, name from dim_cusomters),
                  orders as (select customer_id, product_name, price  from orders)
                  select
                    customers.* except(id),
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id"""
            },
            ["name", "product_name", "price", "is_expensive"],
            id="Query with select * except(...) from",
        ),
        pytest.param(
            {
                "compiled_code": """
                  with dim_customers as (select * from dim_customers),
                  customers as (select id, name from dim_customers),
                  orders as (select customer_id, product_name, price from orders
                  ),
                  final as (
                  select
                    customers.*,
                    orders.product_name,
                    orders.price,
                    case when orders.price > 100 then true else false end as is_expensive
                  from customers
                  left join orders on customers.id = orders.customer_id)
                  select * from final"""
            },
            ["id", "name", "product_name", "price", "is_expensive"],
            id="Final CTE with select *",
        ),
    ],
)
def test_ast_extracted_columns(node_dict, expected_columns):
    node = dbt.DbtNode(node_dict, "duckdb")
    node.ast_extracted_columns == expected_columns


# test logic in docs_yml_file_path property
@pytest.mark.parametrize(
    "node_dict, expected_path",
    [
        pytest.param(
            {"patch_path": "billing_bigquery://models/some_path/_folder__models.yml"},
            "billing_bigquery/models/some_path/_folder__models.yml",
            id="Valid patch_path with expected format",
        ),
        pytest.param(
            {"patch_path": None},
            "",
            id="None patch_path (e.g. for tests) returns empty string",
        ),
        pytest.param({}, "", id="Missing patch_path returns empty string"),
    ],
)
def test_docs_yml_file_path(node_dict, expected_path):
    node = dbt.DbtNode(node_dict)
    assert node.docs_yml_file_path == expected_path
