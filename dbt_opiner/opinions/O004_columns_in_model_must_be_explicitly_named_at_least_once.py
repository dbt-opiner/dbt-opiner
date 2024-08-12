"""
The columns that are used in the model must be explicitly named at least once.
For example.

Good:
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

Bad:
with customers as (select * from {{ref('customers')}}),
orders as (select * from {{ref('orders')}}),
joined as (
    select
        customers.*, -- This is bad because we can't know the final columns that this model uses
        orders.order_id,
        orders.order_date
    from customers
    join orders on customers.customer_id = orders.customer_id
)
select * from joined
"""
