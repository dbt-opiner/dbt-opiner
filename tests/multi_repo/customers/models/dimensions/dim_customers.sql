-- noqa: dbt-opiner O004
with customers as (

    select * from {{ ref('stg_customers') }}

)

select * from customers
