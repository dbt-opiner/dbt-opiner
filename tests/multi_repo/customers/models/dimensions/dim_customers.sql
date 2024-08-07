{{
    config(
        materialized='view',
        unique_key='id'
    )
}}

with customers as (

    select * from {{ ref('stg_customers') }}

)

select * from customers
