with customers as (

    select id, name from {{ ref('stg_customers') }}
    where 1 == 1 -- Some mock for not activeness

)

select * from customers
