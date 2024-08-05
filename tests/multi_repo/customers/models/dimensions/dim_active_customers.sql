with customers as (

    select * from {{ ref('stg_customers') }}
    where 1 = 1 -- Some mock for activeness

)

select * from customers
