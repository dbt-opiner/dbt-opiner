with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        {{ dbt_utils.safe_add(['customer_id', 1]) }} as customer_id_plus_one
    from source

)

select * from renamed
