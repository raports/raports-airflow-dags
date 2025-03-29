with

source as (
    select * from {{ source('dbt_jaffle_shop', 'raw_products') }}
),

renamed as (
    select
        ----------  ids
        sku as product_id,
        ---------- text
        name as product_name,
        product_type as product_type,
        description as product_description,
        ---------- numerics
        {{ cents_to_dollars('price') }} as product_price
    from source
)

select * from renamed