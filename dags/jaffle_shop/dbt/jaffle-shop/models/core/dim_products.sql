with

products as (

    select
        *,
        coalesce(product_type = 'jaffle', false) as is_food_item,
        coalesce(product_type = 'beverage', false) as is_drink_item

    from {{ ref('stg_products') }}

)

select * from products
