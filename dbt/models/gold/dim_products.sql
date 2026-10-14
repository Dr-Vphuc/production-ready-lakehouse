with products as (

    select
        product_id,
        max_by(brand, event_time) filter (where brand is not null)  as brand,
        max_by(price, event_time) filter (where price is not null)  as last_price,
        max_by(
            struct(category_id, category_code, category_l1, category_l2, category_l3),
            event_time
        )                                                           as category,
        min(event_date)                                             as first_seen_date,
        max(event_date)                                             as last_seen_date
    from {{ ref('events') }}
    group by product_id

)

select
    product_id,
    brand,
    category.category_id      as category_id,
    category.category_code    as category_code,
    category.category_l1      as category_l1,
    category.category_l2      as category_l2,
    category.category_l3      as category_l3,
    last_price,
    first_seen_date,
    last_seen_date
from products