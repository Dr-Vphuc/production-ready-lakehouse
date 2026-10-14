{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['months(event_time)']
    )
}}

{% set month = var('month', none) %}

with source as (

    select *
    from {{ source('bronze', 'events') }}
    {% if is_incremental() and month %}
    where event_time >= to_timestamp('{{ month }}', 'yyyy-MM')
      and event_time <  to_timestamp('{{ month }}', 'yyyy-MM') + interval 1 month
    {% endif %}

),

deduped as (

    select
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,
        count(*)          as source_row_count,
        max(_ingested_at) as _ingested_at
    from source
    group by
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session

)

select
    event_time,
    to_date(event_time)                                as event_date,
    event_type,
    product_id,
    category_id,
    category_code,
    try_element_at(split(category_code, '[.]'), 1)     as category_l1,
    try_element_at(split(category_code, '[.]'), 2)     as category_l2,
    try_element_at(split(category_code, '[.]'), 3)     as category_l3,
    brand,
    case when price < 0 then null else price end       as price,
    user_id,
    user_session,
    source_row_count,
    _ingested_at
from deduped
