{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['months(event_time)']
    )
}}

{% set month = var('month', none) %}

{#-
  Chọn các tháng (partition) cần build lại:
  - Có truyền month (DAG lakehouse_monthly): chỉ tháng đó.
  - Không truyền month (DAG lakehouse_intraday): các tháng có dòng mới trong bronze
    kể từ lần build trước, nhận biết qua _ingested_at lớn hơn giá trị lớn nhất đã có ở silver.
  Mỗi tháng được chọn sẽ được build lại trọn vẹn, vì insert_overwrite thay cả partition.
-#}
{% if is_incremental() and month %}
    {% set months = [month] %}
{% elif is_incremental() %}
    {% set new_months_sql %}
        select distinct date_format(event_time, 'yyyy-MM')
        from {{ source('bronze', 'events') }}
        where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endset %}
    {% set months = run_query(new_months_sql).columns[0].values() if execute else [] %}
{% endif %}

with source as (

    select *
    from {{ source('bronze', 'events') }}
    {% if is_incremental() %}
    where
    {%- for m in months %}
        {% if not loop.first %}or {% endif -%}
        (event_time >= to_timestamp('{{ m }}', 'yyyy-MM')
         and event_time < to_timestamp('{{ m }}', 'yyyy-MM') + interval 1 month)
    {%- else %}
        false  -- không có tháng nào có dữ liệu mới: không ghi đè partition nào
    {%- endfor %}
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
