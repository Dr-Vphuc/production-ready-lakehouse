select
    md5(concat_ws('|', user_session, cast(event_time as string)))  as order_id,
    user_session,
    min(user_id)                                                   as user_id,
    event_time                                                     as ordered_at,
    to_date(event_time)                                            as order_date,
    count(*)                                                       as item_count,
    coalesce(sum(price), 0)                                        as revenue,
    count_if(price is null)                                        as unpriced_item_count
from {{ ref('events') }}
where event_type = 'purchase'
group by user_session, event_time
