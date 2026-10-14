select
    user_session,
    min(user_id)                                                        as user_id,
    min(event_date)                                                     as session_date,
    min(event_time)                                                     as started_at,
    max(event_time)                                                     as ended_at,
    timestampdiff(SECOND, min(event_time), max(event_time))             as duration_seconds,
    count(*)                                                            as event_count,
    count_if(event_type = 'view')                                       as view_count,
    count_if(event_type = 'cart')                                       as cart_count,
    count_if(event_type = 'remove_from_cart')                           as remove_count,
    count_if(event_type = 'purchase')                                   as purchase_item_count,
    coalesce(sum(case when event_type = 'purchase' then price end), 0)  as revenue,
    count_if(event_type = 'view') > 0                                   as has_view,
    count_if(event_type = 'cart') > 0                                   as has_cart,
    count_if(event_type = 'purchase') > 0                               as has_purchase
from {{ ref('events') }}
where user_session is not null
group by user_session
