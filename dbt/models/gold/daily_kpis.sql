with events_daily as (

    select
        event_date,
        count_if(event_type = 'view')              as view_count,
        count_if(event_type = 'cart')              as cart_count,
        count_if(event_type = 'remove_from_cart')  as remove_count,
        count(distinct user_id)                    as active_users
    from {{ ref('events') }}
    group by event_date

),

sessions_daily as (

    select
        session_date,
        count(*)                as sessions,
        count_if(has_cart)      as cart_sessions,
        count_if(has_purchase)  as buying_sessions
    from {{ ref('fct_sessions') }}
    group by session_date

),

orders_daily as (

    select
        order_date,
        count(*)                 as orders,
        count(distinct user_id)  as buyers,
        sum(item_count)          as items_sold,
        sum(revenue)             as revenue
    from {{ ref('fct_orders') }}
    group by order_date

)

select
    e.event_date,
    e.view_count,
    e.cart_count,
    e.remove_count,
    e.active_users,
    s.sessions,
    s.cart_sessions,
    s.buying_sessions,
    round(s.buying_sessions / s.sessions, 4)  as session_conversion_rate,
    coalesce(o.orders, 0)                     as orders,
    coalesce(o.buyers, 0)                     as buyers,
    coalesce(o.items_sold, 0)                 as items_sold,
    coalesce(o.revenue, 0)                    as revenue,
    round(o.revenue / o.orders, 2)            as avg_order_value
from events_daily e
left join sessions_daily s on s.session_date = e.event_date
left join orders_daily   o on o.order_date   = e.event_date