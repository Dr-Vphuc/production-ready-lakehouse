-- Cộng các ngày của daily_kpis phải ra đúng số phiên, số phiên có mua, số đơn và số món của các bảng fact
-- (revenue đã được kiểm ở assert_daily_kpis_revenue_reconciles). <=> so sánh được cả khi giá trị là NULL.
with kpis as (

    select
        sum(sessions)        as sessions,
        sum(buying_sessions) as buying_sessions,
        sum(orders)          as orders,
        sum(items_sold)      as items_sold
    from {{ ref('daily_kpis') }}

),

facts as (

    select
        (select count(*) from {{ ref('fct_sessions') }})                as sessions,
        (select count_if(has_purchase) from {{ ref('fct_sessions') }})  as buying_sessions,
        (select count(*) from {{ ref('fct_orders') }})                  as orders,
        (select sum(item_count) from {{ ref('fct_orders') }})           as items_sold

)

select
    k.sessions        as kpi_sessions,        f.sessions        as fact_sessions,
    k.buying_sessions as kpi_buying_sessions, f.buying_sessions as fact_buying_sessions,
    k.orders          as kpi_orders,          f.orders          as fact_orders,
    k.items_sold      as kpi_items_sold,      f.items_sold      as fact_items_sold
from kpis k
cross join facts f
where not (k.sessions <=> f.sessions)
   or not (k.buying_sessions <=> f.buying_sessions)
   or not (k.orders <=> f.orders)
   or not (k.items_sold <=> f.items_sold)
