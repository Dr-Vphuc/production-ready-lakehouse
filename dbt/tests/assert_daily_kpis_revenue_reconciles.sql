select
    k.total_revenue as kpi_revenue,
    o.total_revenue as order_revenue
from (select sum(revenue) as total_revenue from {{ ref('daily_kpis') }}) k
cross join (select sum(revenue) as total_revenue from {{ ref('fct_orders') }}) o
where k.total_revenue <> o.total_revenue
