-- Hai bảng fact cùng tính từ các sự kiện purchase, nên tổng số món và tổng doanh thu phải bằng nhau.
-- fct_sessions bỏ các sự kiện thiếu user_session, nên chỉ so với các đơn có user_session.
select
    s.purchase_items as session_items,
    o.items          as order_items,
    s.revenue        as session_revenue,
    o.revenue        as order_revenue
from (
    select sum(purchase_item_count) as purchase_items, sum(revenue) as revenue
    from {{ ref('fct_sessions') }}
) s
cross join (
    select sum(item_count) as items, sum(revenue) as revenue
    from {{ ref('fct_orders') }}
    where user_session is not null
) o
where not (s.purchase_items <=> o.items)
   or not (s.revenue <=> o.revenue)
