-- Một đơn có ít nhất một món, doanh thu không âm (giá âm đã thành NULL ở silver)
-- và số món chưa có giá không thể vượt số món của đơn
select
    order_id,
    item_count,
    unpriced_item_count,
    revenue
from {{ ref('fct_orders') }}
where item_count < 1
   or revenue < 0
   or unpriced_item_count < 0
   or unpriced_item_count > item_count
