-- Mỗi sự kiện thuộc đúng một trong bốn loại, nên số đếm theo loại cộng lại phải bằng event_count
select
    user_session,
    event_count,
    view_count + cart_count + remove_count + purchase_item_count as counted_events
from {{ ref('fct_sessions') }}
where event_count <> view_count + cart_count + remove_count + purchase_item_count
