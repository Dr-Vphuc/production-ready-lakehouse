-- Tỷ lệ phải nằm trong [0, 1], tử số không vượt mẫu số, doanh thu không âm.
-- Ngày không có phiên nào bắt đầu thì sessions là NULL, các phép so sánh ở đây bỏ qua ngày đó.
select
    event_date,
    sessions,
    cart_sessions,
    buying_sessions,
    session_conversion_rate,
    revenue
from {{ ref('daily_kpis') }}
where session_conversion_rate < 0
   or session_conversion_rate > 1
   or buying_sessions > sessions
   or cart_sessions > sessions
   or revenue < 0
