-- Gold: đặc trưng theo ngày cho agent định tuyến.
-- Grain: MỘT hàng cho MỘT cặp (event_date, customer_id).
--
-- Mỗi ngày vận hành, model chỉ tính phần "mới". Câu hỏi là: "mới" nghĩa là gì?

{{ config(
    materialized = 'incremental'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
where event_date > (select max(event_date) from {{ this }})
{% endif %}

group by 1, 2, 3, 4
