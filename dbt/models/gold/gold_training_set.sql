-- Gold: tập huấn luyện của mô hình phân loại ticket.
-- Grain: MỘT hàng cho MỘT ticket.
--
-- Đọc kỹ khối config() bên dưới trước khi đọc phần SELECT.

{{ config(
    materialized = 'incremental'
) }}

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    length(subject) + length(body)                            as text_len,
    case when status in ('resolved', 'closed') then 1 else 0 end as label_resolved,
    updated_at,
    _ingested_at
from {{ ref('silver_tickets') }}

{% if is_incremental() %}
-- Chỉ xử lý phân vùng của ngày vận hành hiện tại (cho phép backfill từng ngày).
where _ingested_at >= TIMESTAMP '{{ var("run_date") }} 00:00:00'
  and _ingested_at <  TIMESTAMP '{{ var("run_date") }} 00:00:00' + interval 1 day
{% endif %}
