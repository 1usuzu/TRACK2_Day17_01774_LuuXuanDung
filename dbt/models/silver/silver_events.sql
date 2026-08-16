-- Silver: sự kiện runtime đã khử trùng lặp theo event_id.
-- Model này không có mìn — nhưng hai cột `event_time` và `_ingested_at`
-- ở đây là chỗ bạn sẽ cần cho nhiệm vụ 2.

{{ config(materialized = 'table') }}

select
    event_id,
    ticket_id,
    customer_id,
    customer_name,
    segment,
    event_type,
    model,
    latency_ms,
    tokens_in,
    tokens_out,
    is_escalated,
    event_time,
    _ingested_at,
    event_time::date                                          as event_date,
    _ingested_at::date                                        as ingested_date
from {{ source('bronze', 'bronze_events') }}
qualify row_number() over (partition by event_id order by _ingested_at) = 1
