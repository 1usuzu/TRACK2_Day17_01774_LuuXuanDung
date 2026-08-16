-- Silver: trạng thái MỚI NHẤT của mỗi ticket, dựng lại từ luồng CDC.
--
-- Quy tắc CDC đã cài sẵn và đang đúng:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất;
--   * ticket có op = 'd' bị loại khỏi Silver.
--
-- Phần còn lại của model thì… bạn tự đọc.

{{ config(materialized = 'table') }}

with ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,

    -- Nguồn thỉnh thoảng gửi giá trị không phải số. try_cast trả về NULL
    -- thay vì làm job đổ vỡ, nên pipeline vẫn xanh.
    try_cast(priority_raw as integer)                        as priority,

    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
