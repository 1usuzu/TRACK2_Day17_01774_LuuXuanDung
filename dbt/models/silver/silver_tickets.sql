-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- Phần xử lý CDC dưới đây đã đúng và không cần sửa:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất;
--   * ticket có op = 'd' bị loại khỏi Silver.
--
-- KHUNG THỰC HIỆN — NHIỆM VỤ 3
--
--   Cột `priority` được hợp đồng hoá là số nguyên trong miền 1..4. Hãy đối
--   chiếu phân bố giá trị ở Bronze với phân bố ở Silver trước khi sửa:
--
--       SELECT priority_raw, count(*) FROM bronze_tickets_cdc GROUP BY 1;
--       SELECT priority,     count(*) FROM silver_tickets      GROUP BY 1;
--
--   Giá trị nguồn chia thành ba nhóm, và ba nhóm này KHÔNG được xử lý giống
--   nhau. Xác định ba nhóm đó, rồi thiết kế biểu thức chuẩn hoá:
--
--       normalize_priority(raw) := CASE
--           WHEN <nhóm 1: đã đúng hợp đồng>   THEN <giữ nguyên>
--           WHEN <nhóm 2: đổi cách biểu diễn> THEN <ánh xạ về miền hợp lệ>
--           ELSE NULL   -- NULL = "không hợp lệ", tín hiệu để cách ly
--       END
--
--   Thứ tự hai bước dưới đây quyết định số hàng của bảng:
--       (1) loại bỏ bản ghi CDC không chuẩn hoá được
--       (2) SAU ĐÓ mới xếp hạng để lấy bản ghi mới nhất của mỗi ticket
--   Làm ngược lại thì ticket có bản ghi mới nhất bị lỗi sẽ biến mất khỏi
--   Silver, kéo theo gold_training_set thiếu hàng. Cách ly BẢN GHI, không
--   cách ly TICKET.
--
--   Biểu thức chuẩn hoá nên viết MỘT LẦN dùng chung (macro trong dbt/macros/
--   hoặc một CTE), vì quarantine_tickets phải dùng đúng định nghĩa đó. Hai
--   model tự định nghĩa riêng thì sớm muộn cũng lệch nhau.
--
--   Sau khi sửa, bật contract và bổ sung test trong schema.yml.
-- ---------------------------------------------------------------------------

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
    -- thay vì làm job đổ vỡ, nên pipeline vẫn báo xanh.
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
