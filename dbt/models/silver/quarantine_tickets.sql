-- ---------------------------------------------------------------------------
-- quarantine_tickets — NHIỆM VỤ 3.  TRẠNG THÁI: KHUNG RỖNG, CHƯA CÓ LOGIC.
-- ---------------------------------------------------------------------------
-- Mục đích: tiếp nhận các bản ghi CDC không thoả data contract, để pipeline
-- tiếp tục chạy thay vì dừng, và để người trực có một hàng đợi cần xử lý.
--
-- Yêu cầu:
--   * Grain: 1 hàng / 1 BẢN GHI CDC bị loại — không phải 1 hàng / 1 ticket.
--     Một ticket có thể có nhiều bản ghi CDC, trong đó chỉ một bản ghi lỗi.
--   * Số hàng kỳ vọng: xem expected/quarantine_tickets.count
--   * Cột tối thiểu: ticket_id, cdc_seq, event_time, priority_raw, reject_reason
--
-- KHUNG THỰC HIỆN
--   quarantine := SELECT <các cột trên>
--                 FROM   {{ source('bronze', 'bronze_tickets_cdc') }}
--                 WHERE  <priority_raw KHÔNG chuẩn hoá được về miền hợp lệ>
--
--   reject_reason := CASE
--                        WHEN <giá trị rỗng>        THEN '...'
--                        WHEN <là số, ngoài miền>   THEN '...'
--                        ELSE                            '...'
--                    END
--
-- Câu hỏi cần trả lời trước khi viết:
--   1. Điều kiện "không chuẩn hoá được" nên đặt ở đâu để model này và
--      silver_tickets dùng CHUNG một định nghĩa? Nếu hai model tự định nghĩa
--      riêng, chuyện gì xảy ra khi một trong hai được sửa?
--   2. reject_reason nên phân biệt bao nhiêu loại lỗi thì đủ để người trực
--      biết phải làm gì tiếp?
--   3. Nếu một ticket có bản ghi mới nhất bị loại, trạng thái nào của ticket
--      đó nên xuất hiện trong silver_tickets?
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

-- Khung rỗng: đúng cấu trúc cột, không có hàng nào.
-- Thay toàn bộ khối SELECT dưới đây bằng truy vấn thật của bạn.
select
    cast(null as varchar)   as ticket_id,
    cast(null as integer)   as cdc_seq,
    cast(null as timestamp) as event_time,
    cast(null as varchar)   as priority_raw,
    cast(null as varchar)   as reject_reason
where false
