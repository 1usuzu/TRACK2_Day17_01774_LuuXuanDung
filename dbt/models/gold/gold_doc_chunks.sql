-- Gold: chunk văn bản để nạp vào chỉ mục RAG.
-- Grain: MỘT hàng cho MỘT chunk.
--
-- Model này materialized = 'table' (dựng lại toàn bộ mỗi lần chạy).
-- Nó KHÔNG có mìn — hãy dùng nó làm nhóm đối chứng: chạy ba lần vẫn y hệt.

{{ config(materialized = 'table') }}

with exploded as (

    select
        transcript_id,
        ticket_id,
        lang,
        event_time,
        unnest(turns)                                         as chunk_text,
        unnest(range(len(turns)))                             as chunk_index
    from {{ ref('silver_transcripts') }}

)

select
    transcript_id || '#' || chunk_index::varchar              as chunk_id,
    transcript_id,
    ticket_id,
    lang,
    chunk_index,
    chunk_text,
    length(chunk_text)                                        as n_chars,
    event_time
from exploded
