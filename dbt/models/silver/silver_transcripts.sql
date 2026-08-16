-- Silver: hội thoại đã khử trùng lặp. Model này không có mìn.

{{ config(materialized = 'table') }}

select
    transcript_id,
    ticket_id,
    lang,
    turns,
    event_time,
    _ingested_at
from {{ source('bronze', 'bronze_transcripts') }}
qualify row_number() over (partition by transcript_id order by _ingested_at) = 1
