# PSEUDOCODE — khung giải cho từng nhiệm vụ

Đây là **khung**, không phải đáp án chép được. Mỗi `?` là một quyết định bạn
phải tự trả lời, và bạn phải giải thích được lựa chọn đó trong báo cáo.

Ký hiệu: `<...>` = chỗ bạn điền · `# ?` = câu hỏi phải trả lời trước khi điền.

---

## Nhiệm vụ 1 — `gold_training_set` idempotent

### Ý tưởng

```
Bảng thực thể (1 hàng / 1 ticket) + nguồn có bản ghi bị SỬA
  => phép ghi phải là "ghi đè theo khoá", không phải "nối thêm".
```

### `dbt/models/gold/gold_training_set.sql`

```jinja
{{ config(
    materialized         = 'incremental',
    unique_key           = <khoá tự nhiên của grain>,     # ? grain là gì
    incremental_strategy = <'merge' | 'delete+insert'>,   # ? nguồn có bản ghi sửa không
    on_schema_change     = 'fail'
) }}

select <các cột như cũ>
from {{ ref('silver_tickets') }}

{% if is_incremental() %}
where <giữ nguyên bộ lọc theo run_date>
{% endif %}
```

> Vì sao vẫn cần bộ lọc `run_date`? Vì nó là thứ cho phép backfill **một ngày**
> mà không phải quét lại 14 ngày. `unique_key` lo tính đúng, bộ lọc lo tính rẻ.
> Hai việc khác nhau.

### `dags/ai_training_pipeline.py`

```python
with DAG(
    ...
    catchup=<?>,           # ? DAG có nên tự chạy bù mọi ngày quá khứ không
    max_active_runs=<?>,   # ? hai lần chạy có được ghi vào cùng bảng cùng lúc không
) as dag:
```

### Kiểm tra

```
make verify
  gold_training_set: ổn định ✓, 12,480, "1 hàng / 1 ticket" ✓
```

---

## Nhiệm vụ 2 — lookback cho dữ liệu về muộn

### Ý tưởng

```
Dữ liệu về muộn = nhãn của QUÁ KHỨ có thể đổi sau khi ngày đó đã "chạy xong".
  => cửa sổ tính lại phải lùi về quá khứ đủ xa,
  => và vì tính lại nên phép ghi phải là ghi đè theo khoá.
Hai vế. Thiếu một vế là hỏng.
```

### Bước 1 — đo, rồi mới chọn hằng số

```sql
SELECT quantile_cont(delay_days, 0.99)  -- P99
FROM   (SELECT date_diff('second', event_time, _ingested_at)/86400.0 AS delay_days
        FROM bronze_events);
```

```
LOOKBACK_DAYS = ceil(P99)      # ? vì sao dùng P99 chứ không dùng max
                               # ? nếu dùng max thì trả giá gì
```

### Bước 2 — `dbt/models/gold/gold_feature_daily.sql`

```jinja
{{ config(
    materialized         = 'incremental',
    unique_key           = [<cột 1>, <cột 2>],   # ? grain có mấy cột
    incremental_strategy = 'delete+insert',
    on_schema_change     = 'fail'
) }}

select <các cột tổng hợp như cũ>
from {{ ref('silver_events') }}

{% if is_incremental() %}
where event_date >= (
        select coalesce(max(event_date), DATE '<ngày đầu>') from {{ this }}
      ) - interval <LOOKBACK_DAYS> day          # ? vì sao là >= chứ không phải >
{% endif %}

group by <...>
```

> Mẹo: đặt `LOOKBACK_DAYS` thành `var()` trong `dbt_project.yml` thay vì viết
> cứng. Sau này đổi hằng số không phải sửa SQL — và trong báo cáo bạn chỉ vào
> đúng một dòng để nói "đây là con số tôi đo được".

### Bẫy

- Chỉ mở cửa sổ, không đặt `unique_key` → mỗi lượt chạy cộng thêm 3 ngày dữ liệu.
  `verify` sẽ báo `ổn định ✗`.
- Chỉ đặt `unique_key`, không mở cửa sổ → vẫn thiếu 455 hàng.

---

## Nhiệm vụ 3 — contract + quarantine

### Ý tưởng

```
Ba nhóm giá trị, ba cách xử lý:
   số 1..4          -> giữ
   nhãn chuỗi       -> ÁNH XẠ (schema tiến hoá, ý nghĩa không đổi)
   phần còn lại     -> CÁCH LY (dữ liệu bẩn thật)
Cách ly BẢN GHI, không cách ly TICKET.
```

### Bước 1 — hàm chuẩn hoá dùng chung

```sql
-- gợi ý: viết một lần dưới dạng macro dbt hoặc một CTE, dùng ở cả hai model
normalized_priority(raw) =
    CASE
        WHEN try_cast(raw AS integer) BETWEEN 1 AND 4 THEN try_cast(raw AS integer)
        WHEN lower(trim(raw)) = 'urgent'  THEN 1
        WHEN lower(trim(raw)) = <...>     THEN <...>
        ELSE NULL          -- NULL ở đây nghĩa là "không hợp lệ" -> cách ly
    END
```

### Bước 2 — `silver_tickets.sql`

```
cdc      = bronze_tickets_cdc
valid    = cdc  WHERE normalized_priority(priority_raw) IS NOT NULL
latest   = với mỗi ticket_id, lấy bản ghi có (event_time, cdc_seq) lớn nhất
                              TRONG TẬP `valid`          # ? vì sao phải là trong valid
silver   = latest WHERE op <> 'd'
```

> Điểm mấu chốt: xếp hạng **sau khi** đã bỏ bản ghi lỗi. Nếu bạn xếp hạng trước
> rồi mới lọc, ticket có bản ghi mới nhất bị lỗi sẽ **biến mất** khỏi Silver —
> và `gold_training_set` tụt xuống 12.168.

### Bước 3 — model mới `dbt/models/silver/quarantine_tickets.sql`

```
quarantine = cdc WHERE normalized_priority(priority_raw) IS NULL

cột nên có: ticket_id, cdc_seq, event_time, priority_raw,
            reject_reason (ví dụ 'priority không ánh xạ được về 1..4'),
            _quarantined_at
grain     : 1 hàng / 1 BẢN GHI CDC bị loại        -> kỳ vọng 312 hàng
```

### Bước 4 — `dbt/models/silver/schema.yml`

```yaml
- name: silver_tickets
  config:
    contract:
      enforced: true             # <- đổi từ false
  columns:
    - name: priority
      data_type: integer
      tests:
        - not_null
        - accepted_values: { values: [1, 2, 3, 4], quote: false }
```

> Contract ép **kiểu**. Test ép **miền giá trị**. Cần cả hai:
> contract một mình vẫn cho `priority = 99` đi qua.

---

## Nhiệm vụ 4 — partition + compaction + viết lại truy vấn

### Ý tưởng

```
Engine chỉ bỏ qua được thứ nó BIẾT là vô ích trước khi mở file.
Nó biết qua hai kênh:  (a) tên thư mục phân vùng
                       (b) thống kê min/max của row group
Cả hai kênh đều bị vô hiệu nếu WHERE bọc cột trong một hàm.
```

### Bước 1 — `tools/compact.py`

```python
COPY (
    SELECT *
    FROM   read_parquet('data/gold_events/*.parquet')
    ORDER  BY <cột phân vùng>, <cột lọc thứ hai>   # ? cột nào giúp min/max có ích
) TO 'data/gold_events_v2' (
    FORMAT          parquet,
    PARTITION_BY    (<cột mà dashboard lọc theo>), # ? bao nhiêu giá trị -> bao nhiêu thư mục
    OVERWRITE_OR_IGNORE,
    ROW_GROUP_SIZE  <?>                            # ? mặc định 122880 có hợp với 31k hàng/ngày không
)
```

### Bước 2 — `queries/dashboard.sql`

```sql
FROM read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
WHERE <cột phân vùng> = <giá trị>       -- cột đứng MỘT MÌNH một vế, không bọc hàm
  AND customer_name  = 'ACME'
```

### Bước 3 — đo lại

```bash
make compact && make explain
```

```
rows scanned : 5.000.000 -> <?>      cần ≤ 500.000
files        : 5.000     -> <?>
result hash  : phải GIỮ NGUYÊN
```

> Cạm bẫy hay gặp: thêm index thay vì đổi bố cục. Parquet trên đĩa không có
> index. Thứ duy nhất bạn điều khiển được là **file nằm ở đâu** và
> **hàng nằm theo thứ tự nào trong file**.

---

## Nhiệm vụ 5 — delivery semantics

### Ý tưởng

```
at-most-once   : commit offset TRƯỚC khi ghi  -> crash = MẤT dữ liệu
at-least-once  : commit offset SAU khi ghi    -> crash = TRÙNG dữ liệu
exactly-once   : at-least-once + phép ghi IDEMPOTENT
```

### `ingest/consumer.py`

```python
# vòng lặp
batch = consumer.poll(batch_size)

write_batch(con, batch)            # (3) ghi TRƯỚC
maybe_crash(batch_no, crash_at)    # (2) sự cố xảy ra ở đây
consumer.commit()                  # (1) commit SAU
```

```python
# DDL: thêm ràng buộc để ON CONFLICT có chỗ bám
create table if not exists bronze_events_stream (
    event_id varchar <?>,          # ? ràng buộc nào cho phép ON CONFLICT
    ...
);

# write_batch
INSERT INTO bronze_events_stream VALUES (...)
ON CONFLICT (<?>) DO UPDATE SET <?>;   # ? cập nhật cột nào — hay DO NOTHING là đủ?
```

> Câu hỏi cho báo cáo: `DO UPDATE` và `DO NOTHING` khác nhau ở đâu khi message
> **được phát lại với nội dung đã đổi**? Trong bài này chọn cái nào cũng qua
> được test — nhưng bạn phải biết vì sao mình chọn.

### Kiểm tra

```bash
make crash-test     # NHIỆM VỤ 5: ĐẠT ✓
```
