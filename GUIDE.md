# GUIDE — đi từng bước qua LAB 17

File này **không cho bạn đáp án**. Nó cho bạn *thứ tự thao tác*: đo cái gì,
đọc file nào, câu hỏi nào phải trả lời được trước khi gõ dòng sửa đầu tiên.

Pseudo-code cho từng nhiệm vụ nằm ở [PSEUDOCODE.md](PSEUDOCODE.md).

---

## 0 · Chuẩn bị (10 phút)

```bash
git clone https://github.com/VinUni-AI20k/Day17-Track2-DataPipeline.git
cd Day17-Track2-DataPipeline
make setup
```

`make setup` làm bốn việc: tạo `.venv`, cài `duckdb` + `dbt-duckdb`, sinh
14 ngày dữ liệu vào `seed/`, và ghi **mốc đo** cho nhiệm vụ 4 vào
`expected/dashboard_baseline.json`.

> ⚠️ Chỉ chạy `make setup` **một lần**. Nếu chạy lại sau khi đã sửa nhiệm vụ 4,
> mốc cũ vẫn được giữ (script không ghi đè), nhưng `data/gold_events/` sẽ bị
> sinh lại — bạn phải chạy lại `make compact`.

```bash
make pipeline    # ~40s
make verify      # ~3 phút — đây là bảng điểm của bạn
```

Mở một terminal thứ hai và để sẵn một shell DuckDB — bạn sẽ dùng nó liên tục:

```bash
.venv/bin/python -c "
import duckdb; con = duckdb.connect('warehouse.duckdb')
con.sql('show tables').show()
"
```

Hoặc tiện hơn, đặt hàm này vào shell:

```bash
q() { .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }

q "select count(*) from gold_training_set"
```

**Bảy bảng bạn sẽ đọc nhiều nhất**

| Bảng | Là gì |
|---|---|
| `bronze_tickets_cdc` | CDC thô, `priority_raw` là VARCHAR |
| `bronze_events` | sự kiện thô, có `event_time` và `_ingested_at` |
| `silver_tickets` | trạng thái mới nhất của mỗi ticket |
| `silver_events` | sự kiện đã khử trùng lặp, có `event_date` |
| `gold_training_set` | 1 hàng / 1 ticket |
| `gold_feature_daily` | 1 hàng / (ngày, khách hàng) |
| `gold_doc_chunks` | 1 hàng / 1 chunk — **nhóm đối chứng**, không có mìn |

---

## 1 · Nhiệm vụ 1 — bảng training phình lên *(30 phút)*

### 1.1 Tái hiện triệu chứng

```bash
make reset
make pipeline && q "select count(*) from gold_training_set"
make pipeline && q "select count(*) from gold_training_set"
```

Ghi lại hai con số. Chúng nói gì về **phép toán** mà lần chạy thứ hai thực hiện?

### 1.2 Khoanh vùng

```sql
-- Có ticket nào xuất hiện nhiều hơn một lần không?
select ticket_id, count(*) as n
from gold_training_set group by 1 having n > 1
order by n desc limit 10;

-- So sánh với nguồn của nó
select count(*), count(distinct ticket_id) from silver_tickets;
```

Nếu Silver có 1 hàng / 1 ticket mà Gold thì không, vấn đề nằm ở **cách Gold
được vật chất hoá**, không nằm ở dữ liệu.

### 1.3 Đọc ba dòng quyết định tất cả

Mở `dbt/models/gold/gold_training_set.sql`, đọc khối `config()` **trước** phần
`select`. Trong dbt, một model incremental được quyết định bởi ba tham số:

```
materialized          = 'incremental'
unique_key            = ?
incremental_strategy  = ?
```

Rồi đọc tiếp mệnh đề `{% if is_incremental() %}`. Trả lời:

1. Mỗi ngày vận hành, model chọn ra những hàng nào từ Silver?
2. Khi chạy lại **cùng một ngày** lần thứ hai, những hàng đó được **thêm vào**
   hay **thay thế** hàng cũ?
3. Với `dbt`, cái gì cho engine biết "hàng này đã có rồi, hãy ghi đè"?

### 1.4 Yếu tố thứ hai: CDC `op='u'`

```sql
select op, count(*) from bronze_tickets_cdc group by 1;
```

Có bản ghi `u`. Nghĩa là **một ticket có thể được ghi vào Gold ở ngày tạo, rồi
lại được ghi lần nữa ở ngày bị sửa** — ngay trong một lượt chạy duy nhất.
Đây là lý do cách sửa "chỉ xoá phân vùng ngày rồi ghi lại" **không đủ**.

Ba câu hỏi để chọn kỹ thuật:
- Grain của bảng này là gì? (thực thể hay sự kiện?)
- Khoá tự nhiên là gì?
- Với bảng thực thể có bản ghi bị sửa, bạn cần `append`, `delete+insert`,
  hay `merge`?

### 1.5 Yếu tố thứ ba: DAG

Mở `dags/ai_training_pipeline.py`. Phiếu #1041 nói người trực bấm **Clear Task**.
Hai tham số ở phần `TODO` quyết định:
- `catchup` — Airflow có tự chạy bù mọi ngày trong quá khứ không?
- `max_active_runs` — hai lần chạy có được phép ghi vào cùng bảng cùng lúc không?

Sửa cả hai. `make verify` đọc file này bằng AST và kiểm tra.

### ✅ Checkpoint 1

```bash
make verify
```
- `gold_training_set` → `ổn định ✓` và **12,480**
- dòng `gold_training_set: 1 hàng / 1 ticket` → ✓
- dòng `DAG: catchup / max_active_runs` → ✓

> Cạm bẫy: sửa xong nhiệm vụ 1, `gold_feature_daily` **vẫn** 8,645.
> Hai mìn khác nhau. Đừng nhầm.

---

## 2 · Nhiệm vụ 2 — thiếu hàng, không ai biết *(35 phút)*

### 2.1 Đo độ trễ, đừng đoán

```sql
select
    date_diff('hour', event_time, _ingested_at) as delay_h,
    count(*)
from bronze_events
group by 1 order by 1;
```

Bạn sẽ thấy phân bố **hai cụm**. Giờ lấy phân vị:

```sql
select
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay,
    max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
    avg(case when _ingested_at::date > event_time::date then 1.0 else 0 end)   as ty_le_ve_muon
from bronze_events;
```

**Ghi con số P99 vào báo cáo ngay bây giờ.** Nó là căn cứ cho lookback bạn chọn,
và rubric chấm chính con số đó.

### 2.2 Tìm hàng bị mất

```sql
-- kỳ vọng: 14 ngày × 650 khách = 9.100 tổ hợp
select count(*) from gold_feature_daily;

-- những tổ hợp (ngày, khách) có trong Silver mà thiếu trong Gold
select s.event_date, count(distinct s.customer_id) as thieu
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1;
```

Nhìn cột `event_date` của kết quả: **những ngày nào bị thiếu?** Ngày mới hay ngày cũ?

Rồi kiểm chứng giả thuyết:

```sql
-- với các tổ hợp bị thiếu, mọi event tới kho vào ngày nào?
select s.event_date, min(s.ingested_date), max(s.ingested_date), count(*)
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1 limit 5;
```

### 2.3 Đọc mệnh đề incremental

`dbt/models/gold/gold_feature_daily.sql`:

```sql
{% if is_incremental() %}
where event_date > (select max(event_date) from {{ this }})
{% endif %}
```

Đọc thành lời: *"chỉ tính những ngày sự kiện lớn hơn ngày lớn nhất đã có trong
bảng"*. Ba câu hỏi:

1. Một event `event_date = 08-12` nhưng `_ingested_at = 08-15` — hôm 08-15,
   `max(event_date)` trong bảng đang là bao nhiêu? Event đó có lọt qua `>` không?
2. Nếu đổi `>` thành `>=`, đã đủ chưa? (gợi ý: `>=` chỉ cứu được **một** ngày)
3. Lookback bao nhiêu ngày thì đủ? Căn cứ vào **P99** hay vào **max**?
   Chọn cái nào thì phải trả giá gì?

### 2.4 Cái bẫy đi kèm

Mở rộng cửa sổ nghĩa là **cùng một (ngày, khách) sẽ được tính lại nhiều lần**.
Nếu model chỉ biết `insert`, bạn vừa sửa được nhiệm vụ 2 thì lại tự tạo ra
đúng lỗi của nhiệm vụ 1 — nhưng lần này ở `gold_feature_daily`.

Grain ở đây là **hai cột**. `unique_key` của dbt nhận list.

### ✅ Checkpoint 2

```bash
make verify
```
- `gold_feature_daily` → `ổn định ✓` và **9,100**
- `gold_training_set` vẫn **12,480** và `ổn định ✓` (không được phá nhiệm vụ 1)

---

## 3 · Nhiệm vụ 3 — schema đổi giữa chừng *(30 phút)*

### 3.1 Nhìn thấy vết thương

```sql
select priority, count(*) from silver_tickets group by 1 order by 1 nulls last;
```

Có một cụm `NULL` lớn. Và — để ý kỹ — có cả `0`, `5`, `-1`.
`priority` được hợp đồng hoá là **1..4**.

Xem nguồn thô:

```sql
select priority_raw, count(*) from bronze_tickets_cdc group by 1 order by 2 desc;

-- mốc thời gian đổi kiểu
select event_time::date as ngay,
       count(*) filter (where try_cast(priority_raw as integer) is null) as khong_phai_so,
       count(*)                                                          as tong
from bronze_tickets_cdc group by 1 order by 1;
```

### 3.2 Phân loại — đây là phần khó nhất của nhiệm vụ 3

Bạn sẽ thấy `priority_raw` có **ba nhóm**, và chúng phải được đối xử khác nhau:

| Nhóm | Ví dụ | Đây là gì | Xử lý |
|---|---|---|---|
| số hợp lệ | `1` `2` `3` `4` | hợp đồng cũ | giữ nguyên |
| nhãn chuỗi | `urgent` `high` `medium` `low` | **schema tiến hoá** — nguồn đổi cách biểu diễn, ý nghĩa không đổi | **ánh xạ** về 1..4 |
| rác | `P1` `unknown` `0` `5` `-1` `''` `null` | dữ liệu thật sự bẩn | **cách ly** |

> Nhầm nhóm 2 thành nhóm 3 là lỗi hay gặp nhất. Nếu bạn quarantine hết mọi bản
> ghi từ 08-10 trở đi, `quarantine_tickets` sẽ có hàng nghìn hàng thay vì 312,
> và bạn vừa vứt đi một nửa dữ liệu vì backend đổi cách viết.
>
> Câu hỏi phân biệt: *"Giá trị này có mang đúng thông tin cũ, chỉ khác cách
> biểu diễn không?"* Có → ánh xạ. Không → cách ly.

Ánh xạ đúng (theo tài liệu API của team backend):
`urgent → 1`, `high → 2`, `medium → 3`, `low → 4`.

### 3.3 Ba thứ phải làm

**(a) Chuẩn hoá trong `silver_tickets.sql`** — thay `try_cast(...)` bằng một
biểu thức xử lý được cả số lẫn nhãn, và **loại** bản ghi rác ra khỏi Silver.

Lưu ý: một ticket rác thường **đã từng có bản ghi CDC hợp lệ trước đó**.
Cách ly *bản ghi* lỗi ≠ vứt bỏ *ticket*. Sau khi loại bản ghi rác, hãy lấy
bản ghi hợp lệ **mới nhất còn lại** của ticket đó — nếu không, số hàng
`silver_tickets` sẽ tụt xuống dưới 12.480 và nhiệm vụ 1 gãy theo.

**(b) Model mới `quarantine_tickets`** trong `dbt/models/silver/`.
Grain: **1 hàng / 1 bản ghi CDC bị loại** (không phải 1 hàng / 1 ticket).
Nên có: `ticket_id`, `cdc_seq`, `priority_raw`, `event_time`, và một cột
`reject_reason` để người trực đọc là hiểu.
Kỳ vọng: **312 hàng** (`expected/quarantine_tickets.count`).

**(c) Bật contract + thêm test** trong `dbt/models/silver/schema.yml`:

```yaml
config:
  contract:
    enforced: true      # từ false -> true
```

Contract của dbt ép **kiểu cột**, không ép **miền giá trị**. Miền giá trị là
việc của test. Thêm cho `priority`:

```yaml
- name: priority
  data_type: integer
  tests:
    - not_null
    - accepted_values:
        values: [1, 2, 3, 4]
        quote: false
```

> Với model `incremental` mà bật contract, dbt bắt buộc phải có
> `on_schema_change='fail'` (hoặc `'append_new_columns'`) trong `config()`.
> Nếu gặp lỗi này, đó là dbt đang nhắc bạn, không phải bug.

### 3.4 Câu hỏi thiết kế (phải trả lời trong báo cáo)

Chặn ở đâu — Bronze hay Silver? Và vì sao *không* để `dbt test` fail làm sập
DAG? Gợi ý để nghĩ: nếu 312 bản ghi rác làm dừng cả pipeline, thì 439.383 event
và 31.200 chunk hoàn toàn lành lặn cũng không tới được người dùng.

### ✅ Checkpoint 3

```bash
make verify
```
- `dbt test` → ✓, và số test **nhiều hơn 9** (bạn đã thêm test mới)
- `silver_tickets.priority ∈ 1..4, không NULL` → ✓
- `quarantine_tickets` → **312**, `ổn định ✓`
- `gold_training_set` vẫn **12,480**

---

## 4 · Nhiệm vụ 4 — dashboard chậm *(25 phút)*

### 4.1 Đo trước, đừng sửa trước

```bash
make explain
make plan        # in cả cây EXPLAIN ANALYZE
ls data/gold_events | wc -l
du -sh data/gold_events
```

Chép ba con số vào báo cáo: `rows scanned`, `files`, `rows on disk`.

> **Vì sao `rows scanned` (5.000.000) lớn hơn `rows on disk` (439.383)?**
> DuckDB đọc Parquet theo lô và làm tròn **lên** cho từng file. Một file 88 hàng
> vẫn tốn công như ~1.000 hàng. Đó chính là *small-file problem* hiện nguyên
> hình thành một con số — và cũng là lý do nhiệm vụ này chấm theo `rows scanned`
> chứ không theo thời gian (thời gian phụ thuộc máy và cache).

### 4.2 Đối chiếu truy vấn với bố cục

Mở `queries/dashboard.sql`. Hai câu hỏi:

1. Truy vấn lọc theo cột nào? (có **hai** điều kiện lọc)
2. Trong `data/gold_events/`, tên file có mang thông tin của cột nào không?

Nếu đường dẫn không mang thông tin lọc, engine **buộc phải mở mọi file** rồi
mới biết file nào có ích. Đó là toàn bộ vấn đề.

Và một chi tiết nữa trong `WHERE`:

```sql
where strftime(event_time, '%Y-%m-%d') = '2026-08-09'
```

Điều kiện này bọc cột trong một hàm. Engine không thể so thẳng giá trị đó với
thống kê min/max của file, cũng không thể so với tên thư mục phân vùng.
Viết lại sao cho **cột đứng một mình một vế**.

### 4.3 Viết `tools/compact.py`

Khung có sẵn trong file, pseudo-code trong [PSEUDOCODE.md](PSEUDOCODE.md).
Ba quyết định:

- **`partition_by`** — cột nào? (cột mà dashboard lọc theo, và có ít giá trị:
  14 ngày → 14 thư mục)
- **`order by`** — sắp theo cột nào để các hàng cùng khách hàng nằm cạnh nhau?
  (giúp thống kê min/max của row group có ích thay vì phủ toàn bộ dữ liệu)
- **`row_group_size`** — mặc định 122.880 hàng. Với ~31.000 hàng/ngày thì cả
  ngày là **một** row group, thống kê min/max vô dụng. Chọn nhỏ hơn.

> Trên bộ dữ liệu nhỏ này, gần như toàn bộ phần thắng đo được đến từ
> **phân vùng**; `order by` và `row_group_size` chỉ bắt đầu có giá trị khi một
> ngày có hàng chục triệu hàng. Trong báo cáo, hãy nói đúng thứ nào tạo ra
> con số — đó cũng là một phần của "đo trước khi tối ưu".

Sau đó sửa `queries/dashboard.sql` trỏ vào bãi mới, bật `hive_partitioning=true`,
và viết lại `WHERE` cho đúng cột phân vùng.

```bash
make compact
make explain
```

### ✅ Checkpoint 4

- `rows scanned` giảm ≥ **10×** (làm đúng thì thường được **100×+**)
- `files` giảm từ 5.000 xuống hàng chục
- `result hash` **không đổi** — nếu đổi, bạn đã sửa cả ngữ nghĩa truy vấn

---

## 5 · Nhiệm vụ 5 *(mở rộng)* — consumer và sự cố giữa lô

```bash
make crash-test
```

Đọc kỹ output. Bạn **mất** hay bạn **trùng**?

### 5.1 Đọc `ingest/consumer.py`

```python
consumer.commit()                 # (1)
maybe_crash(batch_no, crash_at)   # (2)  <- kill -9 ở đây
write_batch(con, batch)           # (3)
```

Trả lời:
- Nếu tiến trình chết ở (2), lô này đã được ghi chưa? Offset đã nhảy chưa?
  Khi khởi động lại, consumer đọc từ đâu?
- Nếu đổi thứ tự thành (3) → (2) → (1), lần khởi động lại sẽ đọc lại lô đó.
  Chuyện gì xảy ra với `insert` thường?

Đây chính là **at-most-once** và **at-least-once** trên slide.
Bạn không chọn được "exactly-once" ở tầng giao vận — bạn chọn **at-least-once
cộng với một phép ghi idempotent**.

### 5.2 Phép ghi idempotent

DuckDB hỗ trợ `insert ... on conflict (...) do update set ...`, nhưng chỉ khi
cột khoá có ràng buộc `primary key` / `unique`. Xem hằng `DDL` ở đầu file.

### ✅ Checkpoint 5

```bash
make crash-test     # NHIỆM VỤ 5: ĐẠT ✓
make verify         # bốn nhiệm vụ trước vẫn xanh
```

---

## 6 · Viết báo cáo (15 phút)

Dùng [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md). Mỗi nhiệm vụ **ba dòng**:

```
Triệu chứng  : cái đội vận hành nhìn thấy
Nguyên nhân  : cơ chế khiến nó xảy ra — một câu, cụ thể
Cách sửa     : bạn đổi cái gì, ở file nào
Bằng chứng   : số trước / số sau
```

10 điểm cuối chấm dòng **Nguyên nhân**. "Tôi thêm `unique_key`" là *cách sửa*,
không phải nguyên nhân. Nguyên nhân là: *"model incremental không có `unique_key`
nên dbt sinh ra `INSERT`; chạy lại cùng một ngày sẽ nối thêm chứ không ghi đè"*.

---

## Phụ lục — khi bí

| Triệu chứng | Nhìn vào đâu |
|---|---|
| `dbt run` báo `Invalid value for on_schema_change` | model incremental có contract → thêm `on_schema_change='fail'` |
| `Can't open a connection to same database file` | có tiến trình khác đang mở `warehouse.duckdb` — đóng shell DuckDB kia |
| `make verify` treo/lỗi lạ sau khi sửa nhiều | `make clean && make pipeline` |
| số hàng đúng nhưng `ổn định ✗` | bạn đang `insert` thay vì `merge`/`delete+insert` |
| `ổn định ✓` nhưng số hàng sai | cửa sổ lọc của bạn bỏ sót dữ liệu — xem nhiệm vụ 2 |
| `quarantine_tickets` ra hàng nghìn | bạn đang cách ly cả nhãn chuỗi hợp lệ — xem mục 3.2 |
| `silver_tickets` tụt dưới 12.480 | bạn loại cả **ticket** thay vì chỉ loại **bản ghi CDC** lỗi |
| `result hash` đổi sau khi tối ưu | truy vấn mới không còn tương đương — so lại `WHERE` |
| lỡ chạy `make seed` sau khi compact | chạy lại `make compact` |
