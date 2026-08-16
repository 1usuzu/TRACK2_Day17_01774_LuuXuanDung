# HƯỚNG DẪN THỰC HIỆN — LAB 17

Tài liệu này không cung cấp lời giải. Nó xác định **trình tự thao tác** cho
từng nhiệm vụ: cần đo đại lượng nào, đọc file nào, và phải trả lời được câu
hỏi nào trước khi thay đổi code.

Khung mã giả của từng nhiệm vụ nằm ngay trong file cần sửa, dưới dạng chú
thích `KHUNG THỰC HIỆN`:

| Nhiệm vụ | File chứa khung mã giả |
|---|---|
| 1 | `dbt/models/gold/gold_training_set.sql` |
| 2 | `dbt/models/gold/gold_feature_daily.sql` |
| 3 | `dbt/models/silver/silver_tickets.sql`, `dbt/models/silver/quarantine_tickets.sql` |
| 4 | `tools/compact.py` |
| 5 | `ingest/consumer.py` |

---

## 0. Chuẩn bị môi trường

```bash
git clone https://github.com/VinUni-AI20k/Day17-Track2-DataPipeline.git
cd Day17-Track2-DataPipeline
make setup
```

`make setup` thực hiện bốn việc: tạo `.venv`, cài `duckdb` và `dbt-duckdb`,
sinh 14 ngày dữ liệu vào `seed/`, và ghi **mốc đo hiện trạng** cho nhiệm vụ 4
vào `expected/dashboard_baseline.json`.

> Chỉ chạy `make setup` một lần. Mốc đo đã ghi sẽ không bị ghi đè ở các lần
> chạy sau, nhưng `data/gold_events/` sẽ được sinh lại — nếu bạn đã hoàn thành
> nhiệm vụ 4 thì cần chạy lại `make compact`.

```bash
make pipeline    # chạy đường ống một lượt
make verify      # chạy ba lượt và in bảng đánh giá
```

Trong quá trình làm, `make quick` (một lượt) đủ để kiểm tra nhanh; `make verify`
(ba lượt) dùng khi cần xác nhận tính ổn định.

### Công cụ truy vấn

Mở một terminal thứ hai và định nghĩa hàm sau để truy vấn kho dữ liệu:

```bash
q() { .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }

q "select count(*) from gold_training_set"
```

### Các bảng sẽ sử dụng

| Bảng | Nội dung |
|---|---|
| `bronze_tickets_cdc` | CDC thô; `priority_raw` kiểu VARCHAR |
| `bronze_events` | Sự kiện thô; có `event_time` và `_ingested_at` |
| `silver_tickets` | Trạng thái mới nhất của mỗi ticket |
| `silver_events` | Sự kiện đã khử trùng lặp; có `event_date` |
| `gold_training_set` | 1 hàng / 1 ticket |
| `gold_feature_daily` | 1 hàng / (ngày, khách hàng) |
| `gold_doc_chunks` | 1 hàng / 1 chunk — **nhóm đối chứng**, không chứa lỗi |

---

## 1. Nhiệm vụ 1 — Kích thước bảng `gold_training_set` tăng sau mỗi lần chạy

### 1.1 Tái hiện hiện tượng

```bash
make reset
make pipeline && q "select count(*) from gold_training_set"
make pipeline && q "select count(*) from gold_training_set"
```

Ghi lại hai giá trị. Quan hệ giữa chúng cho biết lượt chạy thứ hai đã thực
hiện phép ghi nào lên bảng đích.

### 1.2 Khoanh vùng phạm vi lỗi

```sql
-- Bảng đích có ticket nào xuất hiện nhiều hơn một lần không?
select ticket_id, count(*) as n
from gold_training_set
group by 1 having n > 1
order by n desc limit 10;

-- Đối chiếu với bảng nguồn
select count(*) as tong_hang, count(distinct ticket_id) as so_ticket
from silver_tickets;
```

Nếu bảng nguồn giữ đúng 1 hàng / 1 ticket còn bảng đích thì không, nguyên nhân
nằm ở **cách bảng đích được vật chất hoá**, không nằm ở dữ liệu đầu vào.

### 1.3 Rà soát cấu hình vật chất hoá

Mở `dbt/models/gold/gold_training_set.sql` và đọc khối `KHUNG THỰC HIỆN` ở đầu
file, sau đó là khối `config()`. Một model incremental của dbt được xác định
bởi ba tham số; hiện chỉ có một tham số được khai báo.

Cần trả lời:

1. Khi không khai báo `unique_key`, dbt sinh ra câu lệnh ghi nào?
2. Với câu lệnh đó, chạy lại cùng một ngày lần thứ hai thì hàng cũ bị thay thế
   hay bị ghi thêm?

### 1.4 Đặc điểm nguồn CDC

```sql
select op, count(*) from bronze_tickets_cdc group by 1 order by 1;
```

Nguồn có bản ghi `op = 'u'`. Nghĩa là một ticket được tạo ngày D1 và bị sửa
ngày D2 sẽ đi qua mệnh đề lọc theo `run_date` **hai lần trong cùng một lượt
chạy** — hai lần đó rơi vào hai phân vùng ngày khác nhau.

Hệ quả: phương án "xoá phân vùng của ngày rồi ghi lại" không xử lý được trường
hợp này. Ba câu hỏi để chọn chiến lược:

- Grain của bảng là thực thể hay sự kiện?
- Khoá tự nhiên của grain đó là gì?
- Cần `append`, `delete+insert` theo phân vùng ngày, hay `merge` theo khoá?

### 1.5 Tham số điều phối

Mở `dags/ai_training_pipeline.py`. Phiếu #1041 ghi nhận thao tác **Clear Task**
trên Airflow. Hai tham số ở phần `TODO` quyết định hệ quả của thao tác đó:

- `catchup` — Airflow có tự xếp lịch chạy bù mọi ngày trong quá khứ không?
- `max_active_runs` — nhiều lượt chạy có được phép ghi đồng thời vào cùng một
  bảng không?

Đặt lại cả hai. `make verify` đọc file này bằng AST và kiểm tra giá trị.

Lưu ý khi viết báo cáo: hai tham số này **giảm tần suất kích hoạt** lỗi, chúng
không phải nguyên nhân gốc. Sửa DAG mà không sửa model thì `make verify` vẫn
báo lỗi.

### 1.6 Tiêu chí hoàn thành

```bash
make verify
```

- `gold_training_set`: cột `ỔN ĐỊNH` là ✓ và số hàng bằng **12.480**
- Dòng `gold_training_set: 1 hàng / 1 ticket`: ✓
- Dòng `DAG: catchup / max_active_runs`: ✓

Sau khi hoàn thành nhiệm vụ 1, `gold_feature_daily` **vẫn** ở 8.645 hàng. Đó là
một lỗi độc lập, thuộc nhiệm vụ 2.

---

## 2. Nhiệm vụ 2 — `gold_feature_daily` thiếu hàng ở các ngày trong quá khứ

### 2.1 Đo phân bố độ trễ dữ liệu

```sql
select
    date_diff('hour', event_time, _ingested_at) as do_tre_gio,
    count(*)
from bronze_events
group by 1 order by 1;
```

Phân bố có hai cụm tách biệt. Lượng hoá bằng các phân vị:

```sql
select
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay,
    max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
    avg(case when _ingested_at::date > event_time::date then 1.0 else 0 end)   as ty_le_ve_muon
from bronze_events;
```

**Ghi giá trị P99 vào báo cáo ở bước này.** Đó là căn cứ định lượng cho tham số
lookback, và là một trong hai con số bắt buộc của rubric.

### 2.2 Xác định tập hàng bị thiếu

```sql
-- Kỳ vọng: 14 ngày × 650 khách hàng = 9.100 tổ hợp
select count(*) from gold_feature_daily;

-- Các tổ hợp (ngày, khách hàng) có trong Silver nhưng không có trong Gold
select s.event_date, count(distinct s.customer_id) as so_to_hop_thieu
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1;
```

Quan sát cột `event_date` của kết quả: các tổ hợp bị thiếu tập trung ở ngày mới
hay ngày cũ?

Kiểm chứng giả thuyết bằng thời điểm dữ liệu tới kho:

```sql
select s.event_date,
       min(s.ingested_date) as toi_kho_som_nhat,
       max(s.ingested_date) as toi_kho_muon_nhat,
       count(*)             as so_event
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1 limit 5;
```

### 2.3 Phân tích điều kiện lọc incremental

Mở `dbt/models/gold/gold_feature_daily.sql`:

```sql
where event_date > (select max(event_date) from {{ this }})
```

Diễn giải: *chỉ xử lý những ngày sự kiện lớn hơn ngày sự kiện lớn nhất đã có
trong bảng đích*. Cần trả lời:

1. Một bản ghi có `event_date = 08-12` và `_ingested_at = 08-15`: tại thời
   điểm chạy ngày 08-15, `max(event_date)` trong bảng đích bằng bao nhiêu?
   Bản ghi đó có thoả điều kiện không? Ngày 08-16 thì sao?
2. Đổi `>` thành `>=` đã đủ chưa? Toán tử đó mở rộng cửa sổ thêm mấy ngày?
3. Cửa sổ cần lùi bao nhiêu ngày? Căn cứ vào **P99** hay vào **max**? Mỗi ngày
   lùi thêm phát sinh chi phí gì, và chi phí đó trả một lần hay trả ở mọi lượt
   chạy về sau?

### 2.4 Ràng buộc đi kèm khi mở rộng cửa sổ

Cửa sổ rộng hơn đồng nghĩa cùng một tổ hợp `(event_date, customer_id)` được
tính lại ở nhiều lượt chạy. Nếu model chỉ thực hiện `insert`, kết quả các lần
tính sẽ cộng dồn — tức là tái tạo đúng lỗi của nhiệm vụ 1 trên một bảng khác.

Grain ở đây gồm **hai cột**. Tham số `unique_key` của dbt nhận vào một danh sách.

### 2.5 Tiêu chí hoàn thành

```bash
make verify
```

- `gold_feature_daily`: `ỔN ĐỊNH` ✓ và số hàng bằng **9.100**
- `gold_training_set` giữ nguyên **12.480** và `ỔN ĐỊNH` ✓

---

## 3. Nhiệm vụ 3 — Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

### 3.1 Xác định bất thường trong phân bố giá trị

```sql
select priority, count(*)
from silver_tickets
group by 1 order by 1 nulls last;
```

Có hai bất thường cần ghi nhận: một tỷ lệ `NULL` lớn, và sự xuất hiện của các
giá trị `0`, `5`, `-1` — trong khi hợp đồng dữ liệu quy định `priority ∈ 1..4`.

Đối chiếu với dữ liệu nguồn và xác định mốc thời gian:

```sql
select priority_raw, count(*)
from bronze_tickets_cdc
group by 1 order by 2 desc;

select event_time::date as ngay,
       count(*) filter (where try_cast(priority_raw as integer) is null) as khong_phai_so,
       count(*)                                                          as tong
from bronze_tickets_cdc
group by 1 order by 1;
```

### 3.2 Phân loại giá trị nguồn

`priority_raw` gồm ba nhóm giá trị, và ba nhóm này phải được xử lý khác nhau:

| Nhóm | Ví dụ | Bản chất | Cách xử lý |
|---|---|---|---|
| Số hợp lệ | `1` `2` `3` `4` | Đúng hợp đồng ban đầu | Giữ nguyên |
| Nhãn chuỗi | `urgent` `high` `medium` `low` | **Schema tiến hoá**: nguồn đổi cách biểu diễn, ý nghĩa không đổi | **Ánh xạ** về miền 1..4 |
| Giá trị không hợp lệ | `P1` `unknown` `0` `5` `-1` `''` `null` | Dữ liệu lỗi | **Cách ly** |

Tiêu chí phân biệt nhóm 2 và nhóm 3: *giá trị này có mang đúng thông tin của
hợp đồng cũ, chỉ khác cách biểu diễn hay không?* Nếu có thì ánh xạ, nếu không
thì cách ly.

Ánh xạ theo tài liệu API của team backend: `urgent → 1`, `high → 2`,
`medium → 3`, `low → 4`.

> Xử lý nhóm 2 như nhóm 3 là sai sót phổ biến nhất ở nhiệm vụ này. Nếu cách ly
> toàn bộ bản ghi từ 08-10 trở đi, `quarantine_tickets` sẽ có hàng nghìn hàng
> thay vì số hàng kỳ vọng, đồng thời loại bỏ một lượng lớn dữ liệu hợp lệ chỉ
> vì nguồn đổi định dạng.

### 3.3 Các hạng mục cần thực hiện

**(a) Chuẩn hoá giá trị trong `silver_tickets.sql`.** Thay `try_cast(...)` bằng
biểu thức xử lý được cả số lẫn nhãn chuỗi, và loại bản ghi thuộc nhóm 3 ra khỏi
Silver.

Thứ tự hai bước quyết định số hàng của bảng: **lọc bỏ bản ghi lỗi trước, xếp
hạng lấy bản ghi mới nhất sau**. Làm ngược lại sẽ khiến ticket có bản ghi mới
nhất bị lỗi biến mất khỏi Silver, kéo theo `gold_training_set` thiếu hàng.
Đối tượng bị cách ly là **bản ghi CDC**, không phải **ticket**.

**(b) Bổ sung model `quarantine_tickets`.** File khung đã có sẵn tại
`dbt/models/silver/quarantine_tickets.sql` với đầy đủ yêu cầu và câu hỏi thiết
kế. Grain: 1 hàng / 1 bản ghi CDC bị loại. Số hàng kỳ vọng nằm trong
`expected/quarantine_tickets.count`.

Biểu thức chuẩn hoá ở (a) và điều kiện lọc ở (b) phải dùng **chung một định
nghĩa** — đặt trong một macro tại `dbt/macros/` hoặc một CTE dùng lại. Nếu hai
model định nghĩa riêng, chúng sẽ lệch nhau ngay khi một bên được sửa.

**(c) Bật contract và bổ sung test** trong `dbt/models/silver/schema.yml`:

```yaml
config:
  contract:
    enforced: true      # từ false chuyển thành true
```

Contract ràng buộc **kiểu dữ liệu**; miền giá trị thuộc phạm vi của test. Cần
cả hai — contract một mình vẫn cho `priority = 99` đi qua:

```yaml
- name: priority
  data_type: integer
  tests:
    - not_null
    - accepted_values:
        values: [1, 2, 3, 4]
        quote: false
```

> Với model `incremental` có bật contract, dbt yêu cầu khai báo
> `on_schema_change` là `'fail'` hoặc `'append_new_columns'`. Thông báo lỗi
> tương ứng là yêu cầu cấu hình, không phải lỗi hệ thống.

### 3.4 Câu hỏi thiết kế

Hai câu hỏi này cần được trả lời trong báo cáo:

1. Nên chặn dữ liệu lỗi ở tầng Bronze hay tầng Silver? Nếu Bronze từ chối bản
   ghi lỗi thì việc điều tra sự cố về sau gặp trở ngại gì?
2. Vì sao không để `dbt test` thất bại và dừng toàn bộ DAG khi gặp bản ghi lỗi?
   Cân nhắc quy mô: số bản ghi lỗi so với tổng số bản ghi hợp lệ đang chờ được
   phục vụ.

### 3.5 Tiêu chí hoàn thành

```bash
make verify
```

- `dbt test`: ✓, và tổng số test **lớn hơn 9** (bản gốc có 9 test)
- `silver_tickets.priority ∈ 1..4, không NULL`: ✓
- `quarantine_tickets`: đúng số hàng kỳ vọng và `ỔN ĐỊNH` ✓
- `gold_training_set` giữ nguyên **12.480**

---

## 4. Nhiệm vụ 4 — Hiệu năng truy vấn dashboard suy giảm

### 4.1 Đo lường các chỉ số hiện trạng

```bash
make explain            # rows scanned, rows on disk, files, result hash
make plan               # in thêm cây EXPLAIN ANALYZE
ls data/gold_events | wc -l
du -sh data/gold_events
```

Ghi lại ba chỉ số vào báo cáo: `rows scanned`, `files`, `rows on disk`.

> **Về chênh lệch giữa `rows scanned` và `rows on disk`.** DuckDB đọc Parquet
> theo lô và làm tròn lên theo từng file: một file 88 hàng vẫn phát sinh khối
> lượng đọc tương đương khoảng 1.000 hàng. Chênh lệch này chính là định lượng
> của *small-file problem*.
>
> Nhiệm vụ được đánh giá theo `rows scanned` chứ không theo thời gian, vì thời
> gian phụ thuộc cấu hình máy và trạng thái cache của hệ điều hành.

### 4.2 Đối chiếu điều kiện lọc với bố cục lưu trữ

Mở `queries/dashboard.sql` và xác định:

1. Truy vấn lọc theo những cột nào? (có hai điều kiện lọc)
2. Tên file trong `data/gold_events/` có mang thông tin của cột nào không?

Nếu đường dẫn không mang thông tin lọc, engine buộc phải mở toàn bộ file rồi
mới xác định được file nào chứa dữ liệu cần thiết.

Ngoài ra, xem xét dạng của điều kiện lọc:

```sql
where strftime(event_time, '%Y-%m-%d') = '2026-08-09'
```

Điều kiện này bọc cột trong một lời gọi hàm. Engine không thể đối chiếu kết quả
của hàm với tên thư mục phân vùng, cũng không đối chiếu được với thống kê
min/max của row group. Cần viết lại sao cho **cột nằm độc lập ở một vế**.

### 4.3 Tái cấu trúc bố cục dữ liệu

Hiện thực `tools/compact.py` — khung `COPY ... TO ...` cùng ba quyết định cần
lý giải đã có trong docstring của file. Sau đó cập nhật `queries/dashboard.sql`
để trỏ vào bãi mới, bật `hive_partitioning`, và viết lại điều kiện lọc.

```bash
make compact
make explain
```

### 4.4 Tiêu chí hoàn thành

- `rows scanned` giảm tối thiểu **10 lần** so với mốc
- `files` giảm từ 5.000 xuống hàng chục
- `result hash` **không đổi** — nếu thay đổi, ngữ nghĩa truy vấn đã bị sửa và
  toàn bộ hạng mục này không được tính điểm

---

## 5. Nhiệm vụ 5 *(mở rộng)* — Ngữ nghĩa phân phối của consumer

### 5.1 Tái hiện sự cố

```bash
make crash-test
```

Kịch bản: chạy hết một lượt để lấy số hàng chuẩn, chạy lại và giết tiến trình ở
giữa một lô ghi, sau đó khởi động lại và so sánh. Đọc kết quả để xác định
consumer đang **mất** bản ghi hay tạo ra bản ghi **trùng**.

### 5.2 Phân tích thứ tự thao tác

Mở `ingest/consumer.py`, đọc khối `KHUNG THỰC HIỆN` ở đầu file và khối được
đánh dấu trong hàm `consume()`:

```python
consumer.commit()                 # ghi nhận offset
maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
write_batch(con, batch)           # ghi dữ liệu
```

Cần trả lời:

- Nếu tiến trình dừng tại `maybe_crash()`, lô hiện tại đã được ghi chưa? Offset
  đã dịch chưa? Lần khởi động lại sẽ đọc từ vị trí nào?
- Nếu đảo thứ tự thành ghi trước, commit sau, lần khởi động lại sẽ đọc lại lô
  đó. Với câu lệnh `INSERT` hiện tại, hệ quả là gì?

Đây là hai ngữ nghĩa **at-most-once** và **at-least-once** trong bài giảng.
Exactly-once không tồn tại ở tầng giao vận; điều có thể lựa chọn là
at-least-once kết hợp với một phép ghi idempotent.

### 5.3 Tính idempotent của phép ghi

DuckDB hỗ trợ `insert ... on conflict (...) do update set ...`, nhưng chỉ khi
cột khoá có ràng buộc `primary key` hoặc `unique`. Xem hằng `DDL` ở đầu file.

Câu hỏi cho báo cáo: `DO UPDATE` và `DO NOTHING` khác nhau ở điểm nào khi một
message được phát lại với nội dung đã thay đổi?

### 5.4 Tiêu chí hoàn thành

```bash
make crash-test     # NHIỆM VỤ 5: ĐẠT
make verify         # bốn nhiệm vụ trước không bị ảnh hưởng
```

---

## 6. Viết báo cáo

Sử dụng [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md). Mỗi nhiệm vụ trình bày theo
bốn mục:

```
Triệu chứng   : hiện tượng quan sát được từ phía vận hành
Nguyên nhân   : cơ chế gây ra hiện tượng đó — một câu, cụ thể
Cách khắc phục: thay đổi gì, ở file nào
Bằng chứng    : số liệu trước và sau
```

Mục **Nguyên nhân** chiếm toàn bộ 10 điểm cuối. "Bổ sung tham số `unique_key`"
là *cách khắc phục*, không phải nguyên nhân. Nguyên nhân là phát biểu về cơ
chế: *model incremental không khai báo khoá, nên dbt sinh ra câu lệnh `INSERT`;
chạy lại cùng một phân vùng sẽ ghi thêm thay vì ghi đè.*

---

## Phụ lục A — Xử lý sự cố thường gặp

| Hiện tượng | Hướng xử lý |
|---|---|
| `dbt run` báo `Invalid value for on_schema_change` | Model incremental có bật contract; bổ sung `on_schema_change='fail'` |
| `Can't open a connection to same database file` | Có tiến trình khác đang mở `warehouse.duckdb`; đóng shell DuckDB đang chạy |
| `make verify` báo lỗi bất thường sau nhiều lần sửa | `make clean && make pipeline` |
| Số hàng đúng nhưng `ỔN ĐỊNH` ✗ | Model đang `insert` thay vì `merge` / `delete+insert` |
| `ỔN ĐỊNH` ✓ nhưng số hàng thiếu | Điều kiện lọc bỏ sót dữ liệu — xem nhiệm vụ 2 |
| `quarantine_tickets` có hàng nghìn hàng | Đang cách ly cả nhãn chuỗi hợp lệ — xem mục 3.2 |
| `silver_tickets` dưới 12.480 hàng | Đang loại cả ticket thay vì chỉ loại bản ghi CDC lỗi — xem mục 3.3(a) |
| `result hash` thay đổi sau khi tối ưu | Truy vấn mới không còn tương đương — rà soát lại mệnh đề `WHERE` |
| Đã chạy `make seed` sau khi hoàn thành nhiệm vụ 4 | Chạy lại `make compact` |
