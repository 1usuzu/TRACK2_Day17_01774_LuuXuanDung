# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Học viên AICB  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy make verify</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 22.4s
  run 2/3 … 29.7s
  run 3/3 … 30.2s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 140,308 (35.6×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt (Hoàn thành cả 2 bài mở rộng A & B)**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline hoặc bấm Clear Task trên Airflow, `gold_training_set` tăng số lượng dòng liên tục (từ 12,480 lên 13,790 ở run 1 và 38,750 ở run 3), Checksum thay đổi sau mỗi lần chạy, xuất hiện hàng nghìn ticket bị lặp. |
| **Nguyên nhân** | Bảng `gold_training_set` là bảng thực thể (Grain: 1 hàng / 1 `ticket_id`), nhưng model dbt incremental thiếu `unique_key`, khiến dbt mặc định dùng chiến lược `append` (INSERT INTO). Nguồn CDC có bản ghi cập nhật (`op = 'u'`). Khi chạy qua các ngày hoặc khi re-run, bản ghi mới của ticket bị INSERT thêm vào bảng đích thay vì UPDATE/MERGE, dẫn đến 1 ticket bị nhân bản nhiều dòng. Ngoài ra, Airflow DAG để `catchup=True` và thiếu `max_active_runs=1` gây nguy cơ backfill dồn dập và ghi tranh chấp đồng thời. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào khối `config()`.<br>- `dags/ai_training_pipeline.py`: Cập nhật `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (lượt 3) · sau: 12,480 hàng · checksum 3 lượt: `8dd7c98653` (giống nhau 100% qua cả 3 lượt chạy). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu khoảng 5% số hàng ở các ngày trong quá khứ đã chạy xong (thực tế chỉ có 8,645 / 9,100 hàng, thiếu 455 hàng). |
| **P99 độ trễ đo được** | **2.73 ngày** *(Max = 2.94 ngày; tỷ lệ bản ghi trễ > 1 ngày là 5.05%)* |
| **Lookback đã chọn** | **3 ngày** — vì P99 là 2.73 ngày và Max < 3 ngày, nên cửa sổ 3 ngày đủ bao trọn 100% các sự kiện đến muộn trong tập dữ liệu mà không tốn chi phí quét lại lịch sử quá dài. |
| **Nguyên nhân** | Điều kiện lọc incremental ban đầu `where event_date > (select max(event_date) from target)` chỉ lấy các event có ngày lớn hơn ngày lớn nhất đã có trong Gold. Một event xảy ra ngày D1 nhưng do độ trễ mạng đến kho ở ngày D2 (với D2 > D1) sẽ bị điều kiện so sánh `>` loại bỏ hoàn toàn vì tại ngày D2, `max(event_date)` trong Gold đã vượt qua D1. |
| **Cách khắc phục** | - `dbt/models/gold/gold_feature_daily.sql`: Nới lỏng điều kiện lọc lùi 3 ngày `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>- Thêm Composite Unique Key `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để khi tính lại ngày cũ có event muộn thì dbt ghi đè lên kết quả cũ thay vì cộng dồn số hàng. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (đạt chuẩn 14 ngày × 650 khách hàng) · checksum: `3db448685c`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Trong thực tế sản xuất, `max` có thể bị kéo dài bất thường bởi vài ngoại lệ cá biệt (ví dụ thiết bị mất kết nối 6 tháng mới sync lại log). Nếu chọn lookback theo `max`, mọi lần chạy pipeline hàng ngày đều phải quét và tính toán lại dữ liệu của hàng tháng trời, làm chi phí tính toán (compute cost) và thời gian chạy tăng vọt theo cấp số nhân mỗi ngày. Chọn P99 giúp cân bằng tối ưu: thu hồi 99% dữ liệu trễ với chi phí tính toán cố định và tối thiểu; 1% ngoại lệ cực đoan có thể được xử lý bằng batch reconciliation định kỳ hàng tháng.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Ngày 08-10 backend đổi cách ghi `priority` từ số sang chuỗi. Pipeline không báo lỗi, `dbt test` ban đầu vẫn pass, nhưng `silver_tickets.priority` có 6,606 dòng bị sai/NULL, bảng `quarantine_tickets` rỗng (0/312 hàng), model AI phân loại dự đoán kém hẳn. |
| **Nguyên nhân** | Code ban đầu dùng `try_cast(priority_raw as integer)`, vừa biến các nhãn chữ hợp lệ (`urgent`, `high`,...) thành NULL (gây mất dữ liệu nghiêm trọng sau ngày 08-10), vừa cho lọt các giá trị số rác (`0, 5, -1`) vì chúng là integer. Đồng thời bảng `silver_tickets` xếp hạng `row_number()` trước khi lọc bỏ bản ghi rác khiến các ticket có bản ghi mới nhất bị lỗi sẽ mất sạch toàn bộ ticket. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1. Số hợp lệ (`1, 2, 3, 4` - 6,846 dòng):** Ép kiểu `integer` và giữ nguyên.<br>**2. Nhãn chữ (`urgent, high, medium, low` - 7,142 dòng):** Mapping về số `1..4` tương ứng (`urgent`→1, `high`→2, `medium`→3, `low`→4).<br>**3. Dữ liệu lỗi (`0, 5, -1, P1, P2, unknown, '', NULL` - 312 dòng):** Trả về `NULL` để chuyển sang bảng `quarantine_tickets`. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Dùng khối `CASE` xử lý chuẩn hoá 3 nhóm trên.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi (`where priority_clean is not null`) **trước** khi đánh số thứ tự `row_number()`.<br>- `dbt/models/silver/quarantine_tickets.sql`: Đổi điều kiện thành `where {{ normalize_priority('priority_raw') }} is null`.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và bổ sung test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng 312/312) · `dbt test` 11/11 pass · `silver_tickets.priority` sạch 100% (không còn NULL và luôn ∈ 1..4). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn ở tầng Silver**: Bronze là kho lưu trữ dữ liệu thô nguyên vẹn (Raw Single Source of Truth). Nếu chặn/từ chối bản ghi ngay từ Bronze, ta sẽ mất dấu vết gốc và không thể audit hay debug khi nguồn gặp sự cố. Bronze nhận tất cả; Silver chịu trách nhiệm làm sạch và phân loại.
> 2. **Không để pipeline dừng khi gặp bản ghi lỗi**: Trong hơn 130,000 sự kiện và hàng chục nghìn ticket tốt, chỉ có 312 bản ghi lỗi (chiếm 0.2%). Nếu dừng pipeline, toàn bộ hệ thống phục vụ AI của doanh nghiệp sẽ bị tê liệt vì một lượng nhỏ dữ liệu rác. Cách ly bản ghi lỗi vào bảng Quarantine giúp bảo vệ tính liên tục của hệ thống, đồng thời tạo hàng đợi để kỹ sư điều tra xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A: Tối ưu Dashboard Query (Small-file problem & Partitioning)
| | |
|---|---|
| **Bài đã làm** | **Bài A & Bài B** |
| **Nguyên nhân** | Thư mục `data/gold_events/` có 5,000 file Parquet nhỏ (vài chục KB). DuckDB đọc Parquet theo lô và làm tròn lên ~1,000 hàng/file khiến công quét bị thổi phồng lên 5,000,000 rows scanned cho dataset chỉ có 130,683 dòng. Thêm vào đó, predicate `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` là non-sargable khiến engine không thể tận dụng metadata min/max để prune file. |
| **Cách khắc phục** | - `tools/compact.py`: Dùng `COPY ... TO` gom 5,000 file thành 14 file partition theo ngày (`partition_by (event_date)`), sắp xếp `ORDER BY customer_name, event_time` và đặt `ROW_GROUP_SIZE 10000`.<br>- `queries/dashboard.sql`: Đọc từ `data/gold_events_v2/*/*.parquet` với `hive_partitioning = 1` và sargable filter `event_date = '2026-08-09'`. |
| **Bằng chứng** | `rows scanned`: giảm từ 5,000,000 ➔ **140,308** (giảm **35.6×**, vượt xa yêu cầu ≥ 10×) · số file: 5,000 ➔ **14 file** · thời gian chạy: 76,526 ms ➔ **11.2 ms** · result hash: `4379e4c5d9f3` (không đổi). |

### Bài B: Consumer gặp sự cố giữa batch (Delivery semantics)
| | |
|---|---|
| **Nguyên nhân** | Ngữ nghĩa At-most-once ban đầu: Consumer commit offset trước khi ghi dữ liệu vào database. Khi tiến trình bị `kill -9` ở batch 7, offset đã tăng lên 3,500 nhưng dữ liệu batch 7 chưa vào DB, dẫn đến khi restart bị mất trắng 500 bản ghi. |
| **Cách khắc phục** | - `ingest/consumer.py`: Đổi sang ngữ nghĩa At-least-once bằng cách đảo thứ tự: ghi dữ liệu (`write_batch`) trước, commit offset sau.<br>- Thêm `PRIMARY KEY (event_id)` vào DDL bảng `bronze_events_stream`.<br>- Viết câu lệnh ghi Idempotent: `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` để khi replay batch 7, các bản ghi sẽ ghi đè cập nhật thay vì nhân đôi. |
| **Bằng chứng** | `make crash-test` vượt qua hoàn hảo: không mất bản ghi (✓), không trùng bản ghi (✓), C == A = 20,000 hàng (`BÀI MỞ RỘNG B: ĐẠT ✓`). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra Grain của bảng, xem dữ liệu nguồn có phát sinh update (CDC) hay không, và kiểm tra model incremental đã khai báo `unique_key` + `incremental_strategy = 'merge'` chưa, kết hợp cấu hình `catchup=False` và `max_active_runs=1` trên orchestrator. |
| 2 | Đo đạc phân bố độ trễ giữa event time và ingestion time (đặc biệt là chỉ số phân vị P99) để xác định Lookback window tối ưu, đồng thời khai báo Composite Unique Key khi nới window để tránh duplicate khi tính lại ngày cũ. |
| 3 | Kiểm tra Data Contract (schema enforcement) và dbt tests (accepted_values, not_null), đồng thời thiết lập cơ chế Dead-Letter Queue / Quarantine table để định tuyến dữ liệu lỗi mà không làm dừng pipeline của toàn hệ thống. |
