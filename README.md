# LAB 17 — Data Pipeline Engineering

**AICB-P2T2 · Ngày 17 · Chương 4: Hạ Tầng**
Thời lượng: 2,5 giờ · Trọng số: nằm trong 30% điểm Lab hằng ngày

> Ba file bạn cần đọc:
> **README.md** (bạn đang ở đây — bối cảnh & nhiệm vụ) ·
> [**GUIDE.md**](GUIDE.md) (hướng dẫn từng bước) ·
> [**RUBRIC.md**](RUBRIC.md) (thang điểm 100)

---

## 1. Bối cảnh

Bạn vừa nhận bàn giao đường ống dữ liệu của **nền tảng AI hỗ trợ khách hàng** —
chính hệ thống trong bài giảng hôm nay.

```
Postgres tickets (CDC)  ─┐
S3 transcripts (JSON)   ─┼─→  Bronze  ─→  Silver  ─┬─→  gold_doc_chunks    →  RAG index
Kafka events + feedback ─┘                          ├─→  gold_training_set  →  Classifier
                                                    └─→  gold_feature_daily →  Routing agent
```

Pipeline **chạy được, không báo lỗi, `dbt test` pass**.
Nhưng đội vận hành đã gửi bốn phiếu sự cố.

Việc của bạn không phải dựng lại hệ thống — mà là **chẩn đoán và sửa**,
đúng như một data engineer nhận ca trực.

---

## 2. Sau lab, bạn phải làm được

- Viết transform **idempotent** — chạy lại N lần cho cùng một kết quả
- Xử lý **dữ liệu về muộn** bằng lookback window
- Dùng **data contract** để chặn schema đổi, và **định tuyến bản ghi lỗi**
  thay vì làm sập pipeline
- Đọc **`EXPLAIN ANALYZE`** và chứng minh tối ưu bằng số, không bằng cảm giác

## 3. Điều kiện cần

- **Python 3.11+** và `make`. Hết. Không cần Docker, không cần cloud.
- Biết SQL cơ bản. **Không** cần kinh nghiệm dbt trước đó.
- Đã xem slide Ngày 17 (đặc biệt phần Transform và Engine).

> **Ghi chú về công nghệ.** Kho dữ liệu ở đây là **DuckDB**, transform bằng
> **dbt**. Postgres/S3/Kafka được thay bằng ba file seed và một commit log
> ghi trên đĩa (`ingest/log_client.py`). Mọi khái niệm — CDC, offset, commit,
> partition, contract — giữ nguyên; chỉ có hạ tầng là nhẹ đi để bạn dành 2,5
> giờ cho việc *chẩn đoán* thay vì cho việc *cài đặt*.

---

## 4. Cài đặt (10 phút)

```bash
git clone https://github.com/VinUni-AI20k/Day17-Track2-DataPipeline.git
cd Day17-Track2-DataPipeline

make setup      # venv + thư viện + sinh 14 ngày dữ liệu + ghi mốc đo
make pipeline   # chạy toàn bộ đường ống một lượt (~40 giây)
make verify     # ⭐ chạy 3 lượt liên tiếp, in bảng chấm (~3 phút)
```

`make verify` là **vòng phản hồi của bạn suốt buổi**. Chạy nó sau mỗi lần sửa.
Nếu thấy chậm, dùng `make quick` (1 lượt) khi đang thử, và `make verify` khi
muốn kiểm tra thật.

Kết quả lúc mới bắt đầu — **bốn dòng ✗ là bốn nhiệm vụ**:

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    —                      —         312   chưa có bảng

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ chưa có bảng (kỳ vọng 312)
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×)
  DAG: catchup / max_active_runs              ✗ True / None

  TỔNG KẾT   1/5 tiêu chí đạt
```

Hai cột đầu **nói hai chuyện khác nhau**:
`ỔN ĐỊNH` = chạy lại có ra kết quả cũ không · `SỐ HÀNG` = kết quả đó có đúng không.
Một bảng có thể **ổn định mà vẫn sai** (xem `gold_feature_daily`).

### Cấu trúc repo

```
├─ seed/generate.py            # sinh 14 ngày dữ liệu — KHÔNG cần đụng
├─ seed/tickets_cdc.jsonl      # CDC từ Postgres: op = c / u / d
├─ seed/events.jsonl           # topic `ai-events`
├─ seed/transcripts.jsonl      # file JSON trên S3
├─ ingest/load_bronze.py       # Bronze loader (đã idempotent, không có mìn)
├─ ingest/log_client.py        # Kafka-lite: topic + offset trên đĩa
├─ ingest/consumer.py          #                                  ← nhiệm vụ 5
├─ dbt/models/silver/          # silver_tickets.sql, schema.yml   ← nhiệm vụ 3
├─ dbt/models/gold/            # 3 bảng Gold                      ← nhiệm vụ 1, 2
├─ queries/dashboard.sql       # truy vấn của đội CSKH            ← nhiệm vụ 4
├─ tools/compact.py            # khung trống, bạn viết            ← nhiệm vụ 4
├─ data/gold_events/           # bãi Parquet 5.000 file           ← nhiệm vụ 4
├─ dags/ai_training_pipeline.py# DAG Airflow (đọc, không chạy)    ← nhiệm vụ 1
├─ expected/                   # số hàng đúng — dùng để tự chấm
├─ tools/verify.py             # make verify
└─ Makefile
```

---

## 5. Nhiệm vụ

> Mỗi nhiệm vụ cho bạn **triệu chứng**, không cho nguyên nhân.
> Hãy điều tra trước khi sửa. Chi tiết từng bước: [GUIDE.md](GUIDE.md).

### Nhiệm vụ 1 — Bảng training phình lên sau mỗi lần chạy *(30 phút)*

> **Phiếu sự cố #1041.** "Đêm qua job lỗi mạng, mình vào Airflow bấm Clear Task
> cho chạy lại. Sáng nay `gold_training_set` nhiều hơn hẳn. Chạy lại lần nữa
> lại nhiều thêm. Không thấy lỗi gì cả."

**Cần thu thập trước khi sửa**
- Chạy `make pipeline` hai lần, đếm hàng sau mỗi lần
- Mở `dbt/models/gold/gold_training_set.sql` — đọc khối `config()`
- Trong bốn kỹ thuật idempotent ở slide, kỹ thuật nào hợp với bảng *thực thể*
  có bản ghi bị sửa (`op='u'`)?
- Mở `dags/ai_training_pipeline.py` — hai tham số nào khiến "Clear Task" thành
  chuyện nguy hiểm?

**Tiêu chí đạt**
- `make verify` báo `ổn định ✓` cho `gold_training_set`
- Số hàng đúng bằng `expected/gold_training_set.count`
- Chạy lần thứ tư, thứ năm vẫn không đổi

---

### Nhiệm vụ 2 — Thiếu hàng, không ai biết *(35 phút)*

> **Phiếu sự cố #1043.** "`gold_feature_daily` thiếu khoảng 5% so với đối chiếu
> thủ công. Kỳ lạ là chỉ thiếu ở những ngày *đã chạy xong từ lâu*, ngày mới thì đủ."

**Cần thu thập trước khi sửa**
- Trong Bronze, so sánh `_ingested_at` với `event_time`. Phân bố của hiệu số đó
  ra sao? **P99 là bao nhiêu?**
- Đọc mệnh đề `{% if is_incremental() %}` trong `gold_feature_daily.sql`.
  Nó lọc từ mốc nào?
- Một bản ghi *xảy ra* ngày 08-12 nhưng *tới kho* ngày 08-15 thì rơi vào đâu?

**Tiêu chí đạt**
- Số hàng khớp `expected/gold_feature_daily.count`
- Trong báo cáo: nêu rõ **con số P99 bạn đo được** và lookback bạn chọn dựa trên nó
- Vẫn `ổn định ✓` — sửa nhiệm vụ 2 không được phá nhiệm vụ 1

---

### Nhiệm vụ 3 — Schema đổi giữa chừng *(30 phút)*

> **Phiếu sự cố #1047.** "Team backend đổi kiểu cột `priority` từ số sang chuỗi
> hôm 08-10, có báo trên Slack. Pipeline không hề dừng. Nhưng model phân loại
> từ hôm đó dự đoán kém hẳn."

**Cần thu thập trước khi sửa**
- `select priority, count(*) from silver_tickets group by 1` — bạn thấy gì?
- So sánh với `select priority_raw, count(*) from bronze_tickets_cdc group by 1`
- Mở `dbt/models/silver/schema.yml`. `contract` đang ở trạng thái nào?
- **Câu hỏi thiết kế:** dữ liệu sai kiểu nên làm sập cả DAG, hay nên bị tách
  riêng để pipeline chạy tiếp? Và có phải mọi giá trị "lạ" đều là dữ liệu bẩn?

**Tiêu chí đạt**
- `contract` được bật, `dbt test` pass
- Bảng `quarantine_tickets` chứa **đúng** những bản ghi sai kiểu
  (số hàng khớp `expected/quarantine_tickets.count`)
- `silver_tickets.priority` không còn NULL và luôn nằm trong 1..4
- Pipeline **không** dừng khi gặp bản ghi lỗi — nó tách ra và chạy tiếp

---

### Nhiệm vụ 4 — Truy vấn dashboard chậm gấp 20 lần *(25 phút)*

> **Phiếu sự cố #1052.** "Dashboard của đội CSKH mất 38 giây mới load.
> Ba tháng trước chỉ 2 giây. Không ai sửa dòng code nào."

**Cần thu thập trước khi sửa**
- `make explain` — ghi lại **`rows scanned`**, đừng ghi thời gian
- `ls data/gold_events/ | wc -l` — bao nhiêu file? Kích thước trung bình?
- Truy vấn đang lọc theo cột nào? Bãi dữ liệu đang phân vùng theo cột nào?
- `make plan` để xem cây `EXPLAIN ANALYZE`

**Tiêu chí đạt**
- `rows scanned` giảm **ít nhất 10 lần**, có số liệu `make explain` trước và sau
- Số file giảm rõ rệt (compaction)
- Kết quả truy vấn **không đổi** (`make explain` so hash kết quả giúp bạn)

---

### Nhiệm vụ 5 — *(mở rộng, cho ai xong sớm)* Consumer và sự cố giữa lô

> `make crash-test` giết tiến trình consumer ở giữa một lô ghi rồi khởi động
> lại. Đếm số hàng. Bạn mất bản ghi, hay bạn có bản ghi trùng?

**Cần điều tra:** trong `ingest/consumer.py`, offset được commit **trước** hay
**sau** khi ghi thành công? Đổi thứ tự thì được gì, mất gì? Đổi thứ tự **một
mình nó** đã đủ chưa?

**Tiêu chí đạt:** `make crash-test` báo `NHIỆM VỤ 5: ĐẠT ✓`.

---

## 6. Nộp bài

1. **Repo** đã sửa (link Git hoặc file nén — nhớ `make clean` trước khi nén)
2. **Kết quả `make verify`** — dán nguyên output ba lần chạy
3. **Báo cáo một trang** — dùng [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md).
   Với mỗi nhiệm vụ:
   - Triệu chứng → **nguyên nhân gốc** → cách sửa
   - Nhiệm vụ 2 phải có con số **P99** bạn đo
   - Nhiệm vụ 4 phải có **`rows scanned`** trước/sau

## 7. Cách chấm

Thang 100 điểm, chi tiết trong [RUBRIC.md](RUBRIC.md). Tóm tắt:

| Tiêu chí | Điểm |
|---|---|
| Chạy ba lần ra checksum giống hệt nhau | 30 |
| Số hàng ba bảng Gold khớp `expected/` | 30 |
| `dbt test` pass + quarantine đúng bản ghi | 15 |
| `rows scanned` giảm ≥ 10× có bằng chứng | 15 |
| Báo cáo nêu đúng **nguyên nhân gốc**, không chỉ mô tả cách sửa | 10 |
| *(thưởng)* Nhiệm vụ 5 đạt | +5 |

> Sửa đúng nhưng không giải thích được vì sao thì mất 10 điểm cuối.
> Ở nơi làm việc, phần giải thích mới là thứ ngăn lỗi lặp lại.

## 8. Bài viết về nhà *(nộp cùng Lab 18)*

Nửa trang, dùng bản đồ công nghệ ở slide 5:

> Nếu hệ thống này lớn gấp 50 lần — 20 triệu ticket/ngày, sáu team cùng dùng —
> bạn sẽ đổi lựa chọn nào? **DuckDB hay Spark? Airflow hay Dagster?
> Kafka hay WarpStream?** Với mỗi lựa chọn, nêu **ràng buộc** khiến bạn đổi,
> không nêu tính năng.
