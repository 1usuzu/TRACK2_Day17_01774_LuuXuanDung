# RUBRIC — LAB 17 · thang 100 điểm

Phần lớn điểm được chấm **tự động** bằng `make verify`, `make explain` và
`make crash-test`. Phần còn lại chấm bằng mắt trên báo cáo.

```bash
make verify        # A + B + C  (và phần lớn của D)
make explain       # D
make crash-test    # thưởng
```

---

## Tổng quan

| Mục | Nội dung | Điểm |
|---|---|---|
| **A** | Tính ổn định — chạy ba lần ra checksum giống hệt nhau | **30** |
| **B** | Tính đúng — số hàng ba bảng Gold khớp `expected/` | **30** |
| **C** | Chất lượng dữ liệu — contract, test, quarantine | **15** |
| **D** | Hiệu năng — `rows scanned` giảm ≥ 10× có bằng chứng | **15** |
| **E** | Báo cáo — nêu đúng **nguyên nhân gốc** | **10** |
| **+** | *(thưởng)* Nhiệm vụ 5 đạt | **+5** |
| — | *(trừ)* xem mục "Trừ điểm" | **−** |

Tối đa **105/100**.

---

## A · Tính ổn định — 30 điểm

Nguồn: cột `ỔN ĐỊNH` và bảng `CHECKSUM từng lượt` của `make verify`.

| | Điểm |
|---|---|
| `gold_training_set` cho cùng checksum ở cả ba lượt | 12 |
| `gold_feature_daily` cho cùng checksum ở cả ba lượt | 12 |
| `gold_doc_chunks` cho cùng checksum ở cả ba lượt *(nhóm đối chứng — hỏng nghĩa là bạn đã phá thứ vốn đang chạy tốt)* | 3 |
| `quarantine_tickets` cho cùng checksum ở cả ba lượt | 3 |

> Điểm mục A **không** phụ thuộc số hàng có đúng hay không. Một bảng có thể ổn
> định mà vẫn sai — và ngược lại. Đó là lý do A và B tách riêng.

## B · Tính đúng — 30 điểm

Nguồn: cột `SỐ HÀNG` so với `expected/`.

| | Kỳ vọng | Điểm |
|---|---|---|
| `gold_training_set` | 12.480 | 12 |
| `gold_feature_daily` | 9.100 | 12 |
| `gold_doc_chunks` | 31.200 | 3 |
| `gold_training_set`: 1 hàng / 1 `ticket_id` (không lặp) | — | 3 |

Sai một hàng cũng là sai. Không có điểm từng phần cho "gần đúng" — vì trong
thực tế không ai biết "gần đúng" là gần bao nhiêu.

## C · Chất lượng dữ liệu — 15 điểm

| | Điểm |
|---|---|
| `contract: enforced: true` trên `silver_tickets` và `dbt run` vẫn chạy | 4 |
| `dbt test` pass **và** có thêm test mới so với bản gốc (bản gốc có 9 test) | 4 |
| `quarantine_tickets` đúng **312** hàng, đúng grain (1 hàng / 1 bản ghi CDC) | 4 |
| `silver_tickets.priority` không NULL và luôn ∈ 1..4 | 3 |

**Trừ trong mục C:** quarantine > 1.000 hàng (đã cách ly nhầm nhãn chuỗi hợp lệ)
→ mất toàn bộ 4 điểm của dòng quarantine, dù `dbt test` có pass.

## D · Hiệu năng — 15 điểm

Nguồn: `make explain` (đã so sẵn với mốc trong `expected/dashboard_baseline.json`).

| | Điểm |
|---|---|
| `rows scanned` giảm ≥ 10× | 6 |
| `result hash` không đổi *(nhanh mà sai thì bằng không)* | 4 |
| Số file giảm rõ rệt (compaction thật, không chỉ đổi truy vấn) | 3 |
| Báo cáo có số **trước/sau** của `rows scanned`, không phải số giây | 2 |

> Nếu `result hash` đổi, **cả mục D = 0**. Tối ưu mà đổi kết quả không phải
> tối ưu.

## E · Báo cáo — 10 điểm

Mỗi nhiệm vụ 1–4 được **2,5 điểm**, chấm theo dòng **Nguyên nhân gốc**:

| Chất lượng | Điểm/nhiệm vụ |
|---|---|
| Nêu đúng **cơ chế** gây lỗi, cụ thể tới mức người khác đọc là tránh được lần sau | 2,5 |
| Đúng nhưng chung chung ("model cấu hình sai") | 1,5 |
| Chỉ mô tả cách sửa ("tôi đổi một tham số trong `config()`") | 0,5 |
| Không có / sai | 0 |

Hai con số **bắt buộc** phải xuất hiện trong báo cáo:
- **P99 độ trễ** đo được (nhiệm vụ 2) — thiếu: −1 điểm
- **`rows scanned` trước/sau** (nhiệm vụ 4) — thiếu: −1 điểm

Ví dụ so sánh — lấy một sự cố **không** có trong lab này, để bạn thấy khác biệt
mà không bị lộ đáp án: *"job gửi email nhắc hạn gửi trùng cho một số khách"*.

> ✗ *0,5đ* — "Tôi thêm một bảng `sent_log` và kiểm tra trước khi gửi."
>
> Đây là **cách sửa**. Người đọc không học được gì: lần sau gặp job khác họ
> vẫn mắc lại.
>
> ✓ *2,5đ* — "Job quét theo `where due_date = today` rồi gửi, nhưng không ghi
> lại dấu vết là đã gửi. Retry của scheduler khi timeout mạng vì thế là một
> lần gửi mới hoàn toàn — bản thân hành động gửi không idempotent, nên **mọi**
> cơ chế retry ở tầng trên đều biến thành cơ chế nhân bản. Sửa bằng cách gắn
> khoá tự nhiên (`customer_id`, `due_date`) cho mỗi lần gửi và kiểm tra khoá
> đó trước khi gọi API."
>
> Khác biệt: bản ✓ nói được **vì sao** lỗi xảy ra và **điều kiện nào** làm nó
> tái diễn. Bản ✗ chỉ kể đã gõ gì.

## + · Thưởng — 5 điểm

`make crash-test` báo `NHIỆM VỤ 5: ĐẠT ✓` **và** báo cáo giải thích được
at-most-once / at-least-once / idempotent write.

---

## Trừ điểm

| | |
|---|---|
| Sửa `expected/`, `tools/verify.py`, `tools/explain.py` hoặc `seed/generate.py` để "qua bài" | **0 điểm toàn bài** |
| Xoá bớt dữ liệu nguồn cho số hàng khớp | **0 điểm toàn bài** |
| Nộp kèm `.venv/`, `warehouse.duckdb`, `data/` (không chạy `make clean`) | −3 |
| `make verify` không chạy được trên repo nộp (thiếu file, lỗi import) | −10 |

> Bạn **được phép** sửa mọi thứ trong `dbt/`, `ingest/`, `queries/`,
> `tools/compact.py`, `dags/`. Bạn **không được** sửa `expected/`,
> `seed/generate.py`, `tools/verify.py`, `tools/explain.py`, `tools/common.py`.

---

## Bảng tự chấm nhanh

Điền trước khi nộp:

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | | 12.480 | |
| `gold_training_set` — ổn định 3 lượt | | ✓ | |
| `gold_feature_daily` — số hàng | | 9.100 | |
| `gold_feature_daily` — ổn định 3 lượt | | ✓ | |
| `gold_doc_chunks` — số hàng | | 31.200 | |
| `quarantine_tickets` — số hàng | | 312 | |
| `dbt test` | | pass, > 9 test | |
| `rows scanned` trước → sau | | giảm ≥ 10× | |
| `result hash` | | không đổi | |
| P99 độ trễ đo được | | (ghi số) | |
| `make crash-test` | | ĐẠT ✓ | |
| **Tổng verify** | | 5/5 tiêu chí | |
