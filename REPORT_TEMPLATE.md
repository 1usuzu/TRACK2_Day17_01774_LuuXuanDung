# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
(dán output make verify)
```

</details>

Tổng kết: **… / 5 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Cách khắc phục** | *(file + thay đổi)* |
| **Bằng chứng** | trước: … hàng · sau: … hàng · checksum 3 lượt: … |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | |
| **P99 độ trễ đo được** | **… ngày** *(bắt buộc)* |
| **Lookback đã chọn** | … ngày — vì … |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | trước: … hàng · sau: … hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> …

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách khắc phục** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

---

## 4 · Hiệu năng truy vấn dashboard suy giảm

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Cách khắc phục** | *(storage layout + truy vấn)* |

| Chỉ số | Trước | Sau | Tỷ lệ |
|---|---|---|---|
| `rows scanned` | | | …× |
| số file | | | |
| `result hash` | | | phải giống nhau |

Vì sao chỉ số đánh giá là `rows scanned` chứ không phải thời gian chạy?

> …

---

## 5 · *(mở rộng)* Ngữ nghĩa phân phối khi consumer gặp sự cố

| | |
|---|---|
| **Kết quả kịch bản ban đầu** | mất … hàng / trùng … hàng |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Kết quả `make crash-test`** | |

`DO UPDATE` khác `DO NOTHING` ở điểm nào khi một message được phát lại với
nội dung đã thay đổi?

> …

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | |
| 2 | |
| 3 | |
| 4 | |
