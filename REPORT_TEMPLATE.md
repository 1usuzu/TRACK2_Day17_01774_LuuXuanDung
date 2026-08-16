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

## 1 · Bảng training phình lên sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân gốc** | |
| **Cách sửa** | *(file + thay đổi)* |
| **Bằng chứng** | trước: … hàng · sau: … hàng · checksum 3 lượt: … |

---

## 2 · Thiếu hàng, không ai biết

| | |
|---|---|
| **Triệu chứng** | |
| **P99 độ trễ đo được** | **… ngày** *(bắt buộc)* |
| **Lookback đã chọn** | … ngày — vì … |
| **Nguyên nhân gốc** | |
| **Cách sửa** | |
| **Bằng chứng** | trước: … hàng · sau: … hàng |

Câu hỏi thêm: vì sao chọn P99 chứ không chọn `max`? Trả giá gì?

> …

---

## 3 · Schema đổi giữa chừng

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân gốc** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách sửa** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: chặn ở Bronze hay Silver? Vì sao **không** để pipeline sập
khi gặp bản ghi lỗi?

> …

---

## 4 · Dashboard chậm

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân gốc** | |
| **Cách sửa** | *(bố cục lưu trữ + truy vấn)* |

| Chỉ số | Trước | Sau | Tỷ lệ |
|---|---|---|---|
| `rows scanned` | | | …× |
| số file | | | |
| `result hash` | | | phải giống nhau |

Vì sao đo `rows scanned` chứ không đo thời gian?

> …

---

## 5 · *(mở rộng)* Consumer và sự cố giữa lô

| | |
|---|---|
| **Kịch bản gốc cho ra** | mất … hàng / trùng … hàng |
| **Nguyên nhân gốc** | |
| **Cách sửa** | |
| **Kết quả `make crash-test`** | |

`DO UPDATE` khác `DO NOTHING` ở đâu khi message được phát lại với nội dung
đã đổi?

> …

---

## 6 · Một dòng cho mỗi bài học

| Nhiệm vụ | Nếu gặp lại hệ thống lạ, tôi sẽ kiểm tra điều này đầu tiên |
|---|---|
| 1 | |
| 2 | |
| 3 | |
| 4 | |
