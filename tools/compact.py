#!/usr/bin/env python3
"""Nén và bố trí lại bãi Parquet của dashboard — NHIỆM VỤ 4.  ⚠️ CHƯA VIẾT

Hiện trạng: `data/gold_events/` có 5.000 file, mỗi file vài chục KB, không
phân vùng, thứ tự hàng ngẫu nhiên.

Việc của bạn: đọc toàn bộ bãi cũ, ghi ra bãi mới có bố cục tốt hơn, rồi
sửa `queries/dashboard.sql` để trỏ vào bãi mới.

    python tools/compact.py            # ghi ra bãi mới
    python tools/explain.py            # đo lại

Gợi ý — trả lời ba câu này trước khi gõ code:

    1. Truy vấn trong queries/dashboard.sql lọc theo cột nào?
    2. Cột đó có xuất hiện trong đường dẫn file không?
    3. Nếu ghi mỗi ngày một thư mục, engine có cần mở 5.000 file nữa không?

Khung sẵn có bên dưới. Xem GUIDE.md mục "Nhiệm vụ 4" nếu cần pseudo-code.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    # TODO(nhiệm vụ 4) --------------------------------------------------
    #  con.execute(f'''
    #      copy (
    #          select * from read_parquet('{SRC}/*.parquet')
    #          order by ...                 -- gom hàng cùng khách vào cạnh nhau
    #      ) to '{DST}' (
    #          format parquet,
    #          partition_by (...),          -- cột mà dashboard lọc theo
    #          overwrite_or_ignore,
    #          row_group_size ...
    #      )
    #  ''')
    # -------------------------------------------------------------------
    print("\n  ⚠️  tools/compact.py chưa được viết — đây là nhiệm vụ 4.")
    print("  Mở file này và điền phần TODO, rồi chạy lại `make compact`.")
    print("  Khung code: PSEUDOCODE.md mục 'Nhiệm vụ 4'.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
