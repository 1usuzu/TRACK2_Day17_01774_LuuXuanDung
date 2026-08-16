#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5.

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

Kịch bản sự cố (tools/crash_test.py tự lo):
    thêm --crash-at-batch 7  -> tiến trình tự chết ở lô thứ 7, y hệt kill -9.

Đọc kỹ hàm consume(). Chỉ ba dòng trong "vùng được phép sắp xếp lại" là
đáng quan tâm, cộng với cách write_batch() ghi dữ liệu.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

DDL = f"""
create table if not exists {TABLE} (
    event_id      varchar,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho."""
    con.executemany(
        f"insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
                r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
            )
            for r in batch
        ],
    )


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── vùng bạn được phép sắp xếp lại ────────────────────────────
            consumer.commit()                 # (1) ghi nhận offset
            maybe_crash(batch_no, crash_at)   # (2) sự cố có thể xảy ra ở đây
            write_batch(con, batch)           # (3) ghi dữ liệu
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
