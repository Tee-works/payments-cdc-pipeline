"""Reconcile the Postgres copy of transactions against the MySQL source.

For each day and status, compare row counts and the sum of amounts. A CDC
pipeline can fail quietly: a connector paused, a topic misrouted, a column
type that truncates. Counting alone misses some of these, so we also compare
money.

Run with:
    python -m cdc.reconcile --date 2026-10-15
    python -m cdc.reconcile --today --wait-seconds 120
Exits 1 when the two sides do not match.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from cdc.config import Settings

SOURCE_SQL = """
SELECT DATE(created_at) AS day, status, COUNT(*) AS n, COALESCE(SUM(amount_minor), 0) AS amount
FROM transactions
WHERE created_at >= %s AND created_at < %s
GROUP BY DATE(created_at), status
"""

SINK_SQL = """
SELECT created_at::date AS day, status, COUNT(*) AS n, COALESCE(SUM(amount_minor), 0) AS amount
FROM public.transactions
WHERE created_at >= %s AND created_at < %s
GROUP BY created_at::date, status
"""


@dataclass(frozen=True)
class Bucket:
    day: date
    status: str
    count: int
    amount_minor: int


@dataclass(frozen=True)
class Mismatch:
    day: date
    status: str
    source_count: int
    sink_count: int
    source_amount: int
    sink_amount: int


def compare(source: Iterable[Bucket], sink: Iterable[Bucket]) -> list[Mismatch]:
    src = {(b.day, b.status): b for b in source}
    dst = {(b.day, b.status): b for b in sink}
    mismatches = []
    for key in sorted(src.keys() | dst.keys()):
        s, d = src.get(key), dst.get(key)
        sc, sa = (s.count, s.amount_minor) if s else (0, 0)
        dc, da = (d.count, d.amount_minor) if d else (0, 0)
        if (sc, sa) != (dc, da):
            mismatches.append(Mismatch(key[0], key[1], sc, dc, sa, da))
    return mismatches


def format_report(mismatches: list[Mismatch]) -> str:
    if not mismatches:
        return "source and sink match"
    lines = [f"{len(mismatches)} bucket(s) do not match:"]
    for m in mismatches:
        lines.append(
            f"  {m.day} {m.status:<9} count {m.source_count} vs {m.sink_count}, "
            f"amount {m.source_amount} vs {m.sink_amount} (minor units)"
        )
    return "\n".join(lines)


def _rows_to_buckets(rows: Iterable[tuple]) -> list[Bucket]:
    return [Bucket(r[0], r[1], int(r[2]), int(r[3])) for r in rows]


def fetch_source(settings: Settings, start: date, end: date) -> list[Bucket]:
    import pymysql

    conn = pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(SOURCE_SQL, (start, end))
            return _rows_to_buckets(cur.fetchall())
    finally:
        conn.close()


def fetch_sink(settings: Settings, start: date, end: date) -> list[Bucket]:
    import psycopg2
    import psycopg2.errors

    conn = psycopg2.connect(settings.pg_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(SINK_SQL, (start, end))
            return _rows_to_buckets(cur.fetchall())
    except psycopg2.errors.UndefinedTable:
        # The JDBC sink creates the table when the first record arrives. Until
        # then the sink holds nothing, which is lag to wait out, not an error.
        return []
    finally:
        conn.close()


def reconcile_range(
    settings: Settings, start: date, end: date, wait_seconds: int = 0
) -> list[Mismatch]:
    """Compare [start, end). Retries until wait_seconds runs out, to allow for lag."""
    deadline = time.monotonic() + wait_seconds
    while True:
        mismatches = compare(fetch_source(settings, start, end), fetch_sink(settings, start, end))
        if not mismatches or time.monotonic() >= deadline:
            return mismatches
        time.sleep(5)


def reconcile_day(settings: Settings, day: date, wait_seconds: int = 0) -> list[Mismatch]:
    return reconcile_range(settings, day, day + timedelta(days=1), wait_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    when = parser.add_mutually_exclusive_group(required=True)
    when.add_argument("--date", type=date.fromisoformat, help="UTC day to reconcile, YYYY-MM-DD")
    when.add_argument("--today", action="store_true", help="reconcile today so far (UTC)")
    when.add_argument("--yesterday", action="store_true", help="reconcile yesterday (UTC)")
    parser.add_argument("--wait-seconds", type=int, default=0, help="retry while lagging")
    args = parser.parse_args(argv)

    today = datetime.now(UTC).date()
    if args.today:
        day = today
    elif args.yesterday:
        day = today - timedelta(days=1)
    else:
        day = args.date
    mismatches = reconcile_day(Settings.from_env(), day, args.wait_seconds)
    print(f"{day}: {format_report(mismatches)}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
