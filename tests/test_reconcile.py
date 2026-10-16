from datetime import date

from cdc.reconcile import Bucket, compare, format_report

DAY = date(2026, 10, 15)


def b(status, count, amount, day=DAY):
    return Bucket(day, status, count, amount)


def test_matching_sides():
    side = [b("success", 10, 5000), b("failed", 2, 300)]
    assert compare(side, list(side)) == []
    assert format_report([]) == "source and sink match"


def test_count_mismatch():
    [m] = compare([b("success", 10, 5000)], [b("success", 9, 5000)])
    assert (m.source_count, m.sink_count) == (10, 9)


def test_amount_mismatch_with_equal_counts():
    # Same number of rows, different money: the case row counts alone would miss.
    [m] = compare([b("success", 10, 5000)], [b("success", 10, 4999)])
    assert (m.source_amount, m.sink_amount) == (5000, 4999)


def test_bucket_missing_from_sink():
    [m] = compare([b("reversed", 1, 700)], [])
    assert (m.status, m.sink_count, m.sink_amount) == ("reversed", 0, 0)


def test_bucket_only_in_sink():
    [m] = compare([], [b("success", 1, 100)])
    assert (m.source_count, m.sink_count) == (0, 1)


def test_report_lists_each_mismatch():
    report = format_report(compare([b("success", 2, 10), b("failed", 1, 5)], []))
    assert report.startswith("2 bucket(s) do not match")
    assert "success" in report and "failed" in report


def test_missing_sink_table_counts_as_lag_not_error(monkeypatch):
    # The JDBC sink creates public.transactions on its first record. A reconcile
    # that runs before then must see an empty sink and keep waiting, not crash.
    import psycopg2
    import psycopg2.errors

    from cdc import reconcile
    from cdc.config import Settings

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params):
            raise psycopg2.errors.UndefinedTable('relation "public.transactions" does not exist')

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(psycopg2, "connect", lambda dsn: Conn())
    assert reconcile.fetch_sink(Settings(), DAY, DAY) == []
