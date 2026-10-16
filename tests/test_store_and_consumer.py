"""Store and consumer logic, tested with small fakes instead of real services."""

from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError

from cdc.consumer import handle_batch
from cdc.processing import HistoryRow, Outcome, Violation
from cdc.store import HISTORY_SQL, VIOLATION_SQL, write
from tests.conftest import tx


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, rows):
        self.log.append((sql, list(rows)))


class FakeConn:
    def __init__(self, fail=False):
        self.calls, self.committed, self.rolled_back, self.fail = [], False, False, fail

    def cursor(self):
        if self.fail:
            raise RuntimeError("db down")
        return FakeCursor(self.calls)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_write_is_idempotent_sql_in_one_transaction():
    outcome = Outcome(
        history=[HistoryRow(1, "f:1:0", "u", "pending", "success", 100, 1760600000000)],
        violations=[Violation(1, "f:1:0", "invalid_transition", "x", "failed", "success")],
    )
    conn = FakeConn()
    assert write(conn, [outcome]) == (1, 1)
    assert conn.committed
    assert [sql for sql, _ in conn.calls] == [HISTORY_SQL, VIOLATION_SQL]
    assert all("ON CONFLICT" in sql for sql, _ in conn.calls)
    assert conn.calls[0][1][0][:2] == (1, "f:1:0")


def test_write_rolls_back_and_raises_on_error():
    conn = FakeConn(fail=True)
    with pytest.raises(RuntimeError):
        write(conn, [Outcome()])
    assert conn.rolled_back and not conn.committed


class FakeMessage:
    def __init__(self, value=None, error=None):
        self._value, self._error = value, error

    def value(self):
        return self._value

    def error(self):
        return self._error


def test_handle_batch_splits_good_events_tombstones_and_poison(make_event):
    batch = [
        FakeMessage(make_event("u", before=tx(1), after=tx(1, "success"))),
        FakeMessage(None),  # tombstone
        FakeMessage(b"{broken"),
        FakeMessage(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
    ]
    outcomes, dead = handle_batch(batch)
    assert len(outcomes) == 1 and outcomes[0].history[0].to_status == "success"
    assert len(dead) == 1 and "not valid JSON" in dead[0][1]


def test_handle_batch_raises_on_fatal_kafka_error():
    fatal = KafkaError(KafkaError._FATAL, "boom", fatal=True)
    with pytest.raises(Exception, match="boom"):
        handle_batch([FakeMessage(error=fatal)])


class StopLoop(Exception):
    pass


class FakeConsumer:
    """Returns the given batches in turn, then stops the loop."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0

    def subscribe(self, topics):
        pass

    def consume(self, num_messages, timeout):
        if not self.batches:
            raise StopLoop
        return self.batches.pop(0)

    def commit(self, asynchronous):
        self.commits += 1

    def close(self):
        pass


def test_run_does_not_commit_when_a_poll_returns_only_errors(monkeypatch):
    # Before Debezium creates the topic, a poll returns only UNKNOWN_TOPIC_OR_PART.
    # Committing then raises _NO_OFFSET in confluent-kafka and crashed the consumer.
    from cdc import consumer as mod
    from cdc.config import Settings

    fake = FakeConsumer([[FakeMessage(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))]])
    monkeypatch.setattr(mod, "Consumer", lambda conf: fake)
    monkeypatch.setattr(mod, "Producer", lambda conf: object())
    monkeypatch.setattr(mod.psycopg2, "connect", lambda dsn: SimpleNamespace(close=lambda: None))

    with pytest.raises(StopLoop):
        mod.run(Settings())
    assert fake.commits == 0
