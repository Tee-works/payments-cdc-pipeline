"""Write processed outcomes to Postgres.

Every insert is ON CONFLICT DO NOTHING on (transaction_id, event_id), where
event_id is the binlog position. Kafka delivers at least once, so after a
crash the consumer re-reads some messages; replaying them changes nothing.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cdc.processing import Outcome

HISTORY_SQL = """
INSERT INTO cdc.transaction_status_history
    (transaction_id, event_id, op, from_status, to_status, amount_minor, source_ts)
VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0))
ON CONFLICT (transaction_id, event_id) DO NOTHING
"""

VIOLATION_SQL = """
INSERT INTO cdc.transition_violations
    (transaction_id, event_id, rule, detail, from_status, to_status)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id, event_id, rule) DO NOTHING
"""


def write(conn: Any, outcomes: Iterable[Outcome]) -> tuple[int, int]:
    """Write all outcomes in one database transaction. Returns (history, violations)."""
    history = []
    violations = []
    for o in outcomes:
        history.extend(
            (
                h.transaction_id,
                h.event_id,
                h.op,
                h.from_status,
                h.to_status,
                h.amount_minor,
                h.source_ts_ms,
            )
            for h in o.history
        )
        violations.extend(
            (v.transaction_id, v.event_id, v.rule, v.detail, v.from_status, v.to_status)
            for v in o.violations
        )

    try:
        with conn.cursor() as cur:
            if history:
                cur.executemany(HISTORY_SQL, history)
            if violations:
                cur.executemany(VIOLATION_SQL, violations)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(history), len(violations)
