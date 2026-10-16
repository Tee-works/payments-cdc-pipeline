"""Turn a change event into status-history rows and rule violations.

This is pure logic with no I/O, so every rule is covered by unit tests.

Why a history table at all? The Postgres sink keeps the latest state of each
transaction (upsert). That answers "what is the status now", but not "when did
it become successful" or "was it ever marked failed first". CDC sees every
intermediate state, so we keep them as an append-only history.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cdc.events import ChangeEvent
from cdc.state_machine import INITIAL_STATUS, SETTLED, is_valid_transition


@dataclass(frozen=True)
class HistoryRow:
    transaction_id: int
    event_id: str
    op: str
    from_status: str | None
    to_status: str
    amount_minor: int
    source_ts_ms: int


@dataclass(frozen=True)
class Violation:
    transaction_id: int
    event_id: str
    rule: str
    detail: str
    from_status: str | None
    to_status: str | None


@dataclass
class Outcome:
    history: list[HistoryRow] = field(default_factory=list)
    violations: list[Violation] = field(default_factory=list)


def process(event: ChangeEvent) -> Outcome:
    out = Outcome()
    if event.table != "transactions":
        return out

    tid = event.key

    def violation(rule: str, detail: str, frm: str | None, to: str | None) -> None:
        out.violations.append(Violation(tid, event.event_id, rule, detail, frm, to))

    if event.op == "d":
        before_status = (event.before or {}).get("status")
        violation(
            "hard_delete",
            "transactions must never be deleted; record a reversal instead",
            before_status,
            None,
        )
        return out

    after = event.after or {}
    status = after["status"]
    amount = int(after["amount_minor"])

    def record(frm: str | None) -> None:
        out.history.append(
            HistoryRow(tid, event.event_id, event.op, frm, status, amount, event.source_ts_ms)
        )

    if event.op in ("c", "r"):
        record(None)
        # Snapshot reads reflect whatever state the row is in today, so only
        # brand-new inserts must start as pending.
        if event.op == "c" and status != INITIAL_STATUS:
            violation(
                "invalid_initial_status",
                f"created as {status!r}, expected {INITIAL_STATUS!r}",
                None,
                status,
            )
        return out

    # op == "u"
    if event.before is None:
        violation(
            "missing_before_image",
            "update without a before image; check binlog_row_image=FULL",
            None,
            status,
        )
        record(None)
        return out

    prev = event.before.get("status")
    if prev != status:
        record(prev)
        if not is_valid_transition(prev, status):
            violation("invalid_transition", f"{prev} -> {status} is not allowed", prev, status)

    prev_amount = event.before.get("amount_minor")
    if prev in SETTLED and prev_amount is not None and int(prev_amount) != amount:
        violation(
            "amount_changed_after_settlement",
            f"amount changed {prev_amount} -> {amount} on a {prev} transaction",
            prev,
            status,
        )
    return out
