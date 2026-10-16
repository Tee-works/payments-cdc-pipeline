"""The lifecycle a payment transaction is allowed to follow.

    pending -> success -> reversed
    pending -> failed

Anything else (for example failed -> success) means a bug or a manual
database edit upstream, and finance needs to know about it.
"""

from __future__ import annotations

INITIAL_STATUS = "pending"

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"success", "failed"}),
    "success": frozenset({"reversed"}),
    "failed": frozenset(),
    "reversed": frozenset(),
}

# Once money has moved, the amount must not change.
SETTLED = frozenset({"success", "reversed"})


def is_valid_transition(from_status: str | None, to_status: str) -> bool:
    if from_status is None:
        return False
    return to_status in ALLOWED_TRANSITIONS.get(from_status, frozenset())
