"""Parse Debezium change events into a small, typed structure.

Debezium wraps every row change in an envelope with `before`, `after`,
`op` and a `source` block that says where in the MySQL binlog the change
came from. Kafka Connect's JsonConverter may also wrap that envelope in
{"schema": ..., "payload": ...}. This module accepts both shapes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# c = insert, u = update, d = delete, r = row read during the initial snapshot
OPS = frozenset({"c", "u", "d", "r"})


class EventParseError(ValueError):
    """The message is not a usable Debezium change event."""


@dataclass(frozen=True)
class ChangeEvent:
    op: str
    table: str
    before: Mapping[str, Any] | None
    after: Mapping[str, Any] | None
    event_id: str
    source_ts_ms: int
    snapshot: bool

    @property
    def row(self) -> Mapping[str, Any]:
        """The row after the change, or before it for deletes."""
        row = self.after if self.after is not None else self.before
        if row is None:
            raise EventParseError("event has neither a before nor an after image")
        return row

    @property
    def key(self) -> int:
        return int(self.row["id"])


def parse(raw: bytes | str | Mapping[str, Any] | None) -> ChangeEvent | None:
    """Return a ChangeEvent, or None for a tombstone.

    Raises EventParseError for anything that is not a Debezium change event,
    so the caller can route it to a dead-letter topic instead of dropping it.
    """
    if raw is None:
        return None

    doc: Any = raw
    if isinstance(raw, bytes | str):
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise EventParseError(f"not valid JSON: {exc}") from exc

    if doc is None:
        return None
    if not isinstance(doc, Mapping):
        raise EventParseError("event is not a JSON object")

    payload = doc.get("payload", doc)
    if payload is None:
        return None

    op = payload.get("op")
    if op not in OPS:
        raise EventParseError(f"unknown op {op!r}")

    source = payload.get("source") or {}
    table = source.get("table")
    if not table:
        raise EventParseError("missing source.table")

    # The binlog position identifies a change uniquely. It is the idempotency
    # key downstream, so refuse events without it rather than invent one.
    if source.get("file") is None or source.get("pos") is None:
        raise EventParseError("missing binlog position (source.file / source.pos)")
    event_id = f"{source['file']}:{source['pos']}:{source.get('row', 0)}"

    return ChangeEvent(
        op=op,
        table=table,
        before=payload.get("before"),
        after=payload.get("after"),
        event_id=event_id,
        source_ts_ms=int(source.get("ts_ms") or payload.get("ts_ms") or 0),
        snapshot=str(source.get("snapshot", "false")).lower() != "false",
    )
