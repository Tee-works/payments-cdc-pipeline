from __future__ import annotations

import itertools
import json
from typing import Any

import pytest

_positions = itertools.count(1000, 150)


def debezium_payload(
    op: str,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    table: str = "transactions",
    snapshot: str = "false",
    pos: int | None = None,
) -> dict[str, Any]:
    """A change event shaped like the ones the Debezium MySQL connector emits."""
    return {
        "before": before,
        "after": after,
        "source": {
            "version": "2.7.4.Final",
            "connector": "mysql",
            "name": "gateway",
            "ts_ms": 1760600000000,
            "snapshot": snapshot,
            "db": "payments",
            "table": table,
            "server_id": 1,
            "gtid": None,
            "file": "mysql-bin.000003",
            "pos": pos if pos is not None else next(_positions),
            "row": 0,
        },
        "op": op,
        "ts_ms": 1760600000123,
    }


def tx(id_: int = 1, status: str = "pending", amount: int = 250000) -> dict[str, Any]:
    return {
        "id": id_,
        "reference": f"txn_{id_:016d}",
        "merchant_id": 1,
        "amount_minor": amount,
        "currency": "NGN",
        "channel": "card",
        "status": status,
        "created_at": 1760600000000,
        "updated_at": 1760600000000,
    }


@pytest.fixture
def make_event():
    def _make(*args: Any, wrap_schema: bool = False, **kwargs: Any) -> bytes:
        payload = debezium_payload(*args, **kwargs)
        doc = {"schema": {"type": "struct"}, "payload": payload} if wrap_schema else payload
        return json.dumps(doc).encode()

    return _make
