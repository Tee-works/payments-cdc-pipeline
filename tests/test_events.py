import json

import pytest

from cdc.events import EventParseError, parse
from tests.conftest import tx


def test_parses_bare_payload(make_event):
    event = parse(make_event("c", after=tx(7), pos=4242))
    assert event.op == "c"
    assert event.table == "transactions"
    assert event.key == 7
    assert event.event_id == "mysql-bin.000003:4242:0"
    assert event.snapshot is False


def test_parses_payload_wrapped_with_schema(make_event):
    event = parse(make_event("u", before=tx(3), after=tx(3, "success"), wrap_schema=True))
    assert event.op == "u"
    assert event.before["status"] == "pending"
    assert event.after["status"] == "success"


def test_tombstones_return_none():
    assert parse(None) is None
    assert parse(json.dumps({"schema": None, "payload": None})) is None


def test_snapshot_flag(make_event):
    assert parse(make_event("r", after=tx(), snapshot="true")).snapshot is True
    assert parse(make_event("r", after=tx(), snapshot="last")).snapshot is True


def test_delete_uses_before_image_for_key(make_event):
    event = parse(make_event("d", before=tx(11), after=None))
    assert event.key == 11


@pytest.mark.parametrize(
    "raw, message",
    [
        (b"not json", "not valid JSON"),
        (b"[1, 2]", "not a JSON object"),
        (json.dumps({"op": "x", "source": {"table": "t"}}), "unknown op"),
        (json.dumps({"op": "c", "source": {}}), "missing source.table"),
        (json.dumps({"op": "c", "source": {"table": "transactions"}}), "binlog position"),
    ],
)
def test_rejects_unusable_messages(raw, message):
    with pytest.raises(EventParseError, match=message):
        parse(raw)
