from cdc.events import parse
from cdc.processing import process
from tests.conftest import tx


def rules(outcome):
    return [v.rule for v in outcome.violations]


def test_new_pending_transaction_starts_history(make_event):
    out = process(parse(make_event("c", after=tx(1))))
    assert [(h.from_status, h.to_status) for h in out.history] == [(None, "pending")]
    assert out.violations == []


def test_insert_not_pending_is_flagged(make_event):
    out = process(parse(make_event("c", after=tx(1, "success"))))
    assert rules(out) == ["invalid_initial_status"]
    assert len(out.history) == 1


def test_snapshot_row_in_any_state_is_accepted(make_event):
    out = process(parse(make_event("r", after=tx(1, "reversed"), snapshot="true")))
    assert out.violations == []
    assert out.history[0].to_status == "reversed"


def test_valid_transition_is_recorded(make_event):
    out = process(parse(make_event("u", before=tx(1), after=tx(1, "success"))))
    assert [(h.from_status, h.to_status) for h in out.history] == [("pending", "success")]
    assert out.violations == []


def test_failed_to_success_is_recorded_and_flagged(make_event):
    out = process(parse(make_event("u", before=tx(1, "failed"), after=tx(1, "success"))))
    # Keep the history row: it really happened, and finance needs the full trail.
    assert [(h.from_status, h.to_status) for h in out.history] == [("failed", "success")]
    assert rules(out) == ["invalid_transition"]


def test_update_without_status_change_writes_nothing(make_event):
    out = process(parse(make_event("u", before=tx(1), after=tx(1))))
    assert out.history == [] and out.violations == []


def test_amount_change_after_settlement_is_flagged(make_event):
    before, after = tx(1, "success", 1000), tx(1, "success", 900)
    out = process(parse(make_event("u", before=before, after=after)))
    assert rules(out) == ["amount_changed_after_settlement"]
    assert out.history == []


def test_amount_change_while_pending_is_fine(make_event):
    out = process(parse(make_event("u", before=tx(1, amount=1000), after=tx(1, amount=900))))
    assert out.violations == []


def test_hard_delete_is_flagged(make_event):
    out = process(parse(make_event("d", before=tx(1, "success"), after=None)))
    assert rules(out) == ["hard_delete"]
    assert out.violations[0].from_status == "success"


def test_update_without_before_image_is_flagged(make_event):
    out = process(parse(make_event("u", before=None, after=tx(1, "success"))))
    assert rules(out) == ["missing_before_image"]


def test_other_tables_are_ignored(make_event):
    out = process(parse(make_event("c", after={"id": 1, "name": "x"}, table="merchants")))
    assert out.history == [] and out.violations == []


def test_event_id_is_carried_through(make_event):
    out = process(parse(make_event("u", before=tx(1), after=tx(1, "failed"), pos=777)))
    assert out.history[0].event_id == "mysql-bin.000003:777:0"
