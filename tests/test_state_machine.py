import pytest

from cdc.state_machine import is_valid_transition


@pytest.mark.parametrize(
    "frm, to",
    [("pending", "success"), ("pending", "failed"), ("success", "reversed")],
)
def test_allowed(frm, to):
    assert is_valid_transition(frm, to)


@pytest.mark.parametrize(
    "frm, to",
    [
        ("failed", "success"),
        ("success", "pending"),
        ("success", "failed"),
        ("reversed", "success"),
        ("pending", "reversed"),
        (None, "success"),
        ("unknown", "success"),
    ],
)
def test_not_allowed(frm, to):
    assert not is_valid_transition(frm, to)
