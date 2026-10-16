import random

from cdc.simulate import plan_outcomes


def test_plan_is_deterministic_and_consistent():
    ids = list(range(1, 501))
    a = plan_outcomes(ids, anomalies=5, rng=random.Random(42))
    b = plan_outcomes(ids, anomalies=5, rng=random.Random(42))
    assert a == b
    assert sorted(a.successes + a.failures) == ids
    assert set(a.reversals) <= set(a.successes)
    assert set(a.anomalies) <= set(a.failures)
    assert len(a.anomalies) == 5


def test_anomalies_capped_by_failures():
    plan = plan_outcomes([1, 2], anomalies=10, rng=random.Random(0))
    assert len(plan.anomalies) == len(plan.failures)
