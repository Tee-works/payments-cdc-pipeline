"""End-to-end: write to MySQL, wait for CDC, check Postgres.

Needs the stack running and connectors registered:
    make up register
    make integration
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest

from cdc import reconcile, simulate
from cdc.config import Settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1"),
]


def wait_for(check, timeout=180, every=3):
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if result or time.monotonic() > deadline:
            return result
        time.sleep(every)


def count_violations(settings: Settings, rule: str) -> int:
    with psycopg2.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cdc.transition_violations WHERE rule = %s", (rule,))
        return cur.fetchone()[0]


def test_changes_flow_from_mysql_to_postgres():
    settings = Settings.from_env()
    before = count_violations(settings, "invalid_transition")

    summary = simulate.run(settings, transactions=200, anomalies=3, seed=7)
    assert summary.anomalies == 3

    # Window around today, so a run that crosses midnight UTC still compares everything.
    today = datetime.now(UTC).date()
    start, end = today - timedelta(days=1), today + timedelta(days=2)

    mismatches = reconcile.reconcile_range(settings, start, end, wait_seconds=180)
    assert mismatches == [], reconcile.format_report(mismatches)

    flagged = wait_for(lambda: count_violations(settings, "invalid_transition") - before >= 3)
    assert flagged, "consumer did not flag the injected failed -> success changes"
