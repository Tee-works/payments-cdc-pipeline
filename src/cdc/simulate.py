"""Generate realistic payment traffic in MySQL so there is something to capture.

Creates merchants and transactions, then moves transactions through their
lifecycle (pending -> success / failed, some success -> reversed with a
refund). With --anomalies N it also makes N illegal changes (failed ->
success) that the CDC consumer should flag.

Run with:  python -m cdc.simulate --transactions 500 --anomalies 5 --seed 42
"""

from __future__ import annotations

import argparse
import random
import sys
import uuid
from dataclasses import dataclass

from cdc.config import Settings

MERCHANTS = [
    ("Lagos Bites", "NG", "NGN"),
    ("Accra Threads", "GH", "GHS"),
    ("Nairobi Rides", "KE", "KES"),
    ("Cape Cloud", "ZA", "ZAR"),
    ("Abuja Pharmacy", "NG", "NGN"),
]
CHANNELS = ["card", "bank_transfer", "ussd", "mobile_money"]


@dataclass(frozen=True)
class Plan:
    successes: list[int]
    failures: list[int]
    reversals: list[int]
    anomalies: list[int]


@dataclass(frozen=True)
class Summary:
    created: int
    succeeded: int
    failed: int
    reversed: int
    anomalies: int


def plan_outcomes(ids: list[int], anomalies: int, rng: random.Random) -> Plan:
    """Decide what happens to each pending transaction. Pure, so it is testable."""
    successes, failures = [], []
    for tid in ids:
        (successes if rng.random() < 0.85 else failures).append(tid)
    reversals = [tid for tid in successes if rng.random() < 0.03]
    bad = rng.sample(failures, k=min(anomalies, len(failures)))
    return Plan(successes, failures, reversals, bad)


def run(settings: Settings, transactions: int, anomalies: int, seed: int) -> Summary:
    import pymysql

    rng = random.Random(seed)
    conn = pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, country FROM merchants")
            merchants = cur.fetchall()
            if not merchants:
                cur.executemany(
                    "INSERT INTO merchants (name, country) VALUES (%s, %s)",
                    [(name, country) for name, country, _ in MERCHANTS],
                )
                cur.execute("SELECT id, country FROM merchants")
                merchants = cur.fetchall()
            currency = {country: cur_ for _, country, cur_ in MERCHANTS}

            ids = []
            for _ in range(transactions):
                merchant_id, country = rng.choice(merchants)
                cur.execute(
                    "INSERT INTO transactions"
                    " (reference, merchant_id, amount_minor, currency, channel)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (
                        # Random per run so the generator can be re-run without
                        # clashing with the UNIQUE reference column.
                        f"txn_{uuid.uuid4().hex[:16]}",
                        merchant_id,
                        rng.randint(100_00, 500_000_00),
                        currency[country],
                        rng.choice(CHANNELS),
                    ),
                )
                ids.append(cur.lastrowid)

            plan = plan_outcomes(ids, anomalies, rng)
            set_status = "UPDATE transactions SET status = %s WHERE id = %s"
            cur.executemany(set_status, [("success", t) for t in plan.successes])
            cur.executemany(set_status, [("failed", t) for t in plan.failures])
            for tid in plan.reversals:
                cur.execute(set_status, ("reversed", tid))
                cur.execute(
                    "INSERT INTO refunds (transaction_id, amount_minor, status)"
                    " SELECT id, amount_minor, 'processed' FROM transactions WHERE id = %s",
                    (tid,),
                )
            # Illegal on purpose: a failed payment must never become successful.
            cur.executemany(set_status, [("success", t) for t in plan.anomalies])
    finally:
        conn.close()

    return Summary(
        created=transactions,
        succeeded=len(plan.successes),
        failed=len(plan.failures),
        reversed=len(plan.reversals),
        anomalies=len(plan.anomalies),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate payment traffic in MySQL.")
    parser.add_argument("--transactions", type=int, default=500)
    parser.add_argument("--anomalies", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    summary = run(Settings.from_env(), args.transactions, args.anomalies, args.seed)
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
