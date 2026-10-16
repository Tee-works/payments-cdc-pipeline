"""Consume Debezium transaction events and build the status history.

Delivery guarantees, in order:
  1. read a batch from Kafka (auto-commit off)
  2. write history + violations to Postgres in one transaction
  3. send unparseable messages to the dead-letter topic
  4. only then commit the Kafka offsets

If the process dies between 2 and 4, the batch is re-read on restart. The
inserts are idempotent (see store.py), so the result is the same.

Run with:  python -m cdc.consumer
"""

from __future__ import annotations

import logging
import signal
import sys
from types import FrameType

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer

from cdc.config import Settings
from cdc.events import EventParseError, parse
from cdc.processing import Outcome, process
from cdc.store import write

log = logging.getLogger("cdc.consumer")


def handle_batch(
    messages: list[Message],
) -> tuple[list[Outcome], list[tuple[Message, str]]]:
    """Split a batch into outcomes to store and poison messages for the DLQ."""
    outcomes: list[Outcome] = []
    dead_letters: list[tuple[Message, str]] = []
    for msg in messages:
        err = msg.error()
        if err is not None:
            # The topic appears once Debezium finishes its first snapshot; until
            # then the broker reports it as unknown. That is expected, not fatal.
            if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                log.warning("source topic not available yet: %s", err)
                continue
            if err.fatal():
                raise KafkaException(err)
            log.warning("kafka error, skipping: %s", err)
            continue
        try:
            event = parse(msg.value())
        except EventParseError as exc:
            dead_letters.append((msg, str(exc)))
            continue
        if event is None:  # tombstone that follows a delete
            continue
        outcomes.append(process(event))
    return outcomes, dead_letters


def run(settings: Settings) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    conn = psycopg2.connect(settings.pg_dsn)

    stopping = False

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        log.info("received signal %s, finishing current batch", signum)
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([settings.source_topic])
    log.info("consuming %s as group %s", settings.source_topic, settings.consumer_group)

    try:
        while not stopping:
            messages = consumer.consume(num_messages=settings.batch_size, timeout=1.0)
            # A poll can return only errors (e.g. the topic does not exist yet).
            # There is no offset to commit then, and commit() would raise.
            if not any(m.error() is None for m in messages):
                for msg in messages:
                    handle_batch([msg])  # logs, or raises if fatal
                continue

            outcomes, dead_letters = handle_batch(messages)
            n_history, n_violations = write(conn, outcomes)

            for msg, reason in dead_letters:
                producer.produce(
                    settings.dlq_topic,
                    key=msg.key(),
                    value=msg.value(),
                    headers=[
                        ("error.reason", reason.encode()),
                        ("source.topic", msg.topic().encode()),
                        ("source.partition", str(msg.partition()).encode()),
                        ("source.offset", str(msg.offset()).encode()),
                    ],
                )
            if dead_letters and producer.flush(10) > 0:
                raise RuntimeError("dead-letter messages not delivered; not committing offsets")

            consumer.commit(asynchronous=False)
            log.info(
                "batch=%d history=%d violations=%d dead_letters=%d",
                len(messages),
                n_history,
                n_violations,
                len(dead_letters),
            )
    finally:
        consumer.close()
        conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(Settings.from_env())
    return 0


if __name__ == "__main__":
    sys.exit(main())
