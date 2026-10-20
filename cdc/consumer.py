"""The consumer process.

    python -m cdc.consumer

Reads Debezium topics, applies each batch to the analytics database in one
transaction, and only then commits the Kafka offsets. If the process dies
between the two, the batch is read again and the LSN check makes the replay
a no-op: at-least-once delivery, exactly-once effect.
"""

import logging
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer, KafkaError

from .events import parse
from .sink import Sink

log = logging.getLogger("cdc")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPICS = os.getenv("CDC_TOPICS", "^shop\\.public\\..*")
GROUP = os.getenv("CDC_GROUP", "analytics-replica")
DB = os.getenv("ANALYTICS_DATABASE_URL", "postgresql://analytics:analytics@localhost:5555/analytics")
BATCH = int(os.getenv("CDC_BATCH", "500"))
# Test hook: crash right after the DB commit and before the offset commit, the
# worst moment, after this many batches. Used by scripts/chaos.sh.
CRASH_AFTER = int(os.getenv("CDC_CRASH_AFTER_BATCHES", "0"))


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "topic.metadata.refresh.interval.ms": 5000,   # pick up new tables quickly
        # A crashed consumer never says goodbye; Kafka hands its partitions to a
        # replacement only after this timeout (default 45 s).
        "session.timeout.ms": 10000,
    })
    consumer.subscribe([TOPICS])
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    with psycopg.connect(DB) as conn:
        sink = Sink(conn)
        applied = replayed = batches = 0
        last_log = time.monotonic()
        while not stop:
            msgs = consumer.consume(num_messages=BATCH, timeout=1.0)
            if not msgs:
                continue
            events = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", m.error())
                    continue
                if m.topic().startswith("__"):
                    continue
                ev = parse(m.key(), m.value())
                if ev is not None:
                    events.append(ev)
            batch_replayed = 0
            with conn.transaction():
                for ev in events:
                    if sink.apply(ev):
                        applied += 1
                    else:
                        batch_replayed += 1
            replayed += batch_replayed
            if batch_replayed:
                log.info("skipped %d changes that were already applied (redelivered after a restart)", batch_replayed)
            batches += 1
            if CRASH_AFTER and batches >= CRASH_AFTER:
                log.warning("simulated crash after the DB commit, before committing offsets")
                os._exit(1)
            consumer.commit(asynchronous=False)   # only after the DB commit
            if time.monotonic() - last_log > 5:
                log.info("applied %d changes (%d already applied, skipped)", applied, replayed)
                last_log = time.monotonic()
    consumer.close()
    log.info("stopped: applied %d, skipped %d already applied", applied, replayed)


if __name__ == "__main__":
    run()
