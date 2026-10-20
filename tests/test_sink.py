"""Sink tests against a real PostgreSQL (a throwaway cdc_test database)."""

import os

import psycopg
import pytest

from cdc.events import parse
from cdc.sink import Sink

from .debezium import ORDER_FIELDS, message, order

ADMIN = os.getenv("TEST_ADMIN_DATABASE_URL", "postgresql://analytics:analytics@localhost:5555/analytics")
TEST_DB = "cdc_test"


@pytest.fixture
def conn():
    try:
        with psycopg.connect(ADMIN, autocommit=True, connect_timeout=3) as admin:
            if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,)).fetchone():
                admin.execute(f"CREATE DATABASE {TEST_DB}")
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL not reachable (docker compose up -d analytics-db)")
    url = psycopg.conninfo.make_conninfo(ADMIN, dbname=TEST_DB)
    with psycopg.connect(url) as c:
        c.execute("DROP SCHEMA IF EXISTS replica CASCADE")
        c.commit()
        yield c


def apply(sink, *msgs):
    with sink.conn.transaction():
        return [sink.apply(parse(*m)) for m in msgs]


def rows(conn):
    return conn.execute("SELECT id, status, total, _deleted, _lsn FROM replica.orders ORDER BY id").fetchall()


def test_insert_update_delete(conn):
    sink = Sink(conn)
    apply(sink, message("c", after=order(), lsn=100),
          message("u", before=order(), after=order(status="paid"), lsn=200))
    assert [r[:2] for r in rows(conn)] == [(1, "paid")]
    apply(sink, message("d", before=order(status="paid"), lsn=300))
    assert rows(conn)[0][3] is True        # soft delete: the row stays, flagged
    ops = conn.execute("SELECT op FROM replica.changes ORDER BY lsn").fetchall()
    assert [o[0] for o in ops] == ["c", "u", "d"]


def test_replaying_a_batch_changes_nothing(conn):
    sink = Sink(conn)
    batch = [message("c", after=order(), lsn=100),
             message("u", before=order(), after=order(status="paid"), lsn=200)]
    assert apply(sink, *batch) == [True, True]
    before = rows(conn)
    assert apply(sink, *batch) == [False, False]   # redelivered after a crash
    assert rows(conn) == before
    assert conn.execute("SELECT count(*) FROM replica.changes").fetchone()[0] == 2


def test_an_older_event_never_overwrites_a_newer_one(conn):
    sink = Sink(conn)
    apply(sink, message("u", before=order(), after=order(status="shipped"), lsn=300))
    assert apply(sink, message("u", before=order(), after=order(status="paid"), lsn=200)) == [False]
    assert rows(conn)[0][1] == "shipped"


def test_new_column_with_default_backfills_existing_rows(conn):
    sink = Sink(conn)
    apply(sink, message("c", after=order(id=1), lsn=100))
    fields = ORDER_FIELDS + [{"type": "string", "optional": False, "default": "web", "field": "channel"}]
    apply(sink, message("c", after={**order(id=2), "channel": "app"}, lsn=200, fields=fields))
    got = conn.execute("SELECT id, channel FROM replica.orders ORDER BY id").fetchall()
    assert got == [(1, "web"), (2, "app")]   # same as the source after ADD COLUMN ... DEFAULT 'web'
