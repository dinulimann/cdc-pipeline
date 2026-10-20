"""Applies change events to the analytics database.

replica.<table>   current state of every source row (soft-deleted rows kept, flagged)
replica.changes   every change ever applied: the full history, with before/after

Idempotent: each replica row remembers the LSN of the change that wrote it, and
an event is only applied if its LSN is newer. Replaying a batch after a crash
(Kafka delivers at least once) changes nothing.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .events import ChangeEvent

META = """
CREATE SCHEMA IF NOT EXISTS replica;
CREATE TABLE IF NOT EXISTS replica.changes (
    id          bigserial PRIMARY KEY,
    table_name  text        NOT NULL,
    pk          jsonb       NOT NULL,
    op          char(1)     NOT NULL,
    lsn         bigint      NOT NULL,
    tx_id       bigint,
    before      jsonb,
    after       jsonb,
    source_ts   timestamptz NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (table_name, pk, lsn, op)
);
"""


def _json_safe(row: dict | None):
    if row is None:
        return None
    out = {}
    for k, v in row.items():
        out[k] = str(v) if isinstance(v, (Decimal, datetime, date)) else v
    return Jsonb(out)


class Sink:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.columns: dict[str, set[str]] = {}
        with conn.transaction():
            conn.execute(META)

    # ------------------------------------------------------------- schema
    def _ensure_table(self, ev: ChangeEvent) -> None:
        known = self.columns.get(ev.table)
        if known is None:
            cols = [sql.SQL("{} {}").format(sql.Identifier(c.name), sql.SQL(c.pg_type)) for c in ev.columns]
            meta = sql.SQL("_lsn bigint NOT NULL, _op char(1) NOT NULL, _deleted boolean NOT NULL DEFAULT false, "
                           "_source_ts timestamptz NOT NULL, _synced_at timestamptz NOT NULL DEFAULT clock_timestamp()")
            self.conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS replica.{} ({}, {}, PRIMARY KEY ({}))").format(
                sql.Identifier(ev.table), sql.SQL(", ").join(cols), meta,
                sql.SQL(", ").join(map(sql.Identifier, ev.key_columns))))
            rows = self.conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'replica' AND table_name = %s",
                (ev.table,)).fetchall()
            known = self.columns[ev.table] = {r[0] for r in rows}
        # Schema evolution: a column added in the source appears in the event schema.
        # In the source, ADD COLUMN ... DEFAULT x fills the existing rows without
        # emitting any change event, so the replica has to apply the same default.
        for c in ev.columns:
            if c.name not in known:
                default = sql.SQL(" DEFAULT {}").format(sql.Literal(c.default)) if c.has_default else sql.SQL("")
                self.conn.execute(sql.SQL("ALTER TABLE replica.{} ADD COLUMN IF NOT EXISTS {} {}{}").format(
                    sql.Identifier(ev.table), sql.Identifier(c.name), sql.SQL(c.pg_type), default))
                known.add(c.name)

    # -------------------------------------------------------------- apply
    def apply(self, ev: ChangeEvent) -> bool:
        """Applies one event inside the caller's transaction. Returns True if it changed the replica."""
        self._ensure_table(ev)
        pk = {k: ev.key[k] for k in ev.key_columns}

        inserted = self.conn.execute("""
            INSERT INTO replica.changes (table_name, pk, op, lsn, tx_id, before, after, source_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name, pk, lsn, op) DO NOTHING RETURNING id""",
            (ev.table, Jsonb(pk), ev.op, ev.lsn, ev.tx_id, _json_safe(ev.before), _json_safe(ev.after),
             ev.source_ts)).fetchone()
        if inserted is None:
            return False   # exact replay

        table = sql.Identifier(ev.table)
        if ev.op == "d":
            row = ev.before or pk
            cols = list(row.keys())
            stmt = sql.SQL("""
                INSERT INTO replica.{t} ({cols}, _lsn, _op, _deleted, _source_ts)
                VALUES ({vals}, %s, 'd', true, %s)
                ON CONFLICT ({pk}) DO UPDATE
                   SET _deleted = true, _op = 'd', _lsn = EXCLUDED._lsn, _source_ts = EXCLUDED._source_ts,
                       _synced_at = clock_timestamp()
                 WHERE replica.{t}._lsn < EXCLUDED._lsn""").format(
                t=table, cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
                vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                pk=sql.SQL(", ").join(map(sql.Identifier, ev.key_columns)))
            cur = self.conn.execute(stmt, [*row.values(), ev.lsn, ev.source_ts])
            return cur.rowcount > 0

        row = ev.after
        cols = list(row.keys())
        updates = [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in cols if c not in ev.key_columns]
        stmt = sql.SQL("""
            INSERT INTO replica.{t} ({cols}, _lsn, _op, _deleted, _source_ts)
            VALUES ({vals}, %s, %s, false, %s)
            ON CONFLICT ({pk}) DO UPDATE
               SET {updates}, _lsn = EXCLUDED._lsn, _op = EXCLUDED._op, _deleted = false,
                   _source_ts = EXCLUDED._source_ts, _synced_at = clock_timestamp()
             WHERE replica.{t}._lsn < EXCLUDED._lsn""").format(
            t=table, cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            pk=sql.SQL(", ").join(map(sql.Identifier, ev.key_columns)),
            updates=sql.SQL(", ").join(updates) if updates else sql.SQL("_op = EXCLUDED._op"))
        cur = self.conn.execute(stmt, [*row.values(), ev.lsn, ev.op, ev.source_ts])
        return cur.rowcount > 0


def lag_stats(conn: psycopg.Connection, minutes: int = 10) -> dict:
    row = conn.execute("""
        SELECT count(*),
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM applied_at - source_ts)),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM applied_at - source_ts)),
               max(extract(epoch FROM applied_at - source_ts))
        FROM replica.changes
        WHERE op <> 'r' AND applied_at > now() - make_interval(mins => %s)""", (minutes,)).fetchone()
    return {"changes": row[0], "p50_s": row[1], "p95_s": row[2], "max_s": row[3]}


def to_json(value) -> str:
    return json.dumps(value, default=str)
