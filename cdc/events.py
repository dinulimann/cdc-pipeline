"""Parsing Debezium change events (JSON converter with schemas). Pure functions."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str
    has_default: bool = False
    default: object = None     # already converted, like a row value


@dataclass
class ChangeEvent:
    table: str
    op: str                    # c=insert, u=update, d=delete, r=snapshot read
    key: dict
    before: dict | None
    after: dict | None
    columns: list[Column]      # from the value schema, in source order
    key_columns: list[str]
    lsn: int
    tx_id: int | None
    source_ts: datetime        # when the change was committed in the source
    snapshot: bool


def pg_type(field: dict) -> str:
    """Kafka Connect / Debezium field schema -> PostgreSQL column type."""
    name, t = field.get("name"), field["type"]
    if name == "org.apache.kafka.connect.data.Decimal":
        scale = int(field.get("parameters", {}).get("scale", 0))
        precision = field.get("parameters", {}).get("connect.decimal.precision")
        return f"numeric({precision},{scale})" if precision else "numeric"
    if name in ("io.debezium.time.ZonedTimestamp",):
        return "timestamptz"
    if name in ("io.debezium.time.MicroTimestamp", "io.debezium.time.Timestamp",
                "org.apache.kafka.connect.data.Timestamp"):
        return "timestamp"
    if name in ("io.debezium.time.Date", "org.apache.kafka.connect.data.Date"):
        return "date"
    if name == "io.debezium.data.Json":
        return "jsonb"
    return {"int8": "smallint", "int16": "smallint", "int32": "integer", "int64": "bigint",
            "float32": "real", "float64": "double precision", "boolean": "boolean",
            "string": "text", "bytes": "bytea"}.get(t, "text")


def convert(value, field: dict):
    """Debezium wire value -> Python value matching pg_type(field)."""
    if value is None:
        return None
    name = field.get("name")
    if name == "org.apache.kafka.connect.data.Decimal":
        unscaled = int.from_bytes(base64.b64decode(value), "big", signed=True)
        return Decimal(unscaled).scaleb(-int(field.get("parameters", {}).get("scale", 0)))
    if name == "io.debezium.time.ZonedTimestamp":
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if name == "io.debezium.time.MicroTimestamp":
        return datetime(1970, 1, 1) + timedelta(microseconds=value)
    if name in ("io.debezium.time.Timestamp", "org.apache.kafka.connect.data.Timestamp"):
        return datetime(1970, 1, 1) + timedelta(milliseconds=value)
    if name in ("io.debezium.time.Date", "org.apache.kafka.connect.data.Date"):
        return date(1970, 1, 1) + timedelta(days=value)
    if field["type"] == "bytes":
        return base64.b64decode(value)
    return value


def _row_fields(value_schema: dict) -> list[dict]:
    for f in value_schema["fields"]:
        if f["field"] in ("after", "before"):
            return f["fields"]
    raise ValueError("value schema has no before/after struct")


def parse(key_bytes: bytes | None, value_bytes: bytes | None) -> ChangeEvent | None:
    """Returns None for tombstones (the null value Kafka gets after a delete)."""
    if value_bytes is None:
        return None
    value = json.loads(value_bytes)
    key = json.loads(key_bytes) if key_bytes else {"schema": {"fields": []}, "payload": {}}
    payload = value["payload"]
    if payload is None:
        return None
    fields = _row_fields(value["schema"])
    by_name = {f["field"]: f for f in fields}

    def row(r):
        return None if r is None else {k: convert(v, by_name[k]) for k, v in r.items() if k in by_name}

    src = payload["source"]
    return ChangeEvent(
        table=src["table"],
        op=payload["op"],
        key=key["payload"],
        before=row(payload.get("before")),
        after=row(payload.get("after")),
        columns=[Column(f["field"], pg_type(f), "default" in f, convert(f.get("default"), f)) for f in fields],
        key_columns=[f["field"] for f in key["schema"]["fields"]],
        lsn=int(src["lsn"]),
        tx_id=src.get("txId"),
        source_ts=datetime.fromtimestamp(src["ts_ms"] / 1000, tz=timezone.utc),
        snapshot=str(src.get("snapshot", "false")) not in ("false", "False"),
    )
