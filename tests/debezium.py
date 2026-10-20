"""Builds Debezium-shaped messages (JSON converter, schemas enabled) for tests."""

import base64
import json
from decimal import Decimal

ORDER_FIELDS = [
    {"type": "int32", "optional": False, "field": "id"},
    {"type": "string", "optional": False, "field": "status"},
    {"type": "bytes", "optional": False, "name": "org.apache.kafka.connect.data.Decimal",
     "parameters": {"scale": "2", "connect.decimal.precision": "12"}, "field": "total"},
    {"type": "string", "optional": False, "name": "io.debezium.time.ZonedTimestamp", "field": "updated_at"},
]


def decimal_wire(value: str, scale: int = 2) -> str:
    unscaled = int(Decimal(value).scaleb(scale))
    n = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(n, "big", signed=True)).decode()


def message(op, before=None, after=None, lsn=100, table="orders", fields=None, ts_ms=1_760_000_000_000):
    fields = fields or ORDER_FIELDS
    row_schema = {"type": "struct", "optional": True, "fields": fields}
    value = {
        "schema": {"type": "struct", "fields": [
            {**row_schema, "field": "before"}, {**row_schema, "field": "after"},
            {"type": "struct", "field": "source", "fields": []},
            {"type": "string", "field": "op"}]},
        "payload": {"before": before, "after": after, "op": op,
                    "source": {"table": table, "lsn": lsn, "txId": 7, "ts_ms": ts_ms,
                               "snapshot": "true" if op == "r" else "false"}},
    }
    pk = (after or before)["id"]
    key = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": False, "field": "id"}]},
           "payload": {"id": pk}}
    return json.dumps(key).encode(), json.dumps(value).encode()


def order(id=1, status="pending", total="125000.50", updated_at="2026-10-20T10:00:00.123456Z"):
    return {"id": id, "status": status, "total": decimal_wire(total), "updated_at": updated_at}
