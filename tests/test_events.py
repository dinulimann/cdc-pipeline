from datetime import datetime, timezone
from decimal import Decimal

from cdc.events import parse, pg_type

from .debezium import ORDER_FIELDS, decimal_wire, message, order


def test_insert_is_parsed_with_typed_values():
    ev = parse(*message("c", after=order()))
    assert ev.table == "orders" and ev.op == "c" and ev.before is None
    assert ev.key == {"id": 1} and ev.key_columns == ["id"]
    assert ev.after["total"] == Decimal("125000.50")
    assert ev.after["updated_at"] == datetime(2026, 10, 20, 10, 0, 0, 123456, tzinfo=timezone.utc)
    assert ev.lsn == 100 and ev.tx_id == 7 and not ev.snapshot


def test_negative_and_large_decimals_round_trip():
    for v in ["-0.01", "-125000.50", "9999999999.99", "0.00"]:
        ev = parse(*message("c", after=order(total=v)))
        assert ev.after["total"] == Decimal(v)
    assert decimal_wire("-0.01") != decimal_wire("0.01")


def test_tombstone_after_delete_is_ignored():
    key, _ = message("d", before=order())
    assert parse(key, None) is None


def test_delete_keeps_the_old_row():
    ev = parse(*message("d", before=order(status="paid")))
    assert ev.op == "d" and ev.after is None and ev.before["status"] == "paid"


def test_snapshot_reads_are_flagged():
    assert parse(*message("r", after=order())).snapshot


def test_column_types_come_from_the_schema():
    types = {f["field"]: pg_type(f) for f in ORDER_FIELDS}
    assert types == {"id": "integer", "status": "text", "total": "numeric(12,2)", "updated_at": "timestamptz"}


def test_column_default_is_carried_for_schema_evolution():
    fields = ORDER_FIELDS + [{"type": "string", "optional": False, "default": "web", "field": "channel"}]
    ev = parse(*message("c", after={**order(), "channel": "app"}, fields=fields))
    channel = ev.columns[-1]
    assert channel.name == "channel" and channel.has_default and channel.default == "web"
    assert not ev.columns[0].has_default
