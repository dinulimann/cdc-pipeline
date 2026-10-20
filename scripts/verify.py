"""Compares the replica with the source, row by row, and reports replication lag.

    python scripts/verify.py [--wait 60]

Exits 1 if any table differs. --wait keeps retrying while the consumer catches up.
"""

import argparse
import hashlib
import os
import sys
import time

import psycopg

SOURCE = os.getenv("SOURCE_DATABASE_URL", "postgresql://shop:shop@localhost:5554/shop")
ANALYTICS = os.getenv("ANALYTICS_DATABASE_URL", "postgresql://analytics:analytics@localhost:5555/analytics")
TABLES = ["customers", "products", "orders"]


def fingerprint(conn, query: str) -> tuple[int, str]:
    rows = conn.execute(query).fetchall()
    h = hashlib.sha256()
    for r in rows:
        h.update(repr(tuple(str(v) for v in r)).encode())
    return len(rows), h.hexdigest()[:16]


def compare() -> list[tuple]:
    out = []
    with psycopg.connect(SOURCE) as src, psycopg.connect(ANALYTICS) as dst:
        for t in TABLES:
            cols = [r[0] for r in src.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name = %s ORDER BY ordinal_position", (t,)).fetchall()]
            col_list = ", ".join(f'"{c}"' for c in cols)
            s = fingerprint(src, f"SELECT {col_list} FROM public.{t} ORDER BY id")
            try:
                d = fingerprint(dst, f"SELECT {col_list} FROM replica.{t} WHERE NOT _deleted ORDER BY id")
            except psycopg.errors.UndefinedTable:
                dst.rollback()
                d = (0, s[1]) if s[0] == 0 else (0, "missing")   # no changes yet for an empty table
            out.append((t, s, d))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=float, default=0)
    a = ap.parse_args()
    deadline = time.monotonic() + a.wait
    while True:
        result = compare()
        ok = all(s == d for _, s, d in result)
        if ok or time.monotonic() > deadline:
            break
        time.sleep(2)

    print(f"{'table':<10} {'source rows':>11} {'replica rows':>12}  {'source hash':<16}  {'replica hash':<16}  match")
    for t, s, d in result:
        print(f"{t:<10} {s[0]:>11} {d[0]:>12}  {s[1]:<16}  {d[1]:<16}  {'yes' if s == d else 'NO'}")
    with psycopg.connect(ANALYTICS) as dst:
        n, deleted, changes = dst.execute("""
            SELECT (SELECT count(*) FROM replica.changes),
                   (SELECT count(*) FROM replica.changes WHERE op = 'd'),
                   (SELECT count(*) FROM replica.changes WHERE op <> 'r')""").fetchone()
        lag = dst.execute("""
            SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM applied_at - source_ts)),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM applied_at - source_ts))
            FROM replica.changes WHERE op <> 'r'""").fetchone()
    print(f"\nhistory: {n} changes recorded ({changes} live, {n - changes} from the initial snapshot); "
          f"{deleted} deletes kept as history")
    if lag[0] is not None:
        print(f"lag source commit -> replica: p50 {lag[0]:.2f}s, p95 {lag[1]:.2f}s (includes any time the consumer was stopped)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
