"""Generates shop traffic on the source database: new customers and orders,
order status changes, price and stock updates, cancellations (deletes) and
multi-row transactions.

    python scripts/simulate.py --seconds 60 --rate 20
"""

import argparse
import os
import random
import time

import psycopg

DB = os.getenv("SOURCE_DATABASE_URL", "postgresql://shop:shop@localhost:5554/shop")
NEXT = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}


def step(conn, rng: random.Random) -> str:
    r = rng.random()
    with conn.transaction():
        if r < 0.30:   # an order with a total, and the stock it takes, in one transaction
            customer = conn.execute("SELECT id FROM customers ORDER BY random() LIMIT 1").fetchone()[0]
            product, price = conn.execute("SELECT id, price FROM products WHERE active ORDER BY random() LIMIT 1").fetchone()
            qty = rng.randint(1, 4)
            conn.execute("INSERT INTO orders (customer_id, total) VALUES (%s, %s)", (customer, price * qty))
            conn.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (qty, product))
            return "new order"
        if r < 0.65:
            row = conn.execute("SELECT id, status FROM orders WHERE status <> 'delivered' ORDER BY random() LIMIT 1").fetchone()
            if row:
                conn.execute("UPDATE orders SET status = %s, updated_at = now() WHERE id = %s", (NEXT[row[1]], row[0]))
            return "status change"
        if r < 0.80:
            conn.execute("UPDATE products SET price = round(price * %s::numeric, -2) WHERE id = %s",
                         (rng.choice([0.9, 0.95, 1.05, 1.1]), rng.randint(1, 50)))
            return "price change"
        if r < 0.90:
            n = conn.execute("SELECT coalesce(max(id), 0) + 1 FROM customers").fetchone()[0]
            conn.execute("INSERT INTO customers (email, full_name, city) VALUES (%s, %s, %s)",
                         (f"new{n}-{rng.randint(0, 10**6)}@example.com", f"New Customer {n}",
                          rng.choice(["Jakarta", "Bandung", "Surabaya", "Medan"])))
            return "new customer"
        conn.execute("DELETE FROM orders WHERE id = (SELECT id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1)")
        return "cancelled order"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--rate", type=float, default=20, help="changes per second")
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    counts: dict[str, int] = {}
    with psycopg.connect(DB, autocommit=True) as conn:
        end = time.monotonic() + a.seconds
        while time.monotonic() < end:
            kind = step(conn, rng)
            counts[kind] = counts.get(kind, 0) + 1
            time.sleep(1 / a.rate)
    print("simulated:", ", ".join(f"{v} {k}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
