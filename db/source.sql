-- Operational database of a fictional online shop (the CDC source).
CREATE TABLE customers (
    id          serial PRIMARY KEY,
    email       text NOT NULL UNIQUE,
    full_name   text NOT NULL,
    city        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE products (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    price       numeric(12,2) NOT NULL,
    stock       integer NOT NULL,
    active      boolean NOT NULL DEFAULT true
);
CREATE TABLE orders (
    id          serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES customers,
    status      text NOT NULL DEFAULT 'pending',
    total       numeric(12,2) NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- Debezium needs the full old row for updates and deletes
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

INSERT INTO customers (email, full_name, city)
SELECT 'customer' || i || '@example.com', 'Customer ' || i,
       (ARRAY['Jakarta','Bandung','Surabaya','Medan','Makassar','Denpasar'])[1 + i % 6]
FROM generate_series(1, 200) i;
INSERT INTO products (name, price, stock)
SELECT 'Product ' || i, round((10000 + random() * 490000)::numeric, -3), 50 + (random() * 200)::int
FROM generate_series(1, 50) i;
