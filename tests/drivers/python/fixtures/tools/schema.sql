CREATE SCHEMA inventory;
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE SEQUENCE order_no START 100 INCREMENT 5;
CREATE TABLE customers (
    id serial PRIMARY KEY,
    name varchar(100) NOT NULL,
    email text UNIQUE,
    mood mood,
    born date,
    score numeric(10, 2) DEFAULT 0,
    active boolean DEFAULT true,
    created timestamptz DEFAULT now()
);
CREATE TABLE items (
    sku char(8) PRIMARY KEY,
    title text NOT NULL,
    qty integer CHECK (qty > 0),
    price double precision,
    tags text[],
    meta json,
    weight real,
    added timestamp,
    ready time,
    shelf_life interval,
    uid uuid,
    blob bytea,
    small smallint,
    big bigint
);
CREATE TABLE orders (
    id bigserial,
    customer_id integer NOT NULL,
    sku char(8),
    placed date NOT NULL,
    note text,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (id),
    CONSTRAINT orders_sku_fk FOREIGN KEY (sku) REFERENCES items (sku),
    CONSTRAINT orders_note_len CHECK (length(note) < 500)
);
CREATE TABLE order_lines (
    order_id bigint REFERENCES orders (id),
    line smallint,
    qty integer NOT NULL DEFAULT 1,
    total numeric(12, 2) GENERATED ALWAYS AS (qty * 2.5) STORED,
    PRIMARY KEY (order_id, line),
    UNIQUE (order_id, qty)
);
CREATE TABLE inventory.stock (
    sku char(8) PRIMARY KEY,
    warehouse text NOT NULL,
    amount integer DEFAULT 0
);
CREATE INDEX customers_name_idx ON customers (name);
CREATE UNIQUE INDEX customers_lower_email_idx ON customers (lower(email));
CREATE INDEX orders_placed_desc_idx ON orders (placed DESC, id);
CREATE INDEX orders_partial_idx ON orders (customer_id) WHERE note IS NOT NULL;
COMMENT ON TABLE customers IS 'people who buy';
COMMENT ON COLUMN customers.email IS 'contact address';
CREATE VIEW customer_orders AS
    SELECT c.name, count(o.id) AS n
    FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
    GROUP BY c.name;
COMMENT ON VIEW customer_orders IS 'orders per customer';
CREATE VIEW inventory.low_stock AS SELECT sku FROM inventory.stock WHERE amount < 5;
CREATE FUNCTION add_one(x integer) RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT x + 1';
