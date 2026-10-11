CREATE TABLE customer_orders (
	name TEXT, 
	n BIGINT
);

CREATE TABLE customers (
	id SERIAL NOT NULL, 
	name TEXT NOT NULL, 
	email TEXT, 
	mood mood, 
	born DATE, 
	score NUMERIC(10, 2) DEFAULT 0, 
	active BOOLEAN DEFAULT true, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now(), 
	CONSTRAINT customers_pkey PRIMARY KEY (id), 
	CONSTRAINT customers_email_key UNIQUE NULLS DISTINCT (email)
);

CREATE UNIQUE INDEX customers_lower_email_idx ON customers USING secondary ();

CREATE INDEX customers_name_idx ON customers USING secondary (name);

CREATE TABLE inventory.low_stock (
	sku TEXT
);

CREATE TABLE inventory.stock (
	sku TEXT NOT NULL, 
	warehouse TEXT NOT NULL, 
	amount INTEGER DEFAULT 0, 
	CONSTRAINT stock_pkey PRIMARY KEY (sku)
);

CREATE TABLE items (
	sku TEXT NOT NULL, 
	title TEXT NOT NULL, 
	qty INTEGER, 
	price DOUBLE PRECISION, 
	tags TEXT[], 
	meta JSON, 
	weight REAL, 
	added TIMESTAMP WITHOUT TIME ZONE, 
	ready TIME WITHOUT TIME ZONE, 
	shelf_life INTERVAL, 
	uid UUID, 
	blob BYTEA, 
	small SMALLINT, 
	big BIGINT, 
	CONSTRAINT items_pkey PRIMARY KEY (sku), 
	CONSTRAINT items_qty_check CHECK (qty > 0)
);

CREATE TABLE orders (
	id BIGSERIAL NOT NULL, 
	customer_id INTEGER NOT NULL, 
	sku TEXT, 
	placed DATE NOT NULL, 
	note TEXT, 
	CONSTRAINT orders_pkey PRIMARY KEY (id), 
	CONSTRAINT orders_customer_fk FOREIGN KEY(customer_id) REFERENCES customers (id), 
	CONSTRAINT orders_sku_fk FOREIGN KEY(sku) REFERENCES items (sku), 
	CONSTRAINT orders_note_len CHECK (length(note) < 500)
);

CREATE INDEX orders_partial_idx ON orders USING secondary (customer_id) WHERE (note IS NOT NULL);

CREATE INDEX orders_placed_desc_idx ON orders USING secondary (placed, id);

CREATE TABLE order_lines (
	order_id BIGINT NOT NULL, 
	line SMALLINT NOT NULL, 
	qty INTEGER DEFAULT 1 NOT NULL, 
	total NUMERIC(12, 2) GENERATED ALWAYS AS (CAST((qty * 2.5) AS DECIMAL(12, 2))) STORED, 
	CONSTRAINT order_lines_pkey PRIMARY KEY (order_id, line), 
	CONSTRAINT order_lines_order_id_fkey FOREIGN KEY(order_id) REFERENCES orders (id), 
	CONSTRAINT order_lines_order_id_qty_key UNIQUE NULLS DISTINCT (order_id, qty)
);
