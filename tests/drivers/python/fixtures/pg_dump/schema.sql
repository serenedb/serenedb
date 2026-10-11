--
-- PostgreSQL database dump
--

\restrict drivers


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: inventory; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA inventory;


ALTER SCHEMA inventory OWNER TO postgres;

--
-- Name: mood; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.mood AS ENUM (
    'sad',
    'ok',
    'happy'
);


ALTER TYPE public.mood OWNER TO postgres;

--
-- Name: add_one(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.add_one(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$SELECT x + 1$$;


ALTER FUNCTION public.add_one(x integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: stock; Type: TABLE; Schema: inventory; Owner: postgres
--

CREATE TABLE inventory.stock (
    sku character(8) NOT NULL,
    warehouse text NOT NULL,
    amount integer DEFAULT 0
);


ALTER TABLE inventory.stock OWNER TO postgres;

--
-- Name: low_stock; Type: VIEW; Schema: inventory; Owner: postgres
--

CREATE VIEW inventory.low_stock AS
 SELECT sku
   FROM inventory.stock
  WHERE (amount < 5);


ALTER VIEW inventory.low_stock OWNER TO postgres;

--
-- Name: customers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.customers (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    email text,
    mood public.mood,
    born date,
    score numeric(10,2) DEFAULT 0,
    active boolean DEFAULT true,
    created timestamp with time zone DEFAULT now()
);


ALTER TABLE public.customers OWNER TO postgres;

--
-- Name: TABLE customers; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.customers IS 'people who buy';


--
-- Name: COLUMN customers.email; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.customers.email IS 'contact address';


--
-- Name: orders; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.orders (
    id bigint NOT NULL,
    customer_id integer NOT NULL,
    sku character(8),
    placed date NOT NULL,
    note text,
    CONSTRAINT orders_note_len CHECK ((length(note) < 500))
);


ALTER TABLE public.orders OWNER TO postgres;

--
-- Name: customer_orders; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.customer_orders AS
 SELECT c.name,
    count(o.id) AS n
   FROM (public.customers c
     LEFT JOIN public.orders o ON ((o.customer_id = c.id)))
  GROUP BY c.name;


ALTER VIEW public.customer_orders OWNER TO postgres;

--
-- Name: VIEW customer_orders; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON VIEW public.customer_orders IS 'orders per customer';


--
-- Name: customers_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.customers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.customers_id_seq OWNER TO postgres;

--
-- Name: customers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.customers_id_seq OWNED BY public.customers.id;


--
-- Name: items; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.items (
    sku character(8) NOT NULL,
    title text NOT NULL,
    qty integer,
    price double precision,
    tags text[],
    meta json,
    weight real,
    added timestamp without time zone,
    ready time without time zone,
    shelf_life interval,
    uid uuid,
    blob bytea,
    small smallint,
    big bigint,
    CONSTRAINT items_qty_check CHECK ((qty > 0))
);


ALTER TABLE public.items OWNER TO postgres;

--
-- Name: order_lines; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.order_lines (
    order_id bigint NOT NULL,
    line smallint NOT NULL,
    qty integer DEFAULT 1 NOT NULL,
    total numeric(12,2) GENERATED ALWAYS AS (((qty)::numeric * 2.5)) STORED
);


ALTER TABLE public.order_lines OWNER TO postgres;

--
-- Name: order_no; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.order_no
    START WITH 100
    INCREMENT BY 5
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.order_no OWNER TO postgres;

--
-- Name: orders_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.orders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.orders_id_seq OWNER TO postgres;

--
-- Name: orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.orders_id_seq OWNED BY public.orders.id;


--
-- Name: customers id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customers ALTER COLUMN id SET DEFAULT nextval('public.customers_id_seq'::regclass);


--
-- Name: orders id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders ALTER COLUMN id SET DEFAULT nextval('public.orders_id_seq'::regclass);


--
-- Name: stock stock_pkey; Type: CONSTRAINT; Schema: inventory; Owner: postgres
--

ALTER TABLE ONLY inventory.stock
    ADD CONSTRAINT stock_pkey PRIMARY KEY (sku);


--
-- Name: customers customers_email_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_email_key UNIQUE (email);


--
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (id);


--
-- Name: items items_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.items
    ADD CONSTRAINT items_pkey PRIMARY KEY (sku);


--
-- Name: order_lines order_lines_order_id_qty_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.order_lines
    ADD CONSTRAINT order_lines_order_id_qty_key UNIQUE (order_id, qty);


--
-- Name: order_lines order_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.order_lines
    ADD CONSTRAINT order_lines_pkey PRIMARY KEY (order_id, line);


--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);


--
-- Name: customers_lower_email_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX customers_lower_email_idx ON public.customers USING btree (lower(email));


--
-- Name: customers_name_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX customers_name_idx ON public.customers USING btree (name);


--
-- Name: orders_partial_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX orders_partial_idx ON public.orders USING btree (customer_id) WHERE (note IS NOT NULL);


--
-- Name: orders_placed_desc_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX orders_placed_desc_idx ON public.orders USING btree (placed DESC, id);


--
-- Name: order_lines order_lines_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.order_lines
    ADD CONSTRAINT order_lines_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders(id);


--
-- Name: orders orders_customer_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers(id);


--
-- Name: orders orders_sku_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_sku_fk FOREIGN KEY (sku) REFERENCES public.items(sku);


--
-- PostgreSQL database dump complete
--

\unrestrict drivers

