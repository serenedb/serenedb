from sqllogic_gen import Test

CAUSES = {
    "aggregate_type": "Aggregates over integers and numeric return other types than PostgreSQL's",
    "catalog_content": "SereneDB lacks catalog content PostgreSQL has: builtin object descriptions, every SQL "
                       "feature, the column types of system views",
    "deparse": "Deparsed definitions differ: defaults lose their casts, checks print IN and BETWEEN, views "
               "print count_star()",
    "duckdb_types": "SereneDB has types PostgreSQL lacks: geometry, union, variant",
    "fk_operators": "pg_constraint leaves foreign key operators NULL and pg_depend lacks a default's dependency "
                    "on its type",
    "group_by": "GROUP BY a table alias that is also an output column name is rejected",
    "index_am": "SereneDB indexes use access methods of their own",
    "index_desc": "CREATE INDEX drops DESC",
    "null_array": "NULL || array is NULL where PostgreSQL returns the array",
    "numeric": "A bare numeric is DECIMAL(18,3)",
    "owner": "The bootstrap superuser has oid 16384, not 10, and public belongs to postgres, not "
             "pg_database_owner",
    "pgonly": "Identity columns, referential actions, PL/pgSQL, DDL ORMs emit and jsonb are not supported",
    "quoted_keywords": "quote_ident, regclass and regtype quote words only DuckDB reserves",
    "ri_triggers": "SereneDB checks foreign keys without internal triggers",
    "serial_sequence": "serial creates a bigint sequence where PostgreSQL creates an integer one",
    "server_credentials": "pg_foreign_server shows options that carry credentials, so only a superuser reads it",
    "system_columns": "Tables lack PostgreSQL's system columns",
    "text_search": "SereneDB's full-text search has no tsvector, tsquery or text search configurations",
    "toast": "SereneDB has no TOAST tables",
    "typmod": "Type modifiers are dropped or rejected",
    "updatable_views": "Views are not updatable",
}

MAIN = ("app", "public")
PGONLY = ("pgonly",)

t = Test("PostgreSQL clients read SereneDB's catalog as they read PostgreSQL's: pg_catalog and "
         "information_schema describe the same objects the same way, and the queries psql, "
         "JDBC, DBeaver, asyncpg and other clients run get PostgreSQL's answers. A difference "
         "SereneDB keeps is a pair of queries, each engine's answer recorded from it, with its "
         "cause in CAUSES.")


def check(sql, *causes):
    assert set(causes) <= CAUSES.keys() and (causes or "@NOT " not in sql), sql
    if not causes:
        t.query(sql)
        return
    if "@NOT " in sql:
        t.query(sql.replace("@NOT ", "NOT "))
    differing = sql.replace("@NOT ", "")
    t.query(differing, skipif="serenedb")
    t.query(differing, onlyif="serenedb")


def pgonly(sql, error):
    t.statement(sql, skipif="serenedb")
    t.error(sql, f"db error: ERROR: {error}", onlyif="serenedb")


def oids(scope):
    return "(" + ", ".join(f"'{schema}'::regnamespace::oid" for schema in scope) + ")"


def quoted(values):
    return "(" + ", ".join(f"'{value}'" for value in values) + ")"


def relations(scope):
    return f"(SELECT oid FROM pg_catalog.pg_class WHERE relnamespace IN {oids(scope)})"


def objects(scope):
    owned = [f"SELECT oid FROM pg_catalog.pg_{catalog} WHERE {column} IN {oids(scope)}"
             for catalog, column in (("class", "relnamespace"), ("type", "typnamespace"),
                                     ("proc", "pronamespace"), ("constraint", "connamespace"))]
    owned += [f"SELECT oid FROM pg_catalog.pg_{catalog} WHERE {column} IN {relations(scope)}"
              for catalog, column in (("attrdef", "adrelid"), ("rewrite", "ev_class"),
                                      ("trigger", "tgrelid"), ("policy", "polrelid"))]
    owned += [f"SELECT '{schema}'::regnamespace::oid" for schema in scope if schema != "public"]
    return "(" + " UNION ALL ".join(owned) + ")"


def named(catalog, oid):
    return (f"CASE {catalog} WHEN 'pg_class'::regclass::oid THEN {oid}::regclass::text "
            f"WHEN 'pg_type'::regclass::oid THEN {oid}::regtype::text "
            f"WHEN 'pg_proc'::regclass::oid THEN {oid}::regprocedure::text "
            f"WHEN 'pg_namespace'::regclass::oid THEN {oid}::regnamespace::text "
            "WHEN 'pg_constraint'::regclass::oid THEN (SELECT x.conname::text || ' on ' || CASE WHEN x.conrelid <> 0 "
            "THEN x.conrelid::regclass::text ELSE x.contypid::regtype::text END "
            f"FROM pg_catalog.pg_constraint x WHERE x.oid = {oid}) "
            "WHEN 'pg_attrdef'::regclass::oid THEN (SELECT x.adrelid::regclass::text || ' default ' || x.adnum::text "
            f"FROM pg_catalog.pg_attrdef x WHERE x.oid = {oid}) "
            "WHEN 'pg_rewrite'::regclass::oid THEN (SELECT x.ev_class::regclass::text || ' rule ' || x.rulename::text "
            f"FROM pg_catalog.pg_rewrite x WHERE x.oid = {oid}) "
            "WHEN 'pg_trigger'::regclass::oid THEN (SELECT CASE WHEN x.tgisinternal THEN 'internal trigger' "
            "ELSE x.tgname::text END || ' on ' || x.tgrelid::regclass::text "
            f"FROM pg_catalog.pg_trigger x WHERE x.oid = {oid}) "
            "WHEN 'pg_policy'::regclass::oid THEN (SELECT x.polname::text || ' on ' || x.polrelid::regclass::text "
            f"FROM pg_catalog.pg_policy x WHERE x.oid = {oid}) "
            f"ELSE CASE WHEN {oid} < 16384 THEN {oid}::text ELSE 'user object' END END")


def acl(column):
    return f"(SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE \"C\") FROM unnest({column}) x) AS {column}"


def db(column):
    return f"CASE WHEN {column}::text = current_database() THEN 'db' ELSE {column}::text END AS {column}"


def spaced(expression, name):
    return f"trim(regexp_replace({expression}, '[[:space:]]+', ' ', 'g')) AS {name}"


t.statement("DROP ROLE IF EXISTS conf_reader")
t.statement("CREATE ROLE conf_reader")
t.statement("CREATE SCHEMA app")
t.statement("CREATE TYPE app.mood AS ENUM ('sad', 'ok', 'happy')")
t.statement("CREATE TYPE app.address AS (street text, city varchar(40), zip int4)")
t.statement("CREATE SEQUENCE app.ticket_seq START 100 INCREMENT 5")
t.statement("CREATE SEQUENCE plain_seq")
t.statement("""CREATE TABLE customers (
  id serial PRIMARY KEY,
  email varchar(50) NOT NULL UNIQUE,
  name text NOT NULL,
  active bool DEFAULT true,
  mood app.mood DEFAULT 'ok',
  home app.address,
  created timestamptz DEFAULT now()
)""")
t.statement("""CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  customer_id int4 NOT NULL REFERENCES customers (id),
  amount numeric(10,2) NOT NULL DEFAULT 0 CHECK (amount >= 0),
  status varchar(20) DEFAULT 'new',
  note text,
  ordered_on date DEFAULT current_date,
  ticket int8 DEFAULT nextval('app.ticket_seq'),
  CONSTRAINT orders_status_check CHECK (status IN ('new', 'paid', 'shipped'))
)""")
t.statement("""CREATE TABLE order_items (
  order_id int8 REFERENCES orders (id),
  line int4,
  sku varchar(30) NOT NULL,
  qty int4 NOT NULL CHECK (qty > 0),
  price numeric(12,4),
  PRIMARY KEY (order_id, line)
)""")
t.statement("""CREATE TABLE app.all_types (
  c_int2 int2,
  c_int4 int4,
  c_int8 int8 PRIMARY KEY,
  c_numeric numeric(10,2),
  c_numeric_plain numeric,
  c_varchar varchar(50),
  c_char char(3),
  c_text text NOT NULL DEFAULT 'x',
  c_bool bool,
  c_date date,
  c_time time,
  c_ts timestamp,
  c_tstz timestamptz,
  c_interval interval,
  c_float4 float4,
  c_float8 float8,
  c_uuid uuid,
  c_json json,
  c_bytea bytea,
  c_int_arr int4[],
  c_text_arr text[],
  c_mood app.mood
)""")
t.statement("CREATE TABLE app.json_tab (id int4 PRIMARY KEY, doc json)")
t.statement("""CREATE TABLE app.unique_tab (
  id int8 PRIMARY KEY,
  code varchar(10) UNIQUE,
  qty int4 CHECK (qty BETWEEN 1 AND 100)
)""")
t.statement("CREATE TABLE app.child_tab (id int4 PRIMARY KEY, unique_id int8 REFERENCES app.unique_tab (id))")
t.statement("CREATE UNIQUE INDEX customers_name_email_idx ON customers (name, email)")
t.statement("CREATE INDEX orders_customer_idx ON orders (customer_id)")
t.statement("CREATE INDEX orders_amount_desc_idx ON orders (amount DESC)")
t.statement("CREATE INDEX order_items_sku_idx ON order_items (sku)")
t.statement("CREATE INDEX all_types_varchar_idx ON app.all_types (c_varchar)")
t.statement("CREATE VIEW active_customers AS SELECT id, email, name FROM customers WHERE active")
t.statement("CREATE VIEW app.order_totals AS SELECT o.customer_id, sum(o.amount) AS total, count(*) AS n "
            "FROM orders o GROUP BY o.customer_id")
t.statement("CREATE FUNCTION app.add_one(i int4) RETURNS int4 LANGUAGE sql AS $$ SELECT i + 1 $$")
t.statement("COMMENT ON TABLE customers IS 'Customer master'")
t.statement("COMMENT ON COLUMN customers.email IS 'Login email'")
t.statement("COMMENT ON COLUMN orders.amount IS 'Order amount'")
t.statement("COMMENT ON VIEW active_customers IS 'Only active customers'")
t.statement("COMMENT ON TYPE app.mood IS 'Mood enum'")
t.statement("COMMENT ON INDEX orders_customer_idx IS 'Orders by customer'")
t.statement("COMMENT ON SEQUENCE app.ticket_seq IS 'Ticket numbers'")
t.statement("COMMENT ON FUNCTION app.add_one IS 'Adds one'")
t.statement("GRANT USAGE ON SCHEMA app TO conf_reader")
t.statement("GRANT SELECT ON customers TO conf_reader")
t.statement("GRANT SELECT, INSERT ON orders TO conf_reader")
t.statement("GRANT SELECT ON app.order_totals TO conf_reader")

UNMATCHED = "('sdb_progress', 'sdb_metrics', 'sdb_settings', 'pg_stat_progress_create_table_as')"


def shape(namespace, differing=""):
    return f'''SELECT c.relname::text AS rel, a.attname::text AS col,
  pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype, a.attnotnull AS not_null
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '{namespace}' AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND c.relname NOT IN {UNMATCHED}{differing}
ORDER BY c.relname::text COLLATE "C", a.attnum'''


check(shape("pg_catalog", """
  AND (c.relname::text, a.attname::text) @NOT IN (('pg_replication_origin_status', 'local_lsn'),
    ('pg_replication_origin_status', 'remote_lsn'), ('pg_stat_io', 'extend_bytes'), ('pg_stat_io', 'read_bytes'),
    ('pg_stat_io', 'write_bytes'), ('pg_stat_ssl', 'client_serial'), ('pg_stat_subscription', 'latest_end_lsn'),
    ('pg_stat_subscription', 'received_lsn'), ('pg_stat_wal', 'wal_bytes'))"""), "catalog_content")
check(shape("information_schema"))

SYSTEM_RELATIONS = f"""WHERE c.relnamespace IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
  AND c.relkind IN ('r', 'v') AND c.relname NOT IN {UNMATCHED}"""
SYSTEM_ORDER = 'ORDER BY c.relnamespace::regnamespace::text COLLATE "C", c.relname::text COLLATE "C"'
check(f'''SELECT c.relnamespace::regnamespace::text AS nsp, c.relname::text AS rel,
  CASE WHEN c.relkind = 'r' AND c.relnamespace = 'pg_catalog'::regnamespace THEN c.oid END AS oid, c.relkind,
  c.relnatts, c.relisshared, c.relpersistence, t.typname::text AS rowtype, t.typtype, t.typrelid = c.oid AS linked,
  a.typname::text AS array_type
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_type t ON t.oid = c.reltype
LEFT JOIN pg_catalog.pg_type a ON a.oid = t.typarray
{SYSTEM_RELATIONS}
{SYSTEM_ORDER}''')
check(f'''SELECT c.relnamespace::regnamespace::text AS nsp, c.relname::text AS rel,
  pg_catalog.has_any_column_privilege('conf_reader', c.oid, 'SELECT') AS some_columns
FROM pg_catalog.pg_class c
{SYSTEM_RELATIONS}
  AND NOT pg_catalog.has_table_privilege('conf_reader', c.oid, 'SELECT')
  AND c.relname::text @NOT IN ('pg_foreign_server')
{SYSTEM_ORDER}''', "server_credentials")
check(f'''SELECT c.relnamespace::regnamespace::text AS nsp, c.relname::text AS rel, a.attname::text AS col
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
{SYSTEM_RELATIONS}
  AND pg_catalog.has_any_column_privilege('conf_reader', c.oid, 'SELECT')
  AND NOT pg_catalog.has_column_privilege('conf_reader', c.oid, a.attnum, 'SELECT')
{SYSTEM_ORDER}, a.attnum''')
check(f"""SELECT oid, spcname, pg_catalog.pg_get_userbyid(spcowner) AS spcowner, {acl("spcacl")}, spcoptions
FROM pg_catalog.pg_tablespace
ORDER BY oid""")
check(f'''SELECT oid::regnamespace::text AS oid, nspname, pg_catalog.pg_get_userbyid(nspowner) AS nspowner, {acl("nspacl")}
FROM pg_catalog.pg_namespace
WHERE (oid IN {oids(MAIN)} OR nspname IN ('pg_catalog', 'public', 'information_schema'))
  AND oid::regnamespace::text @NOT IN ('public')
ORDER BY oid::regnamespace::text COLLATE "C"''', "owner")


def pg_class(scope):
    return f'''SELECT oid::regclass::text AS oid, relname, relnamespace::regnamespace::text AS relnamespace,
  reltype::regtype::text AS reltype, reloftype::regtype::text AS reloftype,
  pg_catalog.pg_get_userbyid(relowner) AS relowner, CASE WHEN relkind IN ('i', 'I') THEN NULL ELSE relam END AS relam,
  reltablespace, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers,
  relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relrewrite,
  {acl("relacl")}, reloptions, pg_catalog.pg_get_expr(relpartbound, oid) AS relpartbound
FROM pg_catalog.pg_class
WHERE relnamespace IN {oids(scope)}
ORDER BY oid::regclass::text COLLATE "C"'''


def pg_attribute(scope):
    return f'''SELECT attrelid::regclass::text AS attrelid, attname, attlen, attnum, attndims, attbyval, attalign, attstorage,
  attcompression, attnotnull, atthasdef, atthasmissing, attidentity, attgenerated, attisdropped, attislocal,
  attinhcount, attcollation, attstattarget, {acl("attacl")}, attoptions, attfdwoptions
FROM pg_catalog.pg_attribute
WHERE attrelid IN {relations(scope)} AND attnum > 0
ORDER BY attrelid::regclass::text COLLATE "C", attnum'''


def pg_attribute_types(scope, differing=""):
    return f'''SELECT attrelid::regclass::text AS attrelid, attnum, atttypid::regtype::text AS atttypid, atttypmod
FROM pg_catalog.pg_attribute
WHERE attrelid IN {relations(scope)} AND attnum > 0{differing}
ORDER BY attrelid::regclass::text COLLATE "C", attnum'''


TYPE_COLUMNS = f"""typname, typnamespace::regnamespace::text AS typnamespace,
  pg_catalog.pg_get_userbyid(typowner) AS typowner, typlen, typbyval, typtype, typcategory, typispreferred,
  typisdefined, typdelim, typrelid::regclass::text AS typrelid, typsubscript::text AS typsubscript,
  typelem::regtype::text AS typelem, typarray::regtype::text AS typarray, typinput::text AS typinput,
  typoutput::text AS typoutput, typreceive::text AS typreceive, typsend::text AS typsend, typmodin::text AS typmodin,
  typmodout::text AS typmodout, typanalyze::text AS typanalyze, typalign, typstorage, typnotnull,
  typbasetype::regtype::text AS typbasetype, typtypmod, typndims, typcollation, typdefault, {acl("typacl")}"""


def pg_type(scope):
    return f'''SELECT oid::regtype::text AS oid, {TYPE_COLUMNS}
FROM pg_catalog.pg_type
WHERE typnamespace IN {oids(scope)}
ORDER BY oid::regtype::text COLLATE "C"'''


def pg_constraint(scope, differing=""):
    return f'''SELECT conname, connamespace::regnamespace::text AS connamespace, contype, condeferrable, condeferred, conenforced,
  convalidated, conrelid::regclass::text AS conrelid, contypid::regtype::text AS contypid,
  conindid::regclass::text AS conindid, conparentid = 0 AS conparentid, confrelid::regclass::text AS confrelid,
  translate((confupdtype)::text, ' ', '_') AS confupdtype, translate((confdeltype)::text, ' ', '_') AS confdeltype,
  translate((confmatchtype)::text, ' ', '_') AS confmatchtype, conislocal, coninhcount, connoinherit, conperiod,
  conkey, confkey, array_to_string(conpfeqop::oid[], ' ') AS conpfeqop,
  array_to_string(conppeqop::oid[], ' ') AS conppeqop, array_to_string(conffeqop::oid[], ' ') AS conffeqop,
  confdelsetcols, array_to_string(conexclop::oid[], ' ') AS conexclop,
  pg_catalog.pg_get_expr(conbin, conrelid) AS conbin
FROM pg_catalog.pg_constraint
WHERE connamespace IN {oids(scope)}{differing}
ORDER BY conname::text COLLATE "C", conrelid::regclass::text COLLATE "C", contypid::regtype::text COLLATE "C"'''


def pg_description(scope):
    return f"""SELECT * FROM (
SELECT {named("classoid", "objoid")} AS objoid,
  classoid::regclass::text AS classoid, objsubid, description
FROM pg_catalog.pg_description
WHERE objoid IN {objects(scope)}
) q
ORDER BY q.objoid::text COLLATE "C", q.classoid::text COLLATE "C", q.objsubid"""


def pg_depend(scope, differing=""):
    return f'''SELECT * FROM (
SELECT classid::regclass::text AS classid, {named("classid", "objid")} AS objid,
  objsubid, refclassid::regclass::text AS refclassid, {named("refclassid", "refobjid")} AS refobjid,
  refobjsubid, deptype
FROM pg_catalog.pg_depend
WHERE (objid IN {objects(scope)} OR refobjid IN {objects(scope)})
  AND NOT (classid = 'pg_class'::regclass::oid AND objid IN (SELECT reltoastrelid FROM pg_catalog.pg_class WHERE reltoastrelid <> 0))
) q{differing}
ORDER BY q.classid::text COLLATE "C", q.objid::text COLLATE "C", q.objsubid, q.refclassid::text COLLATE "C",
  q.refobjid::text COLLATE "C", q.refobjsubid, q.deptype::text COLLATE "C"'''


def pg_shdepend(scope):
    return f'''SELECT * FROM (
SELECT dbid = (SELECT x.oid FROM pg_catalog.pg_database x WHERE x.datname = current_database()) AS dbid,
  classid::regclass::text AS classid, {named("classid", "objid")} AS objid,
  objsubid, refclassid::regclass::text AS refclassid, pg_catalog.pg_get_userbyid(refobjid) AS refobjid, deptype
FROM pg_catalog.pg_shdepend
WHERE dbid IN (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()) AND objid IN {objects(scope)}
) q
ORDER BY q.dbid, q.classid::text COLLATE "C", q.objid::text COLLATE "C", q.objsubid, q.refclassid::text COLLATE "C",
  q.refobjid::text COLLATE "C", q.deptype::text COLLATE "C"'''


def pg_sequence(scope, differing=""):
    return f'''SELECT seqrelid::regclass::text AS seqrelid, seqtypid::regtype::text AS seqtypid, seqstart, seqincrement, seqmax,
  seqmin, seqcache, seqcycle
FROM pg_catalog.pg_sequence
WHERE seqrelid IN {relations(scope)}{differing}
ORDER BY seqrelid::regclass::text COLLATE "C"'''


def pg_trigger(scope):
    return f"""SELECT tgrelid::regclass::text AS tgrelid, tgparentid, tgname, tgfoid::regproc::text AS tgfoid, tgtype, tgenabled,
  tgisinternal, tgconstrrelid::regclass::text AS tgconstrrelid, tgconstrindid::regclass::text AS tgconstrindid,
  tgconstraint, tgdeferrable, tginitdeferred, tgnargs, array_to_string(tgattr::int2[], ' ') AS tgattr,
  encode(tgargs, 'hex') AS tgargs, pg_catalog.pg_get_expr(tgqual, tgrelid) AS tgqual, tgoldtable, tgnewtable
FROM pg_catalog.pg_trigger
WHERE tgrelid IN {relations(scope)} AND NOT tgisinternal
ORDER BY tgrelid::regclass::text COLLATE "C", tgname::text COLLATE "C\""""


def pg_proc(scope):
    return f"""SELECT oid::regprocedure::text AS oid, proname, pronamespace::regnamespace::text AS pronamespace,
  pg_catalog.pg_get_userbyid(proowner) AS proowner,
  (SELECT x.lanname::text FROM pg_catalog.pg_language x WHERE x.oid = prolang) AS prolang, procost, prorows,
  provariadic::regtype::text AS provariadic, prosupport::text AS prosupport, prokind, prosecdef, proleakproof,
  proisstrict, proretset, provolatile, proparallel, pronargs, pronargdefaults,
  prorettype::regtype::text AS prorettype,
  array_to_string(ARRAY(SELECT x::regtype::text FROM unnest(proargtypes::oid[]) x), ',') AS proargtypes,
  array_to_string(ARRAY(SELECT x::regtype::text FROM unnest(proallargtypes::oid[]) x), ',') AS proallargtypes,
  proargmodes, proargnames,
  array_to_string(ARRAY(SELECT x::regtype::text FROM unnest(protrftypes::oid[]) x), ',') AS protrftypes, probin,
  proconfig, {acl("proacl")}, {spaced("prosrc", "prosrc")}
FROM pg_catalog.pg_proc
WHERE pronamespace IN {oids(scope)}
ORDER BY oid::regprocedure::text COLLATE "C\""""


def columns(scope, differing=""):
    return f'''SELECT {db("table_catalog")},
  table_schema, table_name, column_name, ordinal_position, is_nullable, numeric_precision, numeric_precision_radix,
  numeric_scale, datetime_precision, interval_type, interval_precision,
  {db("character_set_catalog")}, character_set_schema, character_set_name,
  {db("collation_catalog")}, collation_schema, collation_name,
  {db("domain_catalog")}, domain_schema, domain_name, {db("udt_catalog")}, udt_schema,
  {db("scope_catalog")}, scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_self_referencing,
  is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum,
  identity_cycle, is_generated, generation_expression, is_updatable
FROM information_schema.columns
WHERE table_schema IN {quoted(scope)}{differing}
ORDER BY table_schema::text COLLATE "C", table_name::text COLLATE "C", column_name::text COLLATE "C"'''


def column_types(scope, differing=""):
    return f'''SELECT table_schema, table_name, column_name, column_default, data_type, character_maximum_length,
  character_octet_length, udt_name
FROM information_schema.columns
WHERE table_schema IN {quoted(scope)}{differing}
ORDER BY table_schema::text COLLATE "C", table_name::text COLLATE "C", column_name::text COLLATE "C"'''


check(pg_class(MAIN))
check(pg_attribute(MAIN))
check(pg_attribute_types(MAIN, """
  AND attname::text @NOT IN ('c_char', 'c_numeric_plain', 'c_varchar', 'city', 'code', 'email', 'sku', 'status',
    'total')"""), "typmod", "numeric", "aggregate_type")
check(pg_type(MAIN))
check(f'''SELECT oid, {TYPE_COLUMNS}
FROM pg_catalog.pg_type
WHERE oid < 10000
  AND oid @NOT IN (3615, 3645)
ORDER BY oid''', "text_search")
check(f'''SELECT {TYPE_COLUMNS}
FROM pg_catalog.pg_type
WHERE oid >= 10000 AND oid < 16384
  AND typname::text @NOT IN ('columns', '_columns')
ORDER BY typnamespace::regnamespace::text COLLATE "C", typname::text COLLATE "C"''', "quoted_keywords")
check(f'''SELECT indexrelid::regclass::text AS indexrelid, indrelid::regclass::text AS indrelid, indnatts, indnkeyatts,
  indisunique, indnullsnotdistinct, indisprimary, indisexclusion, indimmediate, indisclustered, indisvalid,
  indcheckxmin, indisready, indislive, indisreplident, array_to_string(indkey::int2[], ' ') AS indkey,
  array_to_string(indcollation::oid[], ' ') AS indcollation, array_to_string(indoption::int2[], ' ') AS indoption,
  pg_catalog.pg_get_expr(indexprs, indrelid) AS indexprs, pg_catalog.pg_get_expr(indpred, indrelid) AS indpred
FROM pg_catalog.pg_index
WHERE indrelid IN {relations(MAIN)}
  AND indexrelid::regclass::text @NOT IN ('orders_amount_desc_idx')
ORDER BY indexrelid::regclass::text COLLATE "C"''', "index_desc")
check(pg_constraint(MAIN, """
  AND (conname, conrelid::regclass::text, contypid::regtype::text) @NOT IN (
    ('child_tab_unique_id_fkey', 'app.child_tab', '-'), ('order_items_order_id_fkey', 'order_items', '-'),
    ('orders_amount_check', 'orders', '-'), ('orders_customer_id_fkey', 'orders', '-'),
    ('orders_status_check', 'orders', '-'), ('unique_tab_qty_check', 'app.unique_tab', '-'))"""),
      "deparse", "fk_operators")
check(f"""SELECT adrelid::regclass::text AS adrelid, adnum, pg_catalog.pg_get_expr(adbin, adrelid) AS adbin,
  pg_catalog.pg_get_expr(adbin, adrelid, true) AS adbin_pretty
FROM pg_catalog.pg_attrdef
WHERE adrelid IN {relations(MAIN)}
  AND (adrelid::regclass::text, (adnum)::text) @NOT IN (('app.all_types', '8'), ('customers', '1'), ('customers', '5'),
    ('orders', '1'), ('orders', '4'), ('orders', '6'), ('orders', '7'))
ORDER BY adrelid::regclass::text COLLATE "C", adnum""", "deparse")
check(pg_description(MAIN))
check(pg_depend(MAIN, """
WHERE (q.classid, q.objid, (q.objsubid)::text, q.refclassid, q.refobjid, (q.refobjsubid)::text, (q.deptype)::text) @NOT IN (
    ('pg_attrdef', 'customers default 5', '0', 'pg_type', 'app.mood', '0', 'n'),
    ('pg_trigger', 'internal trigger on app.child_tab', '0', 'pg_constraint', 'child_tab_unique_id_fkey on app.child_tab', '0', 'i'),
    ('pg_trigger', 'internal trigger on app.unique_tab', '0', 'pg_constraint', 'child_tab_unique_id_fkey on app.child_tab', '0', 'i'),
    ('pg_trigger', 'internal trigger on customers', '0', 'pg_constraint', 'orders_customer_id_fkey on orders', '0', 'i'),
    ('pg_trigger', 'internal trigger on order_items', '0', 'pg_constraint', 'order_items_order_id_fkey on order_items', '0', 'i'),
    ('pg_trigger', 'internal trigger on orders', '0', 'pg_constraint', 'order_items_order_id_fkey on order_items', '0', 'i'),
    ('pg_trigger', 'internal trigger on orders', '0', 'pg_constraint', 'orders_customer_id_fkey on orders', '0', 'i'))"""),
      "ri_triggers", "fk_operators")
check(pg_shdepend(MAIN))
check(pg_sequence(MAIN, """
  AND seqrelid::regclass::text @NOT IN ('customers_id_seq')"""), "serial_sequence")
check(f'''SELECT enumtypid::regtype::text AS enumtypid, enumsortorder, enumlabel
FROM pg_catalog.pg_enum
WHERE enumtypid IN (SELECT oid FROM pg_catalog.pg_type WHERE typnamespace IN {oids(MAIN)})
ORDER BY enumtypid::regtype::text COLLATE "C", enumlabel::text COLLATE "C"''')
check(f'''SELECT rulename, ev_class::regclass::text AS ev_class, ev_type, ev_enabled, is_instead
FROM pg_catalog.pg_rewrite
WHERE ev_class IN {relations(MAIN)}
ORDER BY ev_class::regclass::text COLLATE "C", rulename::text COLLATE "C"''')
check(pg_trigger(MAIN))
check(pg_proc(MAIN))
check(f"""SELECT pg_catalog.pg_get_userbyid(datdba) AS datdba, encoding, datlocprovider, datistemplate, datallowconn,
  dathasloginevt, datconnlimit, dattablespace, {acl("datacl")}
FROM pg_catalog.pg_database
WHERE datname = current_database()""")
check('''SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolconnlimit,
  rolpassword, rolvaliduntil, rolbypassrls, rolconfig
FROM pg_catalog.pg_roles
WHERE rolname IN ('postgres', 'conf_reader')
ORDER BY rolname::text COLLATE "C"''')
check("""SELECT oid, amname, amhandler::text AS amhandler, amtype
FROM pg_catalog.pg_am
WHERE amname NOT IN ('secondary', 'inverted', 'iresearch')
  AND oid @NOT IN (403, 405, 783, 2742, 3580, 4000)
ORDER BY oid""", "index_am")
check('''SELECT rngtypid::regtype::text AS rngtypid, rngsubtype::regtype::text AS rngsubtype,
  rngmultitypid::regtype::text AS rngmultitypid, rngcollation, rngsubopc, rngcanonical::text AS rngcanonical,
  rngsubdiff::text AS rngsubdiff
FROM pg_catalog.pg_range
ORDER BY rngtypid::regtype::text COLLATE "C"''')
check(f"""SELECT inhrelid::regclass::text AS inhrelid, inhparent::regclass::text AS inhparent, inhseqno, inhdetachpending
FROM pg_catalog.pg_inherits
WHERE inhrelid IN {relations(MAIN)} OR inhparent IN {relations(MAIN)}""")

check(f'''SELECT {db("catalog_name")}, schema_name, schema_owner, {db("default_character_set_catalog")},
  default_character_set_schema, default_character_set_name, sql_path
FROM information_schema.schemata
WHERE (schema_name IN {quoted(MAIN)} OR schema_name IN ('pg_catalog', 'information_schema'))
  AND schema_name @NOT IN ('public')
ORDER BY schema_name::text COLLATE "C"''', "owner")
check(f'''SELECT {db("table_catalog")}, table_schema, table_name, table_type, self_referencing_column_name,
  reference_generation, {db("user_defined_type_catalog")}, user_defined_type_schema, user_defined_type_name,
  is_insertable_into, is_typed, commit_action
FROM information_schema.tables
WHERE table_schema IN {quoted(MAIN)}
  AND (table_schema, table_name) @NOT IN (('public', 'active_customers'))
ORDER BY table_schema::text COLLATE "C", table_name::text COLLATE "C"''', "updatable_views")
check(columns(MAIN, """
  AND (table_schema, table_name, column_name) @NOT IN (('public', 'active_customers', 'id'),
    ('public', 'active_customers', 'email'), ('public', 'active_customers', 'name'),
    ('app', 'all_types', 'c_numeric_plain'), ('app', 'order_totals', 'total'))"""),
      "updatable_views", "numeric", "aggregate_type")
check(column_types(MAIN, """
  AND (table_name, column_name) @NOT IN (('all_types', 'c_char'), ('all_types', 'c_text'), ('all_types', 'c_varchar'),
    ('unique_tab', 'code'), ('active_customers', 'email'), ('customers', 'email'), ('customers', 'id'),
    ('customers', 'mood'), ('order_items', 'sku'), ('orders', 'id'), ('orders', 'ordered_on'), ('orders', 'status'),
    ('orders', 'ticket'))"""), "typmod", "deparse")
check(f'''SELECT {db("table_catalog")}, table_schema, table_name, check_option, is_trigger_updatable,
  is_trigger_deletable, is_trigger_insertable_into
FROM information_schema.views
WHERE table_schema IN {quoted(MAIN)}
ORDER BY table_schema::text COLLATE "C", table_name::text COLLATE "C"''')
check(f'''SELECT table_schema, table_name, {spaced("view_definition::text", "view_definition")}, is_updatable,
  is_insertable_into
FROM information_schema.views
WHERE table_schema IN {quoted(MAIN)}
ORDER BY table_schema::text COLLATE "C", table_name::text COLLATE "C"''', "deparse", "updatable_views")
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, {db("table_catalog")},
  table_schema, table_name, constraint_type, is_deferrable, initially_deferred, enforced, nulls_distinct
FROM information_schema.table_constraints
WHERE table_schema IN {quoted(MAIN)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C", table_name::text COLLATE "C"''')
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, {db("table_catalog")},
  table_schema, table_name, column_name, ordinal_position, position_in_unique_constraint
FROM information_schema.key_column_usage
WHERE table_schema IN {quoted(MAIN)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C", table_name::text COLLATE "C",
  column_name::text COLLATE "C"''')
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, {db("unique_constraint_catalog")},
  unique_constraint_schema, unique_constraint_name, match_option, update_rule, delete_rule
FROM information_schema.referential_constraints
WHERE constraint_schema IN {quoted(MAIN)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C"''')
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_schema IN {quoted(MAIN)}
  AND (constraint_schema, constraint_name) @NOT IN (('app', 'unique_tab_qty_check'), ('public', 'orders_amount_check'),
    ('public', 'orders_status_check'))
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C"''', "deparse")
check(f'''SELECT {db("table_catalog")}, table_schema, table_name, column_name, {db("constraint_catalog")},
  constraint_schema, constraint_name
FROM information_schema.constraint_column_usage
WHERE table_schema IN {quoted(MAIN)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C", table_schema::text COLLATE "C",
  table_name::text COLLATE "C", column_name::text COLLATE "C"''')
check(f'''SELECT {db("table_catalog")}, table_schema, table_name, {db("constraint_catalog")}, constraint_schema,
  constraint_name
FROM information_schema.constraint_table_usage
WHERE table_schema IN {quoted(MAIN)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C", table_schema::text COLLATE "C",
  table_name::text COLLATE "C"''')
check(f'''SELECT {db("sequence_catalog")}, sequence_schema, sequence_name, data_type, numeric_precision,
  numeric_precision_radix, numeric_scale, start_value, minimum_value, maximum_value, increment, cycle_option
FROM information_schema.sequences
WHERE sequence_schema IN {quoted(MAIN)}
  AND (sequence_schema, sequence_name) @NOT IN (('public', 'customers_id_seq'))
ORDER BY sequence_schema::text COLLATE "C", sequence_name::text COLLATE "C"''', "serial_sequence")
check(f'''SELECT grantor, grantee, {db("table_catalog")}, table_schema, table_name, privilege_type, is_grantable,
  with_hierarchy
FROM information_schema.table_privileges
WHERE table_schema IN {quoted(MAIN)}
ORDER BY grantee::text COLLATE "C", table_schema::text COLLATE "C", table_name::text COLLATE "C",
  privilege_type::text COLLATE "C"''')
check(f'''SELECT grantor, grantee, {db("table_catalog")}, table_schema, table_name, column_name, privilege_type,
  is_grantable
FROM information_schema.column_privileges
WHERE table_schema IN {quoted(MAIN)} AND grantee = 'conf_reader'
ORDER BY grantee::text COLLATE "C", table_schema::text COLLATE "C", table_name::text COLLATE "C",
  column_name::text COLLATE "C", privilege_type::text COLLATE "C"''')
check(f"""SELECT {db("specific_catalog")}, specific_schema,
  regexp_replace(specific_name::text, '_[0-9]+$', '') AS specific_name, {db("routine_catalog")}, routine_schema,
  routine_name, routine_type, {db("module_catalog")}, module_schema, module_name, {db("udt_catalog")}, udt_schema,
  udt_name, data_type, character_maximum_length, character_octet_length, {db("character_set_catalog")},
  character_set_schema, character_set_name, {db("collation_catalog")}, collation_schema, collation_name,
  numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision,
  {db("type_udt_catalog")}, type_udt_schema, type_udt_name, {db("scope_catalog")}, scope_schema, scope_name,
  maximum_cardinality, dtd_identifier, routine_body, {spaced("routine_definition::text", "routine_definition")},
  external_name, external_language, parameter_style, is_deterministic, sql_data_access, is_null_call, sql_path,
  schema_level_routine, max_dynamic_result_sets, is_user_defined_cast, is_implicitly_invocable, security_type,
  {db("to_sql_specific_catalog")}, to_sql_specific_schema,
  regexp_replace(to_sql_specific_name::text, '_[0-9]+$', '') AS to_sql_specific_name, as_locator, created,
  last_altered, new_savepoint_level, is_udt_dependent, result_cast_from_data_type, result_cast_as_locator,
  result_cast_char_max_length, result_cast_char_octet_length, {db("result_cast_char_set_catalog")},
  result_cast_char_set_schema, result_cast_char_set_name, {db("result_cast_collation_catalog")},
  result_cast_collation_schema, result_cast_collation_name, result_cast_numeric_precision,
  result_cast_numeric_precision_radix, result_cast_numeric_scale, result_cast_datetime_precision,
  result_cast_interval_type, result_cast_interval_precision, {db("result_cast_type_udt_catalog")},
  result_cast_type_udt_schema, result_cast_type_udt_name, {db("result_cast_scope_catalog")},
  result_cast_scope_schema, result_cast_scope_name, result_cast_maximum_cardinality, result_cast_dtd_identifier
FROM information_schema.routines
WHERE routine_schema IN {quoted(MAIN)}""")
check(f"""SELECT {db("specific_catalog")}, specific_schema,
  regexp_replace(specific_name::text, '_[0-9]+$', '') AS specific_name, ordinal_position, parameter_mode, is_result,
  as_locator, parameter_name, data_type, character_maximum_length, character_octet_length,
  {db("character_set_catalog")}, character_set_schema, character_set_name, {db("collation_catalog")},
  collation_schema, collation_name, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision,
  interval_type, interval_precision, {db("udt_catalog")}, udt_schema, udt_name, {db("scope_catalog")}, scope_schema,
  scope_name, maximum_cardinality, dtd_identifier, parameter_default
FROM information_schema.parameters
WHERE specific_schema IN {quoted(MAIN)}""")
check(f'''SELECT {db("udt_catalog")}, udt_schema, udt_name, attribute_name, ordinal_position, attribute_default,
  is_nullable, {db("character_set_catalog")}, character_set_schema, character_set_name, {db("collation_catalog")},
  collation_schema, collation_name, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision,
  interval_type, interval_precision, {db("attribute_udt_catalog")}, attribute_udt_schema, {db("scope_catalog")},
  scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_derived_reference_attribute
FROM information_schema.attributes
WHERE udt_schema IN {quoted(MAIN)}
ORDER BY udt_schema::text COLLATE "C", udt_name::text COLLATE "C", attribute_name::text COLLATE "C"''')
check(f'''SELECT udt_schema, udt_name, attribute_name, data_type, character_maximum_length, character_octet_length,
  attribute_udt_name
FROM information_schema.attributes
WHERE udt_schema IN {quoted(MAIN)}
  AND attribute_name @NOT IN ('city')
ORDER BY udt_schema::text COLLATE "C", udt_name::text COLLATE "C", attribute_name::text COLLATE "C"''', "typmod")
check(f'''SELECT {db("view_catalog")}, view_schema, view_name, {db("table_catalog")}, table_schema, table_name
FROM information_schema.view_table_usage
WHERE view_schema IN {quoted(MAIN)}
ORDER BY view_schema::text COLLATE "C", view_name::text COLLATE "C", table_schema::text COLLATE "C",
  table_name::text COLLATE "C"''')
check(f'''SELECT {db("view_catalog")}, view_schema, view_name, {db("table_catalog")}, table_schema, table_name,
  column_name
FROM information_schema.view_column_usage
WHERE view_schema IN {quoted(MAIN)}
ORDER BY view_schema::text COLLATE "C", view_name::text COLLATE "C", table_schema::text COLLATE "C",
  table_name::text COLLATE "C", column_name::text COLLATE "C"''')
check(f'''SELECT {db("udt_catalog")}, udt_schema, udt_name, {db("table_catalog")}, table_schema, table_name,
  column_name
FROM information_schema.column_udt_usage
WHERE table_schema IN {quoted(MAIN)} AND udt_schema IN {quoted(MAIN)}
ORDER BY udt_schema::text COLLATE "C", udt_name::text COLLATE "C", table_schema::text COLLATE "C",
  table_name::text COLLATE "C", column_name::text COLLATE "C"''')

check("""SELECT
  pg_catalog.obj_description('information_schema'::regnamespace, 'pg_namespace') AS information_schema_description""")
check("""SELECT 'postgres'::regrole::oid AS postgres_oid,
  (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'pg_toast') AS pg_toast_schemas,
  pg_catalog.obj_description('pg_catalog'::regnamespace, 'pg_namespace') AS pg_catalog_description,
  pg_catalog.obj_description('public'::regnamespace, 'pg_namespace') AS public_description,
  pg_catalog.obj_description(23, 'pg_type') AS int4_description,
  pg_catalog.obj_description('int4in'::regproc, 'pg_proc') AS int4in_description""", "owner", "toast", "catalog_content")
check(f'''SELECT c.oid::regclass::text AS index_name,
  (SELECT a.amname::text FROM pg_catalog.pg_am a WHERE a.oid = c.relam) AS amname
FROM pg_catalog.pg_class c
WHERE c.relkind IN ('i', 'I') AND c.relnamespace IN {oids(MAIN)}
ORDER BY c.oid::regclass::text COLLATE "C"''', "index_am")
check("""SELECT 'pg_class'::regclass::oid AS pg_class, 'pg_catalog.pg_class'::regclass::text AS pg_class_qualified,
  1259::regclass::text AS by_oid, 'pg_type'::regclass::oid AS pg_type, 'pg_namespace'::regclass::oid AS pg_namespace,
  'pg_proc'::regclass::oid AS pg_proc, 'pg_attribute'::regclass::oid AS pg_attribute,
  'pg_constraint'::regclass::oid AS pg_constraint, 'pg_description'::regclass::oid AS pg_description,
  to_regclass('pg_class')::oid AS to_regclass, to_regclass('no_such_relation')::text AS to_regclass_missing""")
check("SELECT 'information_schema.columns'::regclass::text AS columns_view", "quoted_keywords")
check("""SELECT 'pg_catalog.array_in'::regproc::oid AS array_in_qualified,
  'pg_catalog.array_in'::regproc::text AS array_in_text, 'array_in'::regproc::oid AS array_in,
  'array_recv'::regproc::oid AS array_recv, 'int4in'::regproc::oid AS int4in,
  'int4in(cstring)'::regprocedure::oid AS int4in_signature,
  'array_in(cstring,oid,integer)'::regprocedure::oid AS array_in_signature,
  'array_in(cstring,oid,integer)'::regprocedure::text AS array_in_signature_text, 42::regproc::text AS proc_42,
  750::regproc::text AS proc_750, 2400::regprocedure::text AS procedure_2400,
  to_regproc('array_in')::oid AS to_regproc, to_regprocedure('int4in(cstring)')::oid AS to_regprocedure,
  to_regproc('no_such_function')::text AS to_regproc_missing""")
check("SELECT 'int4in'::regprocedure::text AS int4in_procedure")
check("SELECT 'int4in'::regprocedure::oid AS int4in_procedure_oid")
check("""SELECT 'int4'::regtype::oid AS int4, 'integer'::regtype::text AS integer,
  'character varying'::regtype::oid AS character_varying, 'varchar'::regtype::text AS varchar,
  'numeric(10,2)'::regtype::text AS numeric_typmod, 'int4[]'::regtype::oid AS int4_array,
  '_text'::regtype::text AS text_array, 23::regtype::text AS type_23, 1043::regtype::text AS type_1043,
  1007::regtype::text AS type_1007, to_regtype('int4')::oid AS to_regtype,
  to_regtype('timestamp with time zone')::text AS to_regtype_tz,
  to_regtype('no_such_type')::text AS to_regtype_missing""")
check("""SELECT 'pg_catalog'::regnamespace::oid AS pg_catalog, 'public'::regnamespace::oid AS public,
  'information_schema'::regnamespace::text AS information_schema, 11::regnamespace::text AS namespace_11""")
check("SELECT 'english'::regconfig::text AS english", "text_search")
check("""SELECT v.typid, v.typmod, pg_catalog.format_type(v.typid::oid, v.typmod) AS formatted
FROM (VALUES (1, 23, -1), (2, 23, NULL), (3, 1700, 655366), (4, 1700, -1), (5, 1043, 54), (6, 1043, -1), (7, 1042, 7),
  (8, 1114, -1), (9, 1114, 3), (10, 1184, -1), (11, 1083, 3), (12, 1186, -1), (13, 1007, -1), (14, 1015, 54),
  (15, 1009, -1), (16, 2950, -1), (17, 114, -1), (18, 3802, -1), (19, 17, -1), (20, 16, -1), (21, 700, -1),
  (22, 701, -1), (23, 26, -1), (24, 2205, -1), (25, 19, -1), (26, 18, -1), (27, 194, -1), (28, 1033, -1),
  (29, 22, -1), (30, 30, -1), (31, 2277, -1), (32, 0, -1), (33, 999999, -1)) v(n, typid, typmod)
ORDER BY v.n""")
check("""SELECT pg_catalog.pg_table_is_visible(1259) AS pg_class_visible, pg_catalog.pg_type_is_visible(23) AS int4_visible,
  pg_catalog.pg_function_is_visible('int4in'::regproc) AS int4in_visible, pg_catalog.pg_get_userbyid(0) AS user_0,
  pg_catalog.pg_encoding_to_char(6) AS encoding_6, pg_catalog.current_schemas(true) AS schemas_implicit,
  pg_catalog.current_schemas(false) AS schemas, current_schema() AS "current_schema",
  pg_catalog.has_database_privilege(current_database(), 'CREATE') AS can_create,
  pg_catalog.pg_get_expr(NULL, 0) AS expr_null, pg_catalog.pg_get_constraintdef(0) AS constraintdef_0,
  pg_catalog.pg_get_indexdef(0) AS indexdef_0, pg_catalog.pg_get_viewdef(0) AS viewdef_0,
  pg_catalog.col_description(0, 1) AS col_description_0,
  (SELECT count(*) FROM pg_catalog.pg_description WHERE classoid = 1259 AND description IN ('Customer master', 'Login email', 'Order amount', 'Only active customers', 'Orders by customer', 'Ticket numbers')) AS class_descriptions,
  (SELECT count(*) FROM pg_catalog.pg_description WHERE classoid = 'pg_class'::regclass AND description IN ('Customer master', 'Login email', 'Order amount', 'Only active customers', 'Orders by customer', 'Ticket numbers')) AS class_descriptions_regclass,
  (SELECT count(*) FROM pg_catalog.pg_depend WHERE classid = 1259 AND refclassid = 1259 AND refobjid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname IN ('customers', 'orders')) AND objid NOT IN (SELECT reltoastrelid FROM pg_catalog.pg_class)) AS table_dependents""")
check("SELECT pg_catalog.pg_get_serial_sequence('no_such_table', 'id') AS serial_missing_table")

RELATIONS = {
    "app.address": "type",
    "app.all_types": "table",
    "app.all_types_pkey": "index",
    "app.all_types_varchar_idx": "index",
    "app.child_tab": "table",
    "app.child_tab_pkey": "index",
    "app.json_tab": "table",
    "app.json_tab_pkey": "index",
    "app.order_totals": "view",
    "app.ticket_seq": "sequence",
    "app.unique_tab": "table",
    "app.unique_tab_code_key": "index",
    "app.unique_tab_pkey": "index",
    "public.active_customers": "view",
    "public.customers": "table",
    "public.customers_email_key": "index",
    "public.customers_id_seq": "sequence",
    "public.customers_name_email_idx": "index",
    "public.customers_pkey": "index",
    "public.order_items": "table",
    "public.order_items_pkey": "index",
    "public.order_items_sku_idx": "index",
    "public.orders": "table",
    "public.orders_amount_desc_idx": "index",
    "public.orders_customer_idx": "index",
    "public.orders_id_seq": "sequence",
    "public.orders_pkey": "index",
    "public.plain_seq": "sequence",
}
DESCENDING = {"public.orders_amount_desc_idx"}
DIFFERING_COLUMNS = {
    "app.address": (("city",), ("typmod",)),
    "app.all_types": (("c_char", "c_numeric_plain", "c_varchar"), ("typmod", "numeric")),
    "app.order_totals": (("total",), ("aggregate_type",)),
    "app.unique_tab": (("code",), ("typmod",)),
    "public.active_customers": (("email",), ("typmod",)),
    "public.customers": (("email",), ("typmod",)),
    "public.order_items": (("sku",), ("typmod",)),
    "public.orders": (("status",), ("typmod",)),
}


def indexdefs(relation):
    return [f"replace(pg_catalog.pg_get_indexdef('{relation}'::regclass), ' USING secondary ', ' USING btree ') AS indexdef",
            f"replace(pg_catalog.pg_get_indexdef('{relation}'::regclass, 0, true), ' USING secondary ', ' USING btree ') "
            "AS indexdef_pretty"]


for relation, kind in RELATIONS.items():
    schema, name = relation.split(".")
    selected = [f"'{relation}'::regclass::text AS regclass",
                f"'{relation}'::regclass::oid = (SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = '{name}' "
                f"AND c.relnamespace = '{schema}'::regnamespace) AS same_oid",
                f"to_regclass('{relation}')::text AS to_regclass",
                f"pg_catalog.pg_table_is_visible('{relation}'::regclass) AS visible",
                f"pg_catalog.obj_description('{relation}'::regclass, 'pg_class') AS description",
                f"pg_catalog.obj_description('{relation}'::regclass) AS any_description"]
    if kind in ("table", "view", "sequence"):
        selected += [f"pg_catalog.has_table_privilege('conf_reader', '{relation}', 'SELECT') AS reader_select",
                     f"pg_catalog.has_table_privilege('{relation}', 'INSERT') AS own_insert"]
    if kind == "index":
        selected += [] if relation in DESCENDING else indexdefs(relation)
        selected.append(f"pg_catalog.pg_get_indexdef('{relation}'::regclass, 1, true) AS indexdef_column")
    check("SELECT " + ",\n  ".join(selected))
    if relation in DESCENDING:
        check("SELECT " + ",\n  ".join(indexdefs(relation)), "index_desc")
    if kind == "view":
        check(f"""SELECT {spaced(f"pg_catalog.pg_get_viewdef('{relation}'::regclass)", "viewdef")},
  {spaced(f"pg_catalog.pg_get_viewdef('{relation}'::regclass, true)", "viewdef_pretty")},
  {spaced(f"pg_catalog.pg_get_viewdef('{relation}')", "viewdef_by_name")}""", "deparse")
        check(f"""SELECT pg_catalog.pg_get_viewdef('{relation}', true) = pg_catalog.pg_get_viewdef('{relation}'::regclass, true) AS by_name_pretty,
  pg_catalog.pg_get_viewdef('{relation}', false) = pg_catalog.pg_get_viewdef('{relation}'::regclass, false) AS by_name_plain""")

for relation, kind in RELATIONS.items():
    if kind in ("index", "sequence"):
        continue
    differing, causes = DIFFERING_COLUMNS.get(relation, ((), ()))
    selected = ["a.attname::text AS col", f"pg_catalog.col_description('{relation}'::regclass, a.attnum) AS description",
                "pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype"]
    if kind == "table":
        selected += [f"pg_catalog.pg_get_serial_sequence('{relation}', a.attname) AS serial_sequence",
                     f"pg_catalog.has_column_privilege('conf_reader', '{relation}', a.attname, 'SELECT') AS reader_select"]
    narrowing = f"\n  AND a.attname::text @NOT IN {quoted(differing)}" if differing else ""
    check(f"""SELECT {",\n  ".join(selected)}
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '{relation}'::regclass AND a.attnum > 0 AND NOT a.attisdropped{narrowing}
ORDER BY a.attnum""", *causes)

check(f'''SELECT * FROM (
SELECT con.conname::text AS conname,
  CASE WHEN con.conrelid <> 0 THEN con.conrelid::regclass::text ELSE con.contypid::regtype::text END AS owner,
  pg_catalog.pg_get_constraintdef(con.oid) AS def, pg_catalog.pg_get_constraintdef(con.oid, true) AS def_pretty,
  pg_catalog.obj_description(con.oid, 'pg_constraint') AS description
FROM pg_catalog.pg_constraint con
WHERE con.connamespace IN {oids(MAIN)}
) q
WHERE (q.conname, q.owner) @NOT IN (('order_items_qty_check', 'order_items'), ('orders_amount_check', 'orders'),
    ('orders_status_check', 'orders'), ('unique_tab_qty_check', 'app.unique_tab'))
ORDER BY q.conname::text COLLATE "C", q.owner::text COLLATE "C"''', "deparse")

TYPES = sorted({"app.mood", "app._mood"} | {
    f"{schema}.{prefix}{name}" for schema, name in (relation.split(".") for relation, kind in RELATIONS.items()
                                                   if kind in ("table", "view", "type"))
    for prefix in ("", "_")})
for name in TYPES:
    schema, typname = name.split(".")
    check(f"""SELECT '{name}'::regtype::text AS regtype,
  '{name}'::regtype::oid = (SELECT t.oid FROM pg_catalog.pg_type t WHERE t.typname = '{typname}' AND t.typnamespace = '{schema}'::regnamespace) AS same_oid,
  to_regtype('{name}')::text AS to_regtype, to_regtype('{typname}')::text AS to_regtype_unqualified,
  pg_catalog.pg_type_is_visible('{name}'::regtype) AS visible,
  pg_catalog.format_type('{name}'::regtype, NULL) AS format_null,
  pg_catalog.format_type('{name}'::regtype, -1) AS format_default,
  pg_catalog.obj_description('{name}'::regtype, 'pg_type') AS description""")

check("""SELECT 'app.add_one(integer)'::regprocedure::text AS regprocedure,
  'app.add_one(integer)'::regprocedure::oid = (SELECT p.oid FROM pg_catalog.pg_proc p WHERE p.proname = 'add_one' AND p.pronamespace = 'app'::regnamespace) AS same_oid,
  'app.add_one'::regproc::text AS regproc, to_regprocedure('app.add_one(integer)')::text AS to_regprocedure,
  pg_catalog.pg_function_is_visible('app.add_one(integer)'::regprocedure) AS visible,
  pg_catalog.pg_get_function_arguments('app.add_one(integer)'::regprocedure) AS arguments,
  pg_catalog.pg_get_function_identity_arguments('app.add_one(integer)'::regprocedure) AS identity_arguments,
  pg_catalog.pg_get_function_result('app.add_one(integer)'::regprocedure) AS result,
  pg_catalog.obj_description('app.add_one(integer)'::regprocedure, 'pg_proc') AS description,
  """ + spaced("pg_catalog.pg_get_functiondef('app.add_one(integer)'::regprocedure)", "functiondef"))
check(f'''SELECT r.ev_class::regclass::text AS rel, r.rulename::text AS rulename
FROM pg_catalog.pg_rewrite r
WHERE r.ev_class IN {relations(MAIN)}
ORDER BY r.ev_class::regclass::text COLLATE "C", r.rulename::text COLLATE "C"''')
check(f'''SELECT r.ev_class::regclass::text AS rel, r.rulename::text AS rulename,
  {spaced("pg_catalog.pg_get_ruledef(r.oid)", "def")}, {spaced("pg_catalog.pg_get_ruledef(r.oid, true)", "def_pretty")}
FROM pg_catalog.pg_rewrite r
WHERE r.ev_class IN {relations(MAIN)}
ORDER BY r.ev_class::regclass::text COLLATE "C", r.rulename::text COLLATE "C"''', "deparse")
check("""SELECT 'app'::regnamespace::text AS app, 'public'::regnamespace::text AS public,
  pg_catalog.has_schema_privilege('conf_reader', 'app', 'USAGE') AS reader_app_usage,
  pg_catalog.has_schema_privilege('conf_reader', 'public', 'USAGE') AS reader_public_usage,
  pg_catalog.obj_description('app'::regnamespace, 'pg_namespace') AS app_description,
  'conf_reader'::regrole::text AS role, pg_catalog.pg_get_userbyid('conf_reader'::regrole) AS role_by_id,
  pg_catalog.pg_has_role('conf_reader', 'USAGE') AS reader_member""")


def asyncpg_types(roots):
    info = ("SELECT t.oid AS oid, ns.nspname AS ns, t.typname AS name, t.typtype AS kind, (CASE WHEN t.typtype = 'd' "
            "THEN (WITH RECURSIVE typebases(oid, depth) AS ( SELECT t2.typbasetype AS oid, 0 AS depth FROM pg_type t2 "
            "WHERE t2.oid = t.oid UNION ALL SELECT t2.typbasetype AS oid, tb.depth + 1 AS depth FROM pg_type t2, "
            "typebases tb WHERE tb.oid = t2.oid AND t2.typbasetype != 0 ) SELECT oid FROM typebases ORDER BY depth DESC "
            "LIMIT 1) ELSE NULL END) AS basetype, t.typelem AS elemtype, elem_t.typdelim AS elemdelim, COALESCE( "
            "range_t.rngsubtype, multirange_t.rngsubtype) AS range_subtype, (CASE WHEN t.typtype = 'c' THEN (SELECT "
            "array_agg(ia.atttypid ORDER BY ia.attnum) FROM pg_attribute ia INNER JOIN pg_class c ON (ia.attrelid = "
            "c.oid) WHERE ia.attnum > 0 AND NOT ia.attisdropped AND c.reltype = t.oid) ELSE NULL END) AS attrtypoids, "
            "(CASE WHEN t.typtype = 'c' THEN (SELECT array_agg(ia.attname::text ORDER BY ia.attnum) FROM pg_attribute "
            "ia INNER JOIN pg_class c ON (ia.attrelid = c.oid) WHERE ia.attnum > 0 AND NOT ia.attisdropped AND "
            "c.reltype = t.oid) ELSE NULL END) AS attrnames FROM pg_catalog.pg_type AS t INNER JOIN "
            "pg_catalog.pg_namespace ns ON ( ns.oid = t.typnamespace) LEFT JOIN pg_type elem_t ON ( t.typlen = -1 AND "
            "t.typelem != 0 AND t.typelem = elem_t.oid ) LEFT JOIN pg_range range_t ON ( t.oid = range_t.rngtypid ) "
            "LEFT JOIN pg_range multirange_t ON ( t.oid = multirange_t.rngmultitypid )")
    columns = "ti.oid, ti.ns, ti.name, ti.kind, ti.basetype, ti.elemtype, ti.elemdelim, ti.range_subtype, ti.attrtypoids, ti.attrnames"
    return ("WITH RECURSIVE typeinfo_tree( oid, ns, name, kind, basetype, elemtype, elemdelim, range_subtype, "
            f"attrtypoids, attrnames, depth) AS ( SELECT {columns}, 0 FROM ( {info} ) AS ti WHERE ti.oid = "
            f"any(ARRAY[{roots}]::oid[]) UNION ALL SELECT {columns}, tt.depth + 1 FROM ( {info} ) ti, typeinfo_tree tt "
            "WHERE (tt.elemtype IS NOT NULL AND ti.oid = tt.elemtype) OR (tt.attrtypoids IS NOT NULL AND ti.oid = "
            "any(tt.attrtypoids)) OR (tt.range_subtype IS NOT NULL AND ti.oid = tt.range_subtype) OR (tt.basetype IS "
            "NOT NULL AND ti.oid = tt.basetype) ) SELECT DISTINCT *, basetype::regtype::text AS basetype_name, "
            "elemtype::regtype::text AS elemtype_name, range_subtype::regtype::text AS range_subtype_name FROM "
            "typeinfo_tree ORDER BY depth DESC")


def asyncpg_check(roots, differing, *causes):
    check(f'''SELECT q.oid::regtype::text AS oid, q.ns, q."name", q.kind, q.basetype, q.elemtype, q.elemdelim,
  q.range_subtype, q.attrtypoids, q.attrnames, q.depth, q.basetype_name, q.elemtype_name, q.range_subtype_name FROM (
{asyncpg_types(roots)}
) q{differing}
ORDER BY q.oid::regtype::text COLLATE "C", q.depth''', *causes)


asyncpg_check("'app.mood'::regtype::oid, 'app.address'::regtype::oid, 1007", """
WHERE q."name" @NOT IN ('address', 'varchar')""", "typmod")
check(r'''SELECT q.relname, q.attrelid::regclass::text AS attrelid, q.attname, q.atttypid::regtype::text AS atttypid, q.attlen,
  q.attnum, q.atttypmod, q.attndims, q.attbyval, q.attalign, q.attstorage, q.attcompression, q.attnotnull,
  q.atthasdef, q.atthasmissing, q.attidentity, q.attgenerated, q.attisdropped, q.attislocal, q.attinhcount,
  q.attcollation, q.attstattarget,
  (SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE "C") FROM unnest(q.attacl) x) AS attacl, q.attoptions,
  q.attfdwoptions, (q.attmissingval)::text AS attmissingval, q.def_value, q.description, q.objid FROM (
SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) LEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid WHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.relnamespace='app'::regnamespace::oid ORDER BY a.attnum
) q
WHERE @NOT (q.attnum < 1 OR (q.relname, q.attname) IN (('all_types', 'c_char'), ('all_types', 'c_numeric_plain'),
    ('all_types', 'c_text'), ('all_types', 'c_varchar'), ('order_totals', 'total'), ('unique_tab', 'code')))
ORDER BY q.relname::text COLLATE "C", q.attname::text COLLATE "C"''', "typmod", "numeric", "aggregate_type", "deparse",
      "system_columns")

DBEAVER_CONSTRAINTS = ("SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, case when "
                       "c.contype='c' then \"substring\"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy FROM "
                       "pg_catalog.pg_constraint c INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid LEFT OUTER JOIN "
                       "pg_catalog.pg_class rt ON rt.oid=c.confrelid LEFT OUTER JOIN pg_catalog.pg_description d ON "
                       "d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass WHERE "
                       "t.relnamespace='public'::regnamespace::oid ORDER BY c.oid")
DBEAVER_CONSTRAINT_COLUMNS = ", ".join(f"c{n}" for n in range(1, 34))
check(f'''SELECT coalesce((SELECT x.conname::text FROM pg_catalog.pg_constraint x WHERE x.oid = q.c1), q.c1::text) AS oid,
  coalesce((SELECT x.conname::text FROM pg_catalog.pg_constraint x WHERE x.oid = q.c2), q.c2::text) AS oid,
  q.c3 AS conname, q.c4 AS connamespace, q.c5 AS contype, q.c6 AS condeferrable, q.c7 AS condeferred,
  q.c8 AS conenforced, q.c9 AS convalidated, q.c10::regclass::text AS conrelid, q.c11 AS contypid,
  q.c12::regclass::text AS conindid, q.c13 AS conparentid, q.c14::regclass::text AS confrelid,
  translate((q.c15)::text, ' ', '_') AS confupdtype, translate((q.c16)::text, ' ', '_') AS confdeltype,
  translate((q.c17)::text, ' ', '_') AS confmatchtype, q.c18 AS conislocal, q.c19 AS coninhcount,
  q.c20 AS connoinherit, q.c21 AS conperiod, q.c22 AS conkey, q.c23 AS confkey, q.c27 AS confdelsetcols,
  q.c28 AS conexclop, (q.c29) IS NOT NULL AS conbin, q.c30 AS tabrelname, q.c31 AS refnamespace, q.c32 AS description FROM (
{DBEAVER_CONSTRAINTS}
) q({DBEAVER_CONSTRAINT_COLUMNS})
ORDER BY q.c3::text COLLATE "C"''')
check(f'''SELECT q.c3 AS conname, q.c24 AS conpfeqop, q.c25 AS conppeqop, q.c26 AS conffeqop, q.c33 AS consrc_copy FROM (
{DBEAVER_CONSTRAINTS}
) q({DBEAVER_CONSTRAINT_COLUMNS})
WHERE q.c3 @NOT IN ('order_items_order_id_fkey', 'orders_amount_check', 'orders_customer_id_fkey', 'orders_status_check')
ORDER BY q.c3::text COLLATE "C"''', "deparse", "fk_operators")
check('''SELECT q.c1::regnamespace::text AS oid, q.c2::regnamespace::text AS oid, q.c3 AS nspname,
  pg_catalog.pg_get_userbyid(q.c4) AS nspowner,
  (SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE "C") FROM unnest(q.c5) x) AS nspacl,
  CASE WHEN q.c3 IN ('pg_catalog', 'public', 'information_schema') THEN NULL ELSE q.c6 END AS description FROM (
SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n left outer join pg_catalog.pg_description d on d.objoid=n.oid and d.objsubid=0 and d.classoid='pg_namespace'::regclass WHERE n.nspname NOT LIKE 'pg_temp%' AND n.nspname NOT LIKE 'pg_toast_temp%' ORDER BY nspname
) q(c1, c2, c3, c4, c5, c6)
WHERE q.c3 IS DISTINCT FROM 'pg_toast'
  AND q.c3 @NOT IN ('public')
ORDER BY q.c3::text COLLATE "C"''', "owner")
check('''SELECT q.c1::regclass::text AS oid, q.c2::regclass::text AS oid, q.c3 AS relname,
  q.c4::regnamespace::text AS relnamespace, q.c5::regtype::text AS reltype, q.c6 AS reloftype,
  pg_catalog.pg_get_userbyid(q.c7) AS relowner, q.c8 AS relam, q.c10 AS reltablespace, q.c16 AS relhasindex,
  q.c17 AS relisshared, q.c18 AS relpersistence, q.c19 AS relkind, q.c20 AS relnatts, q.c21 AS relchecks,
  q.c22 AS relhasrules, q.c23 AS relhastriggers, q.c24 AS relhassubclass, q.c25 AS relrowsecurity,
  q.c26 AS relforcerowsecurity, q.c27 AS relispopulated, q.c28 AS relreplident, q.c29 AS relispartition,
  q.c30 AS relrewrite, (SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE "C") FROM unnest(q.c33) x) AS relacl,
  q.c34 AS reloptions, (q.c35) IS NOT NULL AS relpartbound, q.c36 AS description, q.c37 AS partition_expr,
  q.c38 AS partition_key FROM (
select c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr, pg_catalog.pg_get_partkeydef(c.oid) as partition_key from pg_catalog.pg_class c left outer join pg_catalog.pg_description d on d.objoid=c.oid and d.objsubid=0 and d.classoid='pg_class'::regclass where c.relnamespace='app'::regnamespace::oid and c.relkind not in ('i','I','c') order by c.relname
) q(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38)
ORDER BY q.c3::text COLLATE "C"''')
check('''SELECT q.c1::regtype::text AS oid, q.c2::regtype::text AS oid, q.c3 AS typname,
  q.c4::regnamespace::text AS typnamespace, pg_catalog.pg_get_userbyid(q.c5) AS typowner, q.c6 AS typlen,
  q.c7 AS typbyval, q.c8 AS typtype, q.c9 AS typcategory, q.c10 AS typispreferred, q.c11 AS typisdefined,
  q.c12 AS typdelim, q.c13::regclass::text AS typrelid, (q.c14)::text AS typsubscript,
  q.c15::regtype::text AS typelem, q.c16::regtype::text AS typarray, (q.c17)::text AS typinput,
  (q.c18)::text AS typoutput, (q.c19)::text AS typreceive, (q.c20)::text AS typsend, (q.c21)::text AS typmodin,
  (q.c22)::text AS typmodout, (q.c23)::text AS typanalyze, q.c24 AS typalign, q.c25 AS typstorage,
  q.c26 AS typnotnull, q.c27 AS typbasetype, q.c28 AS typtypmod, q.c29 AS typndims, q.c30 AS typcollation,
  (q.c31) IS NOT NULL AS typdefaultbin, q.c32 AS typdefault,
  (SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE "C") FROM unnest(q.c33) x) AS typacl, q.c34 AS relkind,
  q.c35 AS base_type_name, q.c36 AS description FROM (
SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description FROM pg_catalog.pg_type t LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid WHERE t.typname IS NOT NULL AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C') AND t.typnamespace = 'app'::regnamespace::oid
) q(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36)
ORDER BY q.c3::text COLLATE "C"''')
for schema, table, differing in (("app", "all_types", "('c_char', 'c_numeric_plain', 'c_varchar')"),
                                 ("public", "orders", "('status')")):
    check(f'''SELECT q.column_name, q.data_type, q.character_maximum_length, q.numeric_precision, q.numeric_scale FROM (
select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale from INFORMATION_SCHEMA.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position
) q
WHERE q.column_name @NOT IN {differing}
ORDER BY q.column_name::text COLLATE "C"''', "typmod")
check('''SELECT CASE WHEN q.database = current_database() THEN 'current_database' ELSE q.database END AS database, q."name",
  q."schema", q."type" FROM (
select current_database() as database, tablename as name, schemaname as schema, 'table' as type from pg_tables where schemaname ilike 'app' union all select current_database() as database, viewname as name, schemaname as schema, 'view' as type from pg_views where schemaname ilike 'app' union all select current_database() as database, matviewname as name, schemaname as schema, 'materialized_view' as type from pg_matviews where schemaname ilike 'app'
) q
ORDER BY q."name"::text COLLATE "C"''')
check('''SELECT q.nspname FROM (
select distinct nspname from pg_namespace where nspname not like 'pg_temp%' and nspname not like 'pg_toast_temp%'
) q
WHERE q.nspname IS DISTINCT FROM 'pg_toast'
ORDER BY q.nspname::text COLLATE "C"''')
check(r'''SELECT q.referenced_schema, q.referenced_name, q.dependent_schema, q.dependent_name FROM (
with relation as ( select pg_rewrite.ev_class as class, pg_rewrite.oid as id from pg_rewrite ), class as ( select oid as id, relname as name, relnamespace as schema, relkind as kind from pg_class ), dependency as ( select distinct pg_depend.objid as id, pg_depend.refobjid as ref from pg_depend ), schema as ( select pg_namespace.oid as id, pg_namespace.nspname as name from pg_namespace where nspname != 'information_schema' and nspname not like 'pg\_%' ), referenced as ( select relation.id AS id, referenced_class.name, referenced_class.schema, referenced_class.kind from relation join class as referenced_class on relation.class=referenced_class.id where referenced_class.kind in ('r', 'v', 'm') ), relationships as ( select referenced.name as referenced_name, referenced.schema as referenced_schema_id, dependent_class.name as dependent_name, dependent_class.schema as dependent_schema_id, referenced.kind as kind from referenced join dependency on referenced.id=dependency.id join class as dependent_class on dependency.ref=dependent_class.id where (referenced.name != dependent_class.name or referenced.schema != dependent_class.schema) ) select referenced_schema.name as referenced_schema, relationships.referenced_name as referenced_name, dependent_schema.name as dependent_schema, relationships.dependent_name as dependent_name from relationships join schema as dependent_schema on relationships.dependent_schema_id=dependent_schema.id join schema as referenced_schema on relationships.referenced_schema_id=referenced_schema.id group by referenced_schema, referenced_name, dependent_schema, dependent_name order by referenced_schema, referenced_name, dependent_schema, dependent_name
) q
ORDER BY q.referenced_schema::text COLLATE "C", q.referenced_name::text COLLATE "C",
  q.dependent_schema::text COLLATE "C", q.dependent_name::text COLLATE "C"''', "group_by")
check('''SELECT q.conname, q."array", q.contype, q."?column?", q.reloptions FROM (
SELECT c.conname, array( SELECT attname FROM unnest(c.conkey) WITH ORDINALITY cols(colid, arridx) JOIN pg_attribute AS ca ON cols.colid = ca.attnum WHERE ca.attrelid = c.conrelid ORDER BY cols.arridx ), c.contype, (SELECT fkc.relname || '.' || fka.attname FROM pg_attribute AS fka JOIN pg_class AS fkc ON fka.attrelid = fkc.oid WHERE fka.attrelid = c.confrelid AND fka.attnum = c.confkey[1]), cl.reloptions FROM pg_constraint AS c JOIN pg_class AS cl ON c.conrelid = cl.oid WHERE cl.relname = 'order_items' AND pg_catalog.pg_table_is_visible(cl.oid)
) q
ORDER BY q.conname::text COLLATE "C"''')
check("""SELECT q.id, q.customer_id, q.amount, q.status, q.note, q.ordered_on, q.ticket FROM (
SELECT * FROM "orders" LIMIT 1
) q""")
DJANGO_INDEXES = ("SELECT indexname, array_agg(attname ORDER BY arridx), indisunique, indisprimary, array_agg(ordering "
                  "ORDER BY arridx), amname, exprdef, s2.attoptions FROM ( SELECT c2.relname as indexname, idx.*, "
                  "attr.attname, am.amname, CASE WHEN idx.indexprs IS NOT NULL THEN pg_get_indexdef(idx.indexrelid) END "
                  "AS exprdef, CASE am.amname WHEN 'btree' THEN CASE (option & 1) WHEN 1 THEN 'DESC' ELSE 'ASC' END END "
                  "as ordering, c2.reloptions as attoptions FROM ( SELECT * FROM pg_index i, unnest(i.indkey, i.indoption) "
                  "WITH ORDINALITY koi(key, option, arridx) ) idx LEFT JOIN pg_class c ON idx.indrelid = c.oid LEFT JOIN "
                  "pg_class c2 ON idx.indexrelid = c2.oid LEFT JOIN pg_am am ON c2.relam = am.oid LEFT JOIN pg_attribute "
                  "attr ON attr.attrelid = c.oid AND attr.attnum = idx.key WHERE c.relname = 'orders' AND "
                  "pg_catalog.pg_table_is_visible(c.oid) ) s2 GROUP BY indexname, indisunique, indisprimary, amname, "
                  "exprdef, attoptions")
check(f'''SELECT q.c1 AS indexname, q.c2 AS array_agg, q.c3 AS indisunique, q.c4 AS indisprimary,
  replace(q.c6, 'secondary', 'btree') AS amname, q.c7 AS exprdef, q.c8 AS attoptions FROM (
{DJANGO_INDEXES}
) q(c1, c2, c3, c4, c5, c6, c7, c8)
ORDER BY q.c1::text COLLATE "C"''')
check(f'''SELECT q.c1 AS indexname, q.c5 AS array_agg FROM (
{DJANGO_INDEXES}
) q(c1, c2, c3, c4, c5, c6, c7, c8)
ORDER BY q.c1::text COLLATE "C"''', "index_am", "index_desc")
check("""SELECT q.attname FROM (
SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '"customers"'::regclass AND i.indisprimary
) q""")
check("""SELECT q.sequence_name, q.colname FROM (
SELECT s.relname AS sequence_name, a.attname AS colname FROM pg_class s JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass JOIN pg_attribute a ON d.refobjid = a.attrelid AND d.refobjsubid = a.attnum JOIN pg_class tbl ON tbl.oid = d.refobjid AND tbl.relname = 'orders' AND pg_catalog.pg_table_is_visible(tbl.oid) WHERE s.relkind = 'S'
) q""")
check('''SELECT q.column_name, q.is_nullable, q.column_default, q."collation", q.is_autofield, q.column_comment FROM (
SELECT a.attname AS column_name, NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable, pg_get_expr(ad.adbin, ad.adrelid) AS column_default, CASE WHEN collname = 'default' THEN NULL ELSE collname END AS collation, a.attidentity != '' AS is_autofield, col_description(a.attrelid, a.attnum) AS column_comment FROM pg_attribute a LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum LEFT JOIN pg_collation co ON a.attcollation = co.oid JOIN pg_type t ON a.atttypid = t.oid JOIN pg_class c ON a.attrelid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v') AND c.relname = 'orders' AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid)
) q
WHERE q.column_name @NOT IN ('cmax', 'cmin', 'ctid', 'id', 'ordered_on', 'status', 'tableoid', 'ticket', 'xmax', 'xmin')
ORDER BY q.column_name::text COLLATE "C"''', "deparse", "system_columns")
check('''SELECT q.relname, q."case", q.obj_description FROM (
SELECT c.relname, CASE WHEN c.relispartition THEN 'p' WHEN c.relkind IN ('m', 'v') THEN 'v' ELSE 't' END, obj_description(c.oid, 'pg_class') FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid)
) q
ORDER BY q.relname::text COLLATE "C"''')
check('''SELECT q.table_schema, q.table_name, q.table_type, q.is_insertable_into FROM (
select table_schema, table_name, table_type, is_insertable_into from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema') order by 1, 2
) q
WHERE q.table_name @NOT IN ('active_customers')
ORDER BY q.table_name::text COLLATE "C"''', "updatable_views")
check('''SELECT q."name", q.database_type, q.database_position, q.table_schema, q.table_name, q.pk, q.field_comment,
  q.database_required, q.database_is_auto_increment FROM (
SELECT c.column_name AS name, c.udt_name AS database_type, c.ordinal_position - 1 AS database_position, c.table_schema AS table_schema, c.table_name AS table_name, pk.column_name IS NOT NULL AS pk, col_description(cast(cast(format('%I.%I', cast(c.table_schema AS text), cast(c.table_name AS text)) AS regclass) AS oid), c.ordinal_position) AS field_comment, (column_default IS NULL AND is_nullable = 'NO' AND NOT (column_default IS NOT NULL OR is_identity = 'YES' OR is_generated = 'ALWAYS')) AS database_required, (column_default LIKE '%nextval(%' OR is_identity = 'YES') AS database_is_auto_increment FROM information_schema.columns AS c LEFT JOIN (SELECT tc.table_schema, tc.table_name, kc.column_name FROM information_schema.table_constraints AS tc INNER JOIN information_schema.key_column_usage AS kc ON (tc.constraint_name = kc.constraint_name) AND (tc.table_schema = kc.table_schema) AND (tc.table_name = kc.table_name) WHERE tc.constraint_type = 'PRIMARY KEY') AS pk ON (c.table_schema = pk.table_schema) AND (c.table_name = pk.table_name) AND (c.column_name = pk.column_name) WHERE c.table_schema = 'public' AND c.table_name = 'customers' ORDER BY table_schema ASC, table_name ASC, database_position ASC
) q
WHERE q."name" @NOT IN ('email')
ORDER BY q."name"::text COLLATE "C"''', "typmod")
check('''SELECT q.oid::regtype::text AS oid, q.attname, q.atttypid FROM (
SELECT typ.oid, att.attname, att.atttypid FROM pg_type AS typ JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace) JOIN pg_class AS cls ON (cls.oid = typ.typrelid) JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid) WHERE (typ.typtype = 'c' AND cls.relkind='c') AND attnum > 0 AND NOT attisdropped ORDER BY typ.oid, att.attnum
) q
WHERE q.attname @NOT IN ('city')
ORDER BY q.attname::text COLLATE "C"''', "typmod")
check('''SELECT q.oid::regtype::text AS oid, q.enumlabel FROM (
SELECT pg_type.oid, enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid ORDER BY oid, enumsortorder
) q
ORDER BY q.enumlabel::text COLLATE "C"''')
check('''SELECT q.nspname, q.oid::regtype::text AS oid, q.typname, q.typtype, q.typnotnull,
  q.elemtypoid::regtype::text AS elemtypoid FROM (
SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid FROM ( SELECT typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind, elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind, CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype FROM ( SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid, CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype, CASE WHEN proc.proname='array_recv' THEN typ.typelem WHEN typ.typtype='r' THEN rngsubtype WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid) WHEN typ.typtype='d' THEN typ.typbasetype END AS elemtypoid FROM pg_type AS typ LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid) LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid) ) AS typ LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid) LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive ) AS t JOIN pg_namespace AS ns ON (ns.oid = typnamespace) WHERE typtype IN ('b', 'r', 'm', 'e', 'd') OR (typtype = 'c' AND relkind='c') OR (typtype = 'p' AND typname IN ('record', 'void', 'unknown')) OR (typtype = 'a' AND ( elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR (elemtyptype = 'c' AND elemrelkind='c') )) ORDER BY CASE WHEN typtype IN ('b', 'e', 'p') THEN 0 WHEN typtype = 'r' THEN 1 WHEN typtype = 'm' THEN 2 WHEN typtype = 'c' THEN 3 WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 WHEN typtype = 'a' THEN 5 WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6 END
) q
WHERE q.typname @NOT IN ('_geometry', '_union', '_variant', 'geometry', 'union', 'variant')
ORDER BY q.typname::text COLLATE "C"''', "duckdb_types")
check('''SELECT q.schemaname, q.tablename, q.indexname, q.tablespace,
  replace(q.indexdef, ' USING secondary ', ' USING btree ') AS indexdef FROM (
SELECT schemaname, tablename, indexname, tablespace, indexdef FROM pg_indexes WHERE schemaname IN ('public', 'app')
) q
WHERE q.indexname @NOT IN ('orders_amount_desc_idx')
ORDER BY q.indexname::text COLLATE "C"''', "index_desc")
check("""SELECT q.setting FROM (
select setting from pg_catalog.pg_settings where name='standard_conforming_strings'
) q""")
check('''SELECT q."name", q.setting, q.unit, q.vartype FROM (
select name, setting, unit, vartype from pg_catalog.pg_settings where name in ('max_identifier_length', 'integer_datetimes', 'server_encoding', 'client_encoding', 'DateStyle')
) q
ORDER BY q."name"::text COLLATE "C"''')
check('''SELECT q.schemaname, q.tablename, q.tableowner, q.tablespace, q.hasindexes, q.hasrules, q.hastriggers, q.rowsecurity FROM (
SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM pg_tables WHERE schemaname IN ('public', 'app')
) q
ORDER BY q.tablename::text COLLATE "C"''')
PG_VIEWS = "SELECT schemaname, viewname, viewowner, definition FROM pg_views WHERE schemaname IN ('public', 'app')"
check(f'''SELECT q.schemaname, q.viewname, q.viewowner FROM (
{PG_VIEWS}
) q
ORDER BY q.viewname::text COLLATE "C"''')
check(f'''SELECT q.schemaname, q.viewname, {spaced("q.definition", "definition")} FROM (
{PG_VIEWS}
) q
ORDER BY q.viewname::text COLLATE "C"''', "deparse")
PG_SEQUENCES = ("SELECT schemaname, sequencename, sequenceowner, data_type, start_value, min_value, max_value, increment_by, "
                "cycle, cache_size FROM pg_sequences WHERE schemaname IN ('public', 'app')")
check(f'''SELECT q.schemaname, q.sequencename, q.start_value, q.min_value, q.max_value, q.increment_by, q.cycle, q.cache_size FROM (
{PG_SEQUENCES}
) q
ORDER BY q.sequencename::text COLLATE "C"''')
check(f'''SELECT q.sequencename, q.sequenceowner, (q.data_type)::text AS data_type FROM (
{PG_SEQUENCES}
) q
WHERE q.sequencename @NOT IN ('customers_id_seq')
ORDER BY q.sequencename::text COLLATE "C"''', "serial_sequence")
check("""SELECT q.schemaname, q.tablename, q.policyname, q.permissive, q.roles, q.cmd, q.qual, q.with_check FROM (
SELECT schemaname, tablename, policyname, permissive, roles, cmd, qual, with_check FROM pg_policies WHERE schemaname IN ('public', 'app')
) q""")
PGADMIN_COLUMNS = ("SELECT DISTINCT ON (att.attnum) att.attname as name, att.atttypid, att.attlen, att.attnum, att.attndims, "
                   "att.atttypmod, att.attacl, att.attnotnull, att.attoptions, att.attstattarget, att.attstorage, "
                   "att.attidentity, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval, "
                   "pg_catalog.format_type(ty.oid,NULL) AS typname, pg_catalog.format_type(ty.oid,att.atttypmod) AS "
                   "displaytypname, pg_catalog.format_type(ty.oid,att.atttypmod) AS cltype, CASE WHEN ty.typelem > 0 THEN "
                   "ty.typelem ELSE ty.oid END as elemoid, (SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = "
                   "ty.typnamespace) as typnspname, ty.typstorage AS defaultstorage, description, pi.indkey, (SELECT "
                   "count(1) FROM pg_catalog.pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup, CASE WHEN "
                   "length(coll.collname::text) > 0 AND length(nspc.nspname::text) > 0 THEN "
                   "pg_catalog.concat(pg_catalog.quote_ident(nspc.nspname),'.',pg_catalog.quote_ident(coll.collname)) ELSE "
                   "'' END AS collspcname, EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=att.attrelid AND "
                   "contype='f' AND att.attnum=ANY(conkey)) As is_fk, (SELECT pg_catalog.array_agg(provider || '=' || "
                   "label) FROM pg_catalog.pg_seclabels sl1 WHERE sl1.objoid=att.attrelid AND sl1.objsubid=att.attnum) AS "
                   "seclabels, (CASE WHEN (att.attnum < 1) THEN true ElSE false END) AS is_sys_column, (CASE WHEN "
                   "(att.attidentity in ('a', 'd')) THEN 'Yes' ELSE 'No' END) AS colconstype, (CASE WHEN (att.attidentity "
                   "in ('a', 'd')) THEN (SELECT pg_catalog.pg_get_serial_sequence(pg_catalog.quote_ident(nspname) || '.' "
                   "|| pg_catalog.quote_ident(relname), att.attname) FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n "
                   "WHERE c.relnamespace = n.oid AND c.oid = att.attrelid) END) AS seqname FROM pg_catalog.pg_attribute "
                   "att JOIN pg_catalog.pg_type ty ON ty.oid=atttypid LEFT OUTER JOIN pg_catalog.pg_attrdef def ON "
                   "adrelid=att.attrelid AND adnum=att.attnum LEFT OUTER JOIN pg_catalog.pg_description des ON "
                   "(des.objoid=att.attrelid AND des.objsubid=att.attnum AND des.classoid='pg_class'::regclass) LEFT OUTER "
                   "JOIN (pg_catalog.pg_collation coll JOIN pg_catalog.pg_namespace nspc ON coll.collnamespace=nspc.oid) ON "
                   "att.attcollation=coll.oid LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND "
                   "indisprimary WHERE att.attrelid = 'public.orders'::regclass::oid::oid AND att.attnum > 0 AND "
                   "att.attisdropped IS FALSE ORDER BY att.attnum")
check(f'''SELECT q."name", q.attlen, q.attnum, q.attndims,
  (SELECT string_agg(x::text, ',' ORDER BY x::text COLLATE "C") FROM unnest(q.attacl) x) AS attacl, q.attnotnull,
  q.attoptions, q.attstattarget, q.attstorage, q.attidentity, q.typnspname, q.defaultstorage, q.description,
  array_to_string((q.indkey)::int2[], ' ') AS indkey, q.isdup, q.collspcname, q.is_fk, q.seclabels, q.is_sys_column,
  q.colconstype, q.seqname FROM (
{PGADMIN_COLUMNS}
) q
ORDER BY q."name"::text COLLATE "C"''')
check(f'''SELECT q."name", q.atttypid, q.atttypmod, q.defval, q.typname, q.displaytypname, q.cltype, q.elemoid FROM (
{PGADMIN_COLUMNS}
) q
WHERE q."name" @NOT IN ('id', 'ordered_on', 'status', 'ticket')
ORDER BY q."name"::text COLLATE "C"''', "typmod", "deparse")
check("""SELECT
  coalesce((SELECT 'current_database' FROM pg_catalog.pg_database x WHERE x.oid = q.did AND x.datname = current_database()), q.did::text) AS did,
  CASE WHEN q.datname = current_database() THEN 'current_database' ELSE q.datname END AS datname, q.datallowconn,
  q.serverencoding, q.cancreate, q.datistemplate FROM (
SELECT db.oid as did, db.datname, db.datallowconn, pg_catalog.pg_encoding_to_char(db.encoding) AS serverencoding, pg_catalog.has_database_privilege(db.oid, 'CREATE') as cancreate, datistemplate FROM pg_catalog.pg_database db WHERE db.datname = current_database()
) q""")
check(r'''SELECT q.oid::regnamespace::text AS oid, q."name", q.can_create, q.has_usage,
  CASE WHEN q."name" IN ('pg_catalog', 'public', 'information_schema') THEN NULL ELSE q.description END AS description FROM (
SELECT nsp.oid, nsp.nspname as name, pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create, pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage, des.description FROM pg_catalog.pg_namespace nsp LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass) WHERE NOT ( (nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1)) ) AND nsp.nspname NOT LIKE 'pg\_%' ORDER BY nspname
) q
ORDER BY q."name"::text COLLATE "C"''')
check('''SELECT q.oid::regclass::text AS oid, q."name", q.triggercount, q.has_enable_triggers, q.is_partitioned, q."schema",
  q.schema_id, q.is_partition, q.description, q.rlspolicy, q.forcerlspolicy FROM (
SELECT rel.oid, rel.relname AS name, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers, (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned, nsp.nspname AS schema, nsp.oid AS schema_id, rel.relispartition AS is_partition, pg_catalog.obj_description(rel.oid, 'pg_class') AS description, rel.relrowsecurity AS rlspolicy, rel.relforcerowsecurity AS forcerlspolicy FROM pg_catalog.pg_class rel LEFT JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid WHERE rel.relkind IN ('r','t','f','p') AND rel.relnamespace = 'public'::regnamespace::oid::oid AND NOT rel.relispartition ORDER BY rel.relname
) q
ORDER BY q."name"::text COLLATE "C"''')


def pgjdbc_columns(schema, table):
    return ("SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND "
            "t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,row_number() OVER (PARTITION BY a.attrelid "
            "ORDER BY a.attnum) AS attnum, nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as "
            "attgenerated,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  "
            "FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN "
            "pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT "
            "JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN "
            "pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN "
            "pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN "
            "pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  WHERE c.relkind in "
            "('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped  AND n.nspname LIKE "
            f"'{schema}' AND c.relname LIKE '{table}') c WHERE true  ORDER BY nspname,c.relname,attnum")


for schema, table, differing, causes in (
        ("app", "all_types", "('c_char', 'c_numeric_plain', 'c_text', 'c_varchar')", ("typmod", "numeric", "deparse")),
        ("public", "active_customers", "('email')", ("typmod",))):
    check(f'''SELECT q.nspname, q.relname, q.attname, q.atttypid::regtype::text AS atttypid, q.attnotnull, q.atttypmod, q.attlen,
  q.typtypmod, q.attnum, q.attidentity, q.attgenerated, q.adsrc, q.description, q.typbasetype, q.typtype FROM (
{pgjdbc_columns(schema, table)}
) q
WHERE q.attname @NOT IN {differing}
ORDER BY q.attname::text COLLATE "C"''', *causes)
check("""SELECT q.pktable_cat, q.pktable_schem, q.pktable_name, q.pkcolumn_name, q.fktable_cat, q.fktable_schem,
  q.fktable_name, q.fkcolumn_name, q.key_seq, q.update_rule, q.delete_rule, q.fk_name, q.pk_name, q.deferrability FROM (
SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, pos.n AS KEY_SEQ, CASE con.confupdtype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS UPDATE_RULE, CASE con.confdeltype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS DELETE_RULE, con.conname AS FK_NAME, pkic.relname AS PK_NAME, CASE  WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS DEFERRABILITY  FROM  pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka,  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka,  pg_catalog.pg_constraint con,  pg_catalog.generate_series(1, 32) pos(n),  pg_catalog.pg_class pkic WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid  AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid  AND con.contype = 'f' AND (pkic.relkind = 'i' OR pkic.relkind = 'I') AND pkic.oid = con.conindid  AND fkn.nspname = E'public' AND fkc.relname = E'order_items' ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n
) q""")
PGJDBC_INDEXES = ("SELECT     tmp.TABLE_CAT,     tmp.TABLE_SCHEM,     tmp.TABLE_NAME,     tmp.NON_UNIQUE,     "
                  "tmp.INDEX_QUALIFIER,     tmp.INDEX_NAME,     tmp.TYPE,     tmp.ORDINAL_POSITION,     trim(both '\"' "
                  "from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS COLUMN_NAME,   CASE "
                  "tmp.AM_NAME     WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint       "
                  "WHEN 1 THEN 'D'       ELSE 'A'     END     ELSE NULL   END AS ASC_OR_DESC,     tmp.CARDINALITY,     "
                  "tmp.PAGES,     tmp.FILTER_CONDITION FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   "
                  "ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE,   NULL AS INDEX_QUALIFIER, ci.relname AS "
                  "INDEX_NAME,   CASE i.indisclustered     WHEN true THEN 1    ELSE CASE am.amname       WHEN 'hash' THEN "
                  "2      ELSE 3    END   END AS TYPE,   (information_schema._pg_expandarray(i.indkey)).n AS "
                  "ORDINAL_POSITION,   ci.reltuples AS CARDINALITY,   ci.relpages AS PAGES,   "
                  "pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION,   ci.oid AS CI_OID,   i.indoption "
                  "AS I_INDOPTION,   am.amname AS AM_NAME FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_namespace n ON "
                  "(ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)   JOIN "
                  "pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) "
                  "WHERE true  AND n.nspname = E'public' AND ct.relname = E'orders') AS tmp ORDER BY NON_UNIQUE, TYPE, "
                  "INDEX_NAME, ORDINAL_POSITION")
check(f'''SELECT q.table_cat, q.table_schem, q.table_name, q.non_unique, q.index_qualifier, q.index_name, q."type",
  q.ordinal_position, q.column_name, q.filter_condition FROM (
{PGJDBC_INDEXES}
) q
ORDER BY q.index_name::text COLLATE "C"''')
check(f'''SELECT q.index_name, q.asc_or_desc FROM (
{PGJDBC_INDEXES}
) q
ORDER BY q.index_name::text COLLATE "C"''', "index_am", "index_desc")
check('''SELECT q.table_cat, q.table_schem, q.table_name, q.column_name, q.key_seq, q.pk_name FROM (
SELECT result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = E'public' AND ct.relname = E'order_items' AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.table_name, result.pk_name, result.key_seq
) q
ORDER BY q.column_name::text COLLATE "C"''')
check('''SELECT q.table_cat, q.table_schem, q.table_name, q.table_type, q.remarks, q.type_cat, q.type_schem, q.type_name,
  q.self_referencing_col_name, q.ref_generation FROM (
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'I' THEN 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)  WHERE c.relnamespace = n.oid  AND n.nspname LIKE 'public' AND c.relname LIKE '%' AND (false  OR ( c.relkind IN ('r','p') AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' )  OR ( c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' ) )  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME
) q
ORDER BY q.table_name::text COLLATE "C"''')
check("""SELECT q."?column?", q.nspname, q.typname FROM (
SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = 1043
) q""")
check("""SELECT q.typarray, q.typname FROM (
SELECT t.typarray, arr.typname   FROM pg_catalog.pg_type t  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid  JOIN pg_catalog.pg_type arr ON arr.oid = t.typarray WHERE t.typname = 'int4' AND (n.nspname = 'pg_catalog' OR 'pg_catalog' IS NULL AND n.nspname = ANY (current_schemas(true))) ORDER BY t.oid DESC LIMIT 1
) q""")
check("""SELECT q.oid::regtype::text AS oid, q.typname FROM (
SELECT pg_type.oid, typname   FROM pg_catalog.pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE typname = 'customers'  ORDER BY sp.r, pg_type.oid DESC LIMIT 1
) q""")
check("""SELECT q.is_array, q.typtype, q.typname, q.oid::regtype::text AS oid FROM (
SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = 'app.mood'::regtype::oid  ORDER BY sp.r, pg_type.oid DESC
) q""")
check("""SELECT q.oid, q."?column?", q.nspname, q.typname FROM (
SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid JOIN pg_catalog.pg_type e ON t.typelem = e.oid WHERE t.oid = 1007
) q""")
check('''SELECT q.oid::regtype::text AS oid, q.typarray::regtype::text AS typarray FROM (
select b.oid, b.typarray from pg_catalog.pg_type a left join pg_catalog.pg_type b on b.oid = a.typelem where a.typcategory = 'A' group by b.oid, b.typarray order by b.oid
) q
WHERE q.oid::regtype::text @NOT IN ('geometry', 'information_schema."columns"', 'information_schema.columns', '"union"',
    'union', 'variant')
ORDER BY q.oid::regtype::text COLLATE "C"''', "duckdb_types", "quoted_keywords")
ADDRESS_FIELDS = ("SELECT t.typname AS name, t.oid AS oid, t.typarray AS array_oid, t.oid::regtype::text AS regtype, "
                  "coalesce(a.fnames, '{}') AS field_names, coalesce(a.ftypes, '{}') AS field_types FROM pg_type t LEFT "
                  "JOIN ( SELECT attrelid, array_agg(attname) AS fnames, array_agg(atttypid) AS ftypes FROM ( SELECT "
                  "a.attrelid, a.attname, a.atttypid FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE "
                  "t.oid = to_regtype('app.address') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum ) x GROUP "
                  "BY attrelid ) a ON a.attrelid = t.typrelid WHERE t.oid = to_regtype('app.address')")
check(f"""SELECT q."name", q.oid::regtype::text AS oid, q.array_oid::regtype::text AS array_oid, q.regtype, q.field_names FROM (
{ADDRESS_FIELDS}
) q""")
check(f"""SELECT q.field_types FROM (
{ADDRESS_FIELDS}
) q""", "typmod")
check("""SELECT q."name", q.oid::regtype::text AS oid, q.array_oid::regtype::text AS array_oid, q.labels FROM (
SELECT name, oid, array_oid, array_agg(label) AS labels FROM ( SELECT t.typname AS name, t.oid AS oid, t.typarray AS array_oid, e.enumlabel AS label FROM pg_type t LEFT JOIN pg_enum e ON e.enumtypid = t.oid WHERE t.oid = to_regtype('app.mood') ORDER BY e.enumsortorder ) x GROUP BY name, oid, array_oid
) q""")
check("""SELECT q."name", q.oid::regtype::text AS oid, q.array_oid::regtype::text AS array_oid, q.regtype, q.delimiter FROM (
SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter FROM pg_type t WHERE t.oid = to_regtype('app.mood') ORDER BY t.oid
) q""")
CHECK_DEFS = ("SELECT conname, pg_get_constraintdef(c.oid, true) AS constraintdef, c.convalidated AS valid FROM pg_constraint c "
              "JOIN pg_class t ON c.conrelid = t.oid JOIN pg_namespace n ON n.oid = c.connamespace WHERE c.contype = 'c' AND "
              "t.relname = 'orders' AND n.nspname = ANY (current_schemas(false))")
check(f'''SELECT q.conname, q.valid FROM (
{CHECK_DEFS}
) q
ORDER BY q.conname::text COLLATE "C"''')
check(f'''SELECT q.conname, q.constraintdef FROM (
{CHECK_DEFS}
) q
ORDER BY q.conname::text COLLATE "C"''', "typmod", "deparse")
ORDERS_COLUMNS = ("SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull, "
                  "a.atttypid, a.atttypmod, c.collname, col_description(a.attrelid, a.attnum) AS comment, attidentity AS "
                  "identity, attgenerated as attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = "
                  "d.adrelid AND a.attnum = d.adnum LEFT JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN pg_collation c ON "
                  "a.attcollation = c.oid AND a.attcollation <> t.typcollation WHERE a.attrelid = '\"orders\"'::regclass AND "
                  "a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")
check(f'''SELECT q.attname, q.format_type, q.attnotnull, q.atttypid, q.atttypmod, q.collname, q.comment, q.identity,
  q.attgenerated FROM (
{ORDERS_COLUMNS}
) q
WHERE q.attname @NOT IN ('status')
ORDER BY q.attname::text COLLATE "C"''', "typmod")
check(f'''SELECT q.attname, q.pg_get_expr FROM (
{ORDERS_COLUMNS}
) q
WHERE q.attname @NOT IN ('id', 'ordered_on', 'status', 'ticket')
ORDER BY q.attname::text COLLATE "C"''', "deparse")
check("""SELECT q.relname FROM (
SELECT c.relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY (current_schemas(false)) AND c.relname = 'orders' AND c.relkind IN ('r','p')
) q""")
check("""SELECT q."name", q.oid::regtype::text AS oid, q."schema", q."value" FROM (
SELECT type.typname AS name, type.OID AS oid, n.nspname AS schema, array_agg(enum.enumlabel ORDER BY enum.enumsortorder) AS value FROM pg_enum AS enum JOIN pg_type AS type ON (type.oid = enum.enumtypid) JOIN pg_namespace n ON type.typnamespace = n.oid WHERE n.nspname = ANY (current_schemas(false)) OR n.nspname = 'app' GROUP BY type.OID, n.nspname, type.typname
) q""")
check("""SELECT q.to_table, q."column", q.primary_key, q."name", q.on_update, q.on_delete, q.valid, q."deferrable", q.deferred,
  q.conkey, q.confkey, q.conrelid::regclass::text AS conrelid, q.confrelid::regclass::text AS confrelid FROM (
SELECT t2.oid::regclass::text AS to_table, a1.attname AS column, a2.attname AS primary_key, c.conname AS name, c.confupdtype AS on_update, c.confdeltype AS on_delete, c.convalidated AS valid, c.condeferrable AS deferrable, c.condeferred AS deferred, c.conkey, c.confkey, c.conrelid, c.confrelid FROM pg_constraint c JOIN pg_class t1 ON c.conrelid = t1.oid JOIN pg_class t2 ON c.confrelid = t2.oid JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid JOIN pg_namespace t3 ON c.connamespace = t3.oid WHERE c.contype = 'f' AND t1.relname = 'order_items' AND t3.nspname = ANY (current_schemas(false)) ORDER BY c.conname
) q""")
check('''SELECT q.relname, q.indisunique, array_to_string((q.indkey)::int2[], ' ') AS indkey, q.oid::regclass::text AS oid,
  q.comment, q.indisvalid, replace(q.pg_get_indexdef, ' USING secondary ', ' USING btree ') AS pg_get_indexdef FROM (
SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid, pg_catalog.obj_description(i.oid, 'pg_class') AS comment, d.indisvalid FROM pg_class t INNER JOIN pg_index d ON t.oid = d.indrelid INNER JOIN pg_class i ON d.indexrelid = i.oid LEFT JOIN pg_namespace n ON n.oid = t.relnamespace WHERE i.relkind IN ('i', 'I') AND d.indisprimary = 'f' AND t.relname = 'orders' AND n.nspname = ANY (current_schemas(false)) ORDER BY i.relname
) q
WHERE q.relname @NOT IN ('orders_amount_desc_idx')
ORDER BY q.relname::text COLLATE "C"''', "index_desc")
check('''SELECT q.oid::regtype::text AS oid, q.typname, q.typelem::regtype::text AS typelem, q.typdelim,
  (q.typinput)::text AS typinput, q.rngsubtype, q.typtype, q.typbasetype FROM (
SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype FROM pg_type as t LEFT JOIN pg_range as r ON oid = rngtypid WHERE t.typname IN ('int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'numeric', 'bool', 'timestamp', 'timestamptz') OR t.typtype IN ('r', 'e', 'd') OR t.typinput = 'array_in(cstring,oid,integer)'::regprocedure OR t.typelem != 0
) q
WHERE q.typname @NOT IN ('_columns', '_geometry', '_union', '_variant')
ORDER BY q.typname::text COLLATE "C"''', "duckdb_types", "quoted_keywords")
check("""SELECT q.attname, q.nspname, q.relname FROM (
SELECT attr.attname, nsp.nspname, seq.relname FROM pg_class seq, pg_attribute attr, pg_depend dep, pg_constraint cons, pg_namespace nsp WHERE seq.oid = dep.objid AND seq.relkind = 'S' AND attr.attrelid = dep.refobjid AND attr.attnum = dep.refobjsubid AND attr.attrelid = cons.conrelid AND attr.attnum = cons.conkey[1] AND seq.relnamespace = nsp.oid AND cons.contype = 'p' AND dep.classid = 'pg_class'::regclass AND dep.refobjid = '"orders"'::regclass
) q""")
check('''SELECT q.attname FROM (
SELECT a.attname FROM ( SELECT indrelid, indkey, generate_subscripts(indkey, 1) idx FROM pg_index WHERE indrelid = '"order_items"'::regclass AND indisprimary ) i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[i.idx] ORDER BY i.idx
) q
ORDER BY q.attname::text COLLATE "C"''')
check('''SELECT q."name", q.data_type, q.is_array, q.column_comment FROM (
SELECT c.column_name AS name, COALESCE( NULLIF(c.udt_name || '(' || c.character_maximum_length::varchar || ')', c.udt_name || '(NULL)'), NULLIF(c.udt_name || '(' || c.numeric_precision::varchar || ',' || c.numeric_scale::varchar || ')', c.udt_name || '(,)'), NULLIF(c.udt_name || '(' || c.numeric_precision::varchar || ')', c.udt_name || '(NULL)'), NULLIF(c.udt_name || '(' || c.datetime_precision::varchar || ')', c.udt_name || '(NULL)'), c.udt_name) AS data_type, CASE WHEN c.data_type = 'ARRAY' THEN 'YES' ELSE 'NO' END AS is_array, pg_catalog.col_description(cls.oid, c.ordinal_position) AS column_comment FROM pg_class cls JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace JOIN information_schema.columns c ON c.table_schema = nsp.nspname AND c.table_name = cls.relname WHERE cls.oid = 'app.all_types'::regclass::oid ORDER BY c.ordinal_position
) q
WHERE q."name" @NOT IN ('c_char', 'c_numeric_plain', 'c_varchar')
ORDER BY q."name"::text COLLATE "C"''', "typmod", "numeric")
check('''SELECT q.id::regclass::text AS id, q."name" FROM (
SELECT c.oid AS id, c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p') AND has_schema_privilege(current_user, n.nspname, 'USAGE') AND has_table_privilege(current_user, c.oid, 'SELECT') AND n.oid = 'public'::regnamespace::oid
) q
ORDER BY q."name"::text COLLATE "C"''')


def sqlalchemy_columns(scope, relations):
    return ("SELECT pg_catalog.pg_attribute.attname AS name, pg_catalog.format_type(pg_catalog.pg_attribute.atttypid, "
            "pg_catalog.pg_attribute.atttypmod) AS format_type, (SELECT "
            "pg_catalog.pg_get_expr(pg_catalog.pg_attrdef.adbin, pg_catalog.pg_attrdef.adrelid) AS pg_get_expr_1 FROM "
            "pg_catalog.pg_attrdef WHERE pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid AND "
            "pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum AND pg_catalog.pg_attribute.atthasdef) AS "
            "\"default\", pg_catalog.pg_attribute.attnotnull AS not_null, pg_catalog.pg_class.relname AS table_name, "
            "pg_catalog.pg_description.description AS comment, pg_catalog.pg_attribute.attgenerated AS generated, "
            "(SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a', 'start', "
            "pg_catalog.pg_sequence.seqstart, 'increment', pg_catalog.pg_sequence.seqincrement, 'minvalue', "
            "pg_catalog.pg_sequence.seqmin, 'maxvalue', pg_catalog.pg_sequence.seqmax, 'cache', "
            "pg_catalog.pg_sequence.seqcache, 'cycle', pg_catalog.pg_sequence.seqcycle) AS json_build_object_1 FROM "
            "pg_catalog.pg_sequence WHERE pg_catalog.pg_attribute.attidentity != '' AND "
            "pg_catalog.pg_sequence.seqrelid = CAST(CAST(pg_catalog.pg_get_serial_sequence(CAST(CAST("
            "pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), pg_catalog.pg_attribute.attname) AS REGCLASS) AS "
            "OID)) AS identity_options FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_attribute ON "
            "pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attribute.attnum > 0 AND NOT "
            "pg_catalog.pg_attribute.attisdropped LEFT OUTER JOIN pg_catalog.pg_description ON "
            "pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid AND "
            "pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum JOIN pg_catalog.pg_namespace ON "
            "pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relkind = ANY "
            f"(ARRAY['r', 'p', 'f', 'v', 'm']) AND {scope} AND pg_catalog.pg_class.relname IN {quoted(relations)} "
            "ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum")


SQLALCHEMY_DOMAINS = ("SELECT pg_catalog.format_type(pg_catalog.pg_type.typbasetype, pg_catalog.pg_type.typtypmod) AS attype, "
                      "NOT pg_catalog.pg_type.typnotnull AS nullable, pg_catalog.pg_type.typname AS name, "
                      "pg_catalog.pg_type_is_visible(pg_catalog.pg_type.oid) AS visible, pg_catalog.pg_namespace.nspname AS "
                      "schema, pg_catalog.pg_type.typdefault AS \"default\", pg_catalog.pg_collation.collname FROM "
                      "pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = "
                      "pg_catalog.pg_type.typnamespace LEFT OUTER JOIN pg_catalog.pg_collation ON "
                      "pg_catalog.pg_type.typcollation = pg_catalog.pg_collation.oid WHERE pg_catalog.pg_type.typtype = 'd' "
                      "ORDER BY pg_catalog.pg_namespace.nspname, pg_catalog.pg_type.typname")
SQLALCHEMY_VISIBLE = "pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'"
check(f'''SELECT q."name", q.format_type, q.not_null, q.table_name, q.comment, q.generated, q.identity_options FROM (
{sqlalchemy_columns(SQLALCHEMY_VISIBLE, ("orders", "customers"))}
) q
WHERE (q."name", q.table_name) @NOT IN (('email', 'customers'), ('status', 'orders'))
ORDER BY q."name"::text COLLATE "C", q.table_name::text COLLATE "C"''', "typmod", "deparse")
check(f'''SELECT q."name", q."default", q.table_name FROM (
{sqlalchemy_columns(SQLALCHEMY_VISIBLE, ("orders", "customers"))}
) q
WHERE (q."name", q.table_name) @NOT IN (('id', 'customers'), ('id', 'orders'), ('mood', 'customers'),
    ('ordered_on', 'orders'), ('status', 'orders'), ('ticket', 'orders'))
ORDER BY q."name"::text COLLATE "C", q.table_name::text COLLATE "C"''', "deparse")
check(f'''SELECT q.attype, q.nullable, q."name", q.visible, q."schema", q."default", q.collname FROM (
{SQLALCHEMY_DOMAINS}
) q
ORDER BY q."name"::text COLLATE "C"''')
check("""SELECT q."name", q.visible, q."schema", q.labels FROM (
SELECT pg_catalog.pg_type.typname AS name, pg_catalog.pg_type_is_visible(pg_catalog.pg_type.oid) AS visible, pg_catalog.pg_namespace.nspname AS schema, lbl_agg.labels AS labels FROM pg_catalog.pg_type JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_type.typnamespace LEFT OUTER JOIN (SELECT pg_catalog.pg_enum.enumtypid AS enumtypid, array_agg(CAST(pg_catalog.pg_enum.enumlabel AS TEXT) ORDER BY pg_catalog.pg_enum.enumsortorder) AS labels FROM pg_catalog.pg_enum GROUP BY pg_catalog.pg_enum.enumtypid) AS lbl_agg ON pg_catalog.pg_type.oid = lbl_agg.enumtypid WHERE pg_catalog.pg_type.typtype = 'e' ORDER BY pg_catalog.pg_namespace.nspname, pg_catalog.pg_type.typname
) q""")
check('''SELECT q.relname, q.conname, q.anon_1, q.nspname, q.description FROM (
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname, CASE WHEN (pg_catalog.pg_constraint.oid IS NOT NULL) THEN pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) END AS anon_1, nsp_ref.nspname, pg_catalog.pg_description.description FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_class.oid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_constraint.contype = 'f' LEFT OUTER JOIN pg_catalog.pg_class AS cls_ref ON cls_ref.oid = pg_catalog.pg_constraint.confrelid LEFT OUTER JOIN pg_catalog.pg_namespace AS nsp_ref ON cls_ref.relnamespace = nsp_ref.oid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid JOIN pg_catalog.pg_namespace ON pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('orders', 'order_items') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname
) q
ORDER BY q.relname::text COLLATE "C", q.conname::text COLLATE "C"''')
SQLALCHEMY_INDEXES = ("SELECT pg_catalog.pg_index.indrelid, cls_idx.relname AS relname_index, pg_catalog.pg_index.indisunique, "
                      "pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint, pg_catalog.pg_index.indoption, "
                      "cls_idx.reloptions, pg_catalog.pg_am.amname, CASE WHEN (pg_catalog.pg_index.indpred IS NOT NULL) "
                      "THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid) END AS "
                      "filter_definition, pg_catalog.pg_index.indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct FROM "
                      "pg_catalog.pg_index JOIN pg_catalog.pg_class AS cls_idx ON pg_catalog.pg_index.indexrelid = "
                      "cls_idx.oid JOIN pg_catalog.pg_am ON cls_idx.relam = pg_catalog.pg_am.oid LEFT OUTER JOIN "
                      "pg_catalog.pg_constraint ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid AND "
                      "pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid AND "
                      "pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x']) WHERE pg_catalog.pg_index.indrelid IN "
                      "('public.orders'::regclass::oid) AND NOT pg_catalog.pg_index.indisprimary ORDER BY "
                      "pg_catalog.pg_index.indrelid, cls_idx.relname")
check(f'''SELECT q.indrelid::regclass::text AS indrelid, q.relname_index, q.indisunique, q.has_constraint, q.reloptions,
  replace(q.amname, 'secondary', 'btree') AS amname, q.filter_definition, q.indnkeyatts, q.indnullsnotdistinct,
  array_to_string((q.indoption)::int2[], ' ') AS indoption FROM (
{SQLALCHEMY_INDEXES}
) q
WHERE q.relname_index @NOT IN ('orders_amount_desc_idx')
ORDER BY q.relname_index::text COLLATE "C"''', "index_desc")
check("""SELECT q.conrelid::regclass::text AS conrelid, q.cols, q.conname, q.description FROM (
SELECT attr.conrelid, array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols, attr.conname, min(attr.description) AS description FROM (SELECT con.conrelid AS conrelid, con.conname AS conname, con.conindid AS conindid, con.description AS description, con.ord AS ord, pg_catalog.pg_attribute.attname AS attname FROM pg_catalog.pg_attribute JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid, pg_catalog.pg_constraint.conname AS conname, pg_catalog.pg_constraint.conindid AS conindid, unnest(pg_catalog.pg_constraint.conkey) AS attnum, generate_subscripts(pg_catalog.pg_constraint.conkey, 1) AS ord, pg_catalog.pg_description.description AS description FROM pg_catalog.pg_constraint LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid WHERE pg_catalog.pg_constraint.contype = 'p' AND pg_catalog.pg_constraint.conrelid IN ('public.order_items'::regclass::oid)) AS con ON pg_catalog.pg_attribute.attnum = con.attnum AND pg_catalog.pg_attribute.attrelid = con.conrelid WHERE con.conrelid IN ('public.order_items'::regclass::oid)) AS attr GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname
) q""")
check('''SELECT q.relname FROM (
SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p']) AND pg_catalog.pg_class.relpersistence != 't' AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
) q
ORDER BY q.relname::text COLLATE "C"''')
check("""SELECT q.oid::regclass::text AS oid FROM (
SELECT pg_catalog.pg_class.oid FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relname = 'all_types' AND pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_namespace.nspname = 'app'
) q""")
check(f"""SELECT {spaced("q.pg_get_viewdef_1", "pg_get_viewdef_1")} FROM (
SELECT pg_catalog.pg_get_viewdef(pg_catalog.pg_class.oid, true) AS pg_get_viewdef_1 FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relname = 'order_totals' AND pg_catalog.pg_class.relkind = ANY (ARRAY['v', 'm']) AND pg_catalog.pg_namespace.nspname = 'app'
) q""", "deparse")
check("""SELECT q.relname FROM (
SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relkind = 'S' AND pg_catalog.pg_class.relname = 'ticket_seq' AND pg_catalog.pg_namespace.nspname = 'app'
) q""")
check("""SELECT q.relname FROM (
SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relname = 'orders' AND pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
) q""")
check('''SELECT q.attname, q.atttypid FROM (
SELECT attname, atttypid FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT typrelid FROM pg_catalog.pg_type WHERE oid = 'app.address'::regtype::oid) AND attnum > 0 AND NOT attisdropped ORDER BY attnum
) q
WHERE q.attname @NOT IN ('city')
ORDER BY q.attname::text COLLATE "C"''', "typmod")
check('''SELECT q.enumlabel FROM (
SELECT enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = 'app.mood'::regtype::oid ORDER BY enumsortorder
) q
ORDER BY q.enumlabel::text COLLATE "C"''')
check("""SELECT q.typname, q.typtype, q.typelem, q.rngsubtype, q.typbasetype, q.nspname, q.typrelid FROM (
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid FROM pg_catalog.pg_type t LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = 'app.mood'::regtype::oid
) q""")
check('''SELECT q."Schema", q."Name", q."Description" FROM (
SELECT n.nspname as "Schema",
  pg_catalog.format_type(t.oid, NULL) AS "Name",
  pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  AND n.nspname OPERATOR(pg_catalog.~) '^(app)$' COLLATE pg_catalog.default
ORDER BY 1, 2
) q
ORDER BY q."Name"::text COLLATE "C"''')


def psql_lookup(name, schema):
    scope = (f"n.nspname OPERATOR(pg_catalog.~) '^({schema})$' COLLATE pg_catalog.default" if schema != "public"
             else "pg_catalog.pg_table_is_visible(c.oid)")
    return f"""SELECT q.oid::regclass::text AS oid, q.nspname, q.relname FROM (
SELECT c.oid,
  n.nspname,
  c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^({name})$' COLLATE pg_catalog.default
  AND {scope}
ORDER BY 2, 3
) q"""


def psql_relation(relation, verbose=False, projection=None):
    options = ("pg_catalog.array_to_string(c.reloptions || array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')\n, "
               if verbose else "'', ")
    unnamed = "" if verbose else ' q."?column?",'
    projection = projection or (
        "q.relchecks, q.relkind, q.relhasindex, q.relhasrules, q.relhastriggers, q.relrowsecurity,\n"
        f"  q.relforcerowsecurity, q.relhasoids, q.relispartition,{unnamed} q.reltablespace, q.reloftype, q.relpersistence,\n"
        "  q.relreplident, replace(q.amname, 'secondary', 'btree') AS amname")
    return f"""SELECT {projection} FROM (
SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, {options}c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = '{relation}'::regclass
) q"""


PSQL_COLUMNS = "q.attname, q.format_type, q.pg_get_expr, q.attnotnull, q.attcollation, q.attidentity, q.attgenerated"
PSQL_TABLE_EXTRAS = ("a.attstorage", "a.attcompression AS attcompression",
                     "CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget",
                     "pg_catalog.col_description(a.attrelid, a.attnum)")
PSQL_VIEW_EXTRAS = ("a.attstorage", "pg_catalog.col_description(a.attrelid, a.attnum)")


def psql_columns(relation, projection, differing="", extras=()):
    added = "".join(f",\n  {column}" for column in extras)
    return f'''SELECT {projection} FROM (
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated{added}
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '{relation}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
) q{differing}
ORDER BY q.attname::text COLLATE "C"'''


def psql_table_tail(relation, references=True):
    if references:
        check(f"""SELECT q.sametable, q.conname, q.condef, (q.ontable)::text AS ontable FROM (
SELECT true as sametable, conname,
  pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
  conrelid::pg_catalog.regclass AS ontable
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = '{relation}'::regclass AND r.contype = 'f'
     AND conparentid = 0
ORDER BY conname
) q""")
        check(f"""SELECT q.conname, (q.ontable)::text AS ontable, q.condef FROM (
SELECT conname, conrelid::pg_catalog.regclass AS ontable,
       pg_catalog.pg_get_constraintdef(oid, true) AS condef
  FROM pg_catalog.pg_constraint c
 WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{relation}'::regclass)
                     UNION ALL VALUES ('{relation}'::pg_catalog.regclass))
       AND contype = 'f' AND conparentid = 0
ORDER BY conname
) q""")
    check(f"""SELECT q.c1 AS polname, q.c2 AS polpermissive, q.c3 AS array_to_string, q.c4 AS pg_get_expr, q.c5 AS pg_get_expr,
  q.c6 AS cmd FROM (
SELECT pol.polname, pol.polpermissive,
  CASE WHEN pol.polroles = '{{0}}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
  CASE pol.polcmd
    WHEN 'r' THEN 'SELECT'
    WHEN 'a' THEN 'INSERT'
    WHEN 'w' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    END AS cmd
FROM pg_catalog.pg_policy pol
WHERE pol.polrelid = '{relation}'::regclass ORDER BY 1
) q(c1, c2, c3, c4, c5, c6)""")
    check(f"""SELECT q.oid, (q.stxrelid)::text AS stxrelid, q.nsp, q.stxname, q.columns, q.ndist_enabled, q.deps_enabled,
  q.mcv_enabled, q.stxstattarget FROM (
SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
  'd' = any(stxkind) AS ndist_enabled,
  'f' = any(stxkind) AS deps_enabled,
  'm' = any(stxkind) AS mcv_enabled,
stxstattarget
FROM pg_catalog.pg_statistic_ext
WHERE stxrelid = '{relation}'::regclass
ORDER BY nsp, stxname
) q""")
    check(f"""SELECT q.c1 AS pubname, q.c2 AS "?column?", q.c3 AS "?column?" FROM (
SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
     JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
     JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
WHERE pc.oid ='{relation}'::regclass and pg_catalog.pg_relation_is_publishable('{relation}'::regclass)
UNION
SELECT pubname
     , pg_get_expr(pr.prqual, c.oid)
     , (CASE WHEN pr.prattrs IS NOT NULL THEN
         (SELECT string_agg(attname, ', ')
           FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                pg_catalog.pg_attribute
          WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
        ELSE NULL END) FROM pg_catalog.pg_publication p
     JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
     JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
WHERE pr.prrelid = '{relation}'::regclass
UNION
SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('{relation}'::regclass)
ORDER BY 1
) q(c1, c2, c3)""")
    if references:
        check(f"""SELECT q.tgname, q.pg_get_triggerdef, q.tgenabled, q.tgisinternal, (q.parent)::text AS parent FROM (
SELECT t.tgname, pg_catalog.pg_get_triggerdef(t.oid, true), t.tgenabled, t.tgisinternal,
  CASE WHEN t.tgparentid != 0 THEN
    (SELECT u.tgrelid::pg_catalog.regclass
     FROM pg_catalog.pg_trigger AS u,
          pg_catalog.pg_partition_ancestors(t.tgrelid) WITH ORDINALITY AS a(relid, depth)
     WHERE u.tgname = t.tgname AND u.tgrelid = a.relid
           AND u.tgparentid = 0
     ORDER BY a.depth LIMIT 1)
  END AS parent
FROM pg_catalog.pg_trigger t
WHERE t.tgrelid = '{relation}'::regclass AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = 'D'))
ORDER BY 1
) q""")
    check(f"""SELECT (q.oid)::text AS oid FROM (
SELECT c.oid::pg_catalog.regclass
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhparent AND i.inhrelid = '{relation}'::regclass
  AND c.relkind != 'p' AND c.relkind != 'I'
ORDER BY inhseqno
) q""")
    check(f"""SELECT (q.oid)::text AS oid, q.relkind, q.inhdetachpending, q.pg_get_expr FROM (
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = '{relation}'::regclass
ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text
) q""")


def psql_indexes(relation, differing=""):
    return f'''SELECT q.relname, q.indisprimary, q.indisunique, q.indisclustered, q.indisvalid,
  replace(q.pg_get_indexdef, ' USING secondary ', ' USING btree ') AS pg_get_indexdef, q.pg_get_constraintdef,
  q.contype, q.condeferrable, q.condeferred, q.indisreplident, q.reltablespace FROM (
SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = '{relation}'::regclass AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, c2.relname
) q{differing}
ORDER BY q.relname::text COLLATE "C"'''


def psql_checks(relation):
    return f'''SELECT q.conname, q.pg_get_constraintdef FROM (
SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = '{relation}'::regclass AND r.contype = 'c'
ORDER BY 1
) q
ORDER BY q.conname::text COLLATE "C"'''


check(psql_lookup("active_customers", "public"))
check(psql_relation("public.active_customers"))
check(psql_columns("public.active_customers", PSQL_COLUMNS, """
WHERE q.attname @NOT IN ('email')"""), "typmod")
for relation, differing, causes in (
        ("app.all_types", "('c_char', 'c_numeric_plain', 'c_text', 'c_varchar')", ("typmod", "numeric", "deparse")),
        ("public.customers", "('email', 'id', 'mood')", ("typmod", "deparse")),
        ("public.order_items", "('sku')", ("typmod",)),
        ("public.orders", "('status')", ("typmod",))):
    schema, name = relation.split(".")
    check(psql_lookup(name, schema))
    check(psql_relation(relation))
    if relation == "public.orders":
        check(psql_columns(relation, PSQL_COLUMNS.replace(" q.pg_get_expr,", ""), f"\nWHERE q.attname @NOT IN {differing}"),
              *causes)
        check(psql_columns(relation, "q.attname, q.pg_get_expr", "\nWHERE q.attname @NOT IN ('id', 'ordered_on', 'status', 'ticket')"),
              "deparse")
        check(psql_indexes(relation, "\nWHERE q.relname @NOT IN ('orders_amount_desc_idx')"), "index_desc")
    else:
        check(psql_columns(relation, PSQL_COLUMNS, f"\nWHERE q.attname @NOT IN {differing}"), *causes)
        check(psql_indexes(relation))
    if relation in ("public.order_items", "public.orders"):
        check(psql_checks(relation), "deparse")
    psql_table_tail(relation, references=relation != "app.all_types")
check(psql_lookup("orders_customer_idx", "public"))
check(psql_relation("public.orders_customer_idx"))
check("""SELECT q.attname, q.format_type, q.is_key, q.indexdef FROM (
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  CASE WHEN a.attnum <= (SELECT i.indnkeyatts FROM pg_catalog.pg_index i WHERE i.indexrelid = 'public.orders_customer_idx'::regclass) THEN 'yes' ELSE 'no' END AS is_key,
  pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'public.orders_customer_idx'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
) q""")
check("""SELECT q.indisunique, q.indisprimary, q.indisclustered, q.indisvalid, q.condeferrable, q.condeferred,
  q.indisreplident, q.indnullsnotdistinct, replace(q.amname, 'secondary', 'btree') AS amname, q.relname, q.pg_get_expr FROM (
SELECT i.indisunique, i.indisprimary, i.indisclustered, i.indisvalid,
  (NOT i.indimmediate) AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x') AND condeferrable) AS condeferrable,
  (NOT i.indimmediate) AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x') AND condeferred) AS condeferred,
i.indisreplident,
i.indnullsnotdistinct,
  a.amname, c2.relname, pg_catalog.pg_get_expr(i.indpred, i.indrelid, true)
FROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am a
WHERE i.indexrelid = c.oid AND c.oid = 'public.orders_customer_idx'::regclass AND c.relam = a.oid
AND i.indrelid = c2.oid
) q""")
check(psql_lookup("ticket_seq", "app"))
check(psql_relation("app.ticket_seq"))
check("""SELECT q."Type", q."Start", q."Minimum", q."Maximum", q."Increment", q."Cycles?", q."Cache" FROM (
SELECT pg_catalog.format_type(seqtypid, NULL) AS "Type",
       seqstart AS "Start",
       seqmin AS "Minimum",
       seqmax AS "Maximum",
       seqincrement AS "Increment",
       CASE WHEN seqcycle THEN 'yes' ELSE 'no' END AS "Cycles?",
       seqcache AS "Cache"
FROM pg_catalog.pg_sequence
WHERE seqrelid = 'app.ticket_seq'::regclass
) q""")
check("""SELECT q."?column?", q.deptype FROM (
SELECT pg_catalog.quote_ident(nspname) || '.' ||
   pg_catalog.quote_ident(relname) || '.' ||
   pg_catalog.quote_ident(attname),
   d.deptype
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_depend d ON c.oid=d.refobjid
INNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
INNER JOIN pg_catalog.pg_attribute a ON (
 a.attrelid=c.oid AND
 a.attnum=d.refobjsubid)
WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
 AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass
 AND d.objid='app.ticket_seq'::regclass
 AND d.deptype IN ('a', 'i')
) q""")
check("""SELECT q."Schema", q."Name", q."Result data type", q."Argument data types", q."Type" FROM (
SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
 CASE p.prokind
  WHEN 'a' THEN 'agg'
  WHEN 'w' THEN 'window'
  WHEN 'p' THEN 'proc'
  ELSE 'func'
 END as "Type"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname OPERATOR(pg_catalog.~) '^(app)$' COLLATE pg_catalog.default
ORDER BY 1, 2, 4
) q""")


def psql_listing(kinds, scope, joins="", tables=False):
    projected, selected = ((', q."Table"', ',\n  c2.relname as "Table"') if tables else ("", ""))
    return f'''SELECT q."Schema", q."Name", q."Type", q."Owner"{projected} FROM (
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"{selected}
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace{joins}
WHERE c.relkind IN ({kinds}){scope}
ORDER BY 1,2
) q
ORDER BY q."Name"::text COLLATE "C"'''


PSQL_VISIBLE = """
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)"""
PSQL_AM = "\n     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam"
check(psql_listing("'i','I',''", PSQL_VISIBLE,
                   PSQL_AM + "\n     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid"
                   "\n     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid", tables=True))
check('''SELECT q."Name", q."Owner" FROM (
SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1
) q
WHERE q."Name" @NOT IN ('public')
ORDER BY q."Name"::text COLLATE "C"''', "owner")
check(r'''SELECT q."Schema", q."Name", q."Type",
  trim(regexp_replace(q."Access privileges", '[[:space:]]+', ' ', 'g')) AS "Access privileges", q."Column privileges",
  q."Policies" FROM (
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END as "Type",
  pg_catalog.array_to_string(c.relacl, E'\n') AS "Access privileges",
  pg_catalog.array_to_string(ARRAY(
    SELECT attname || E':\n  ' || pg_catalog.array_to_string(attacl, E'\n  ')
    FROM pg_catalog.pg_attribute a
    WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL
  ), E'\n') AS "Column privileges",
  pg_catalog.array_to_string(ARRAY(
    SELECT polname
    || CASE WHEN NOT polpermissive THEN
       E' (RESTRICTIVE)'
       ELSE '' END
    || CASE WHEN polcmd != '*' THEN
           E' (' || polcmd::pg_catalog.text || E'):'
       ELSE E':'
       END
    || CASE WHEN polqual IS NOT NULL THEN
           E'\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)
       ELSE E''
       END
    || CASE WHEN polwithcheck IS NOT NULL THEN
           E'\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)
       ELSE E''
       END    || CASE WHEN polroles <> '{0}' THEN
           E'\n  to: ' || pg_catalog.array_to_string(
               ARRAY(
                   SELECT rolname
                   FROM pg_catalog.pg_roles
                   WHERE oid = ANY (polroles)
                   ORDER BY 1
               ), E', ')
       ELSE E''
       END
    FROM pg_catalog.pg_policy pol
    WHERE polrelid = c.oid), E'\n')
    AS "Policies"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','m','S','f','p')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1, 2
) q
ORDER BY q."Name"::text COLLATE "C"''')
check(psql_lookup("customers", "public"))
check(psql_relation("public.customers", verbose=True))
check(psql_relation("public.customers", True, "q.array_to_string"), "null_array")
check(psql_columns("public.customers", PSQL_COLUMNS.replace(" q.pg_get_expr,", "") + ", q.attstorage,\n  q.attcompression, "
                   "q.attstattarget, q.col_description", "\nWHERE q.attname @NOT IN ('email')", PSQL_TABLE_EXTRAS),
      "typmod")
check(psql_columns("public.customers", "q.attname, q.pg_get_expr", "\nWHERE q.attname @NOT IN ('id', 'mood')",
                   PSQL_TABLE_EXTRAS), "deparse")
check(psql_indexes("public.customers"))
psql_table_tail("public.customers")
check(psql_lookup("order_totals", "app"))
check(psql_relation("app.order_totals", verbose=True))
check(psql_relation("app.order_totals", True, "q.array_to_string"), "null_array")
check(psql_columns("app.order_totals", PSQL_COLUMNS + ", q.attstorage, q.col_description", "\nWHERE q.attname @NOT IN ('total')",
                   PSQL_VIEW_EXTRAS), "aggregate_type")
check(f"""SELECT {spaced("q.pg_get_viewdef", "pg_get_viewdef")} FROM (
SELECT pg_catalog.pg_get_viewdef('app.order_totals'::regclass::oid, true)
) q""", "deparse")
check("""SELECT q.rulename, q.rtrim FROM (
SELECT r.rulename, trim(trailing ';' from pg_catalog.pg_get_ruledef(r.oid, true))
FROM pg_catalog.pg_rewrite r
WHERE r.ev_class = 'app.order_totals'::regclass AND r.rulename != '_RETURN' ORDER BY 1
) q""")
check(psql_listing("'S','s',''", ""))
check(psql_listing("'r','p',''", PSQL_VISIBLE, PSQL_AM))
check(psql_listing("'r','p','t','s',''", "\n  AND n.nspname OPERATOR(pg_catalog.~) '^(app)$' COLLATE pg_catalog.default", PSQL_AM))
check(psql_listing("'v',''", PSQL_VISIBLE))

check("SELECT 1 AS one")
check("SELECT relname, relkind, relnatts, relhasindex FROM pg_catalog.pg_class WHERE oid = 'app.all_types'::regclass")
check("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'all_types' AND relnamespace = 'app'::regnamespace")
check("""SELECT attname, attnum, attnotnull, atthasdef FROM pg_catalog.pg_attribute
WHERE attrelid = 'app.all_types'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum""")
check("""SELECT indexrelid::regclass::text AS index, indnatts, indisprimary FROM pg_catalog.pg_index
WHERE indrelid = 'app.all_types'::regclass ORDER BY 1""")
check("SELECT conname, contype FROM pg_catalog.pg_constraint WHERE conrelid = 'public.orders'::regclass ORDER BY 1")
check("SELECT adnum FROM pg_catalog.pg_attrdef WHERE adrelid = 'public.orders'::regclass ORDER BY 1")
check("SELECT objsubid, description FROM pg_catalog.pg_description WHERE objoid = 'public.customers'::regclass ORDER BY 1")
check("""SELECT classid::regclass::text AS classid, deptype, count(*) AS n FROM pg_catalog.pg_depend
WHERE refobjid = 'public.customers'::regclass AND objid NOT IN (SELECT reltoastrelid FROM pg_catalog.pg_class)
GROUP BY 1, 2 ORDER BY 1, 2""")
check("SELECT nspname, oid = 'app'::regnamespace AS same_oid FROM pg_catalog.pg_namespace WHERE nspname = 'app'")
check("SELECT 'app.all_types'::regclass::text AS rel, 'app.all_types'::regclass::oid::regclass::text AS round_trip")
check("SELECT schemaname, relname FROM pg_catalog.pg_stat_user_tables WHERE relname = 'all_types' AND schemaname = 'app'")
check("SELECT count(pg_catalog.pg_get_constraintdef(oid)) AS n FROM pg_catalog.pg_constraint WHERE contype = 'p' AND oid >= 16384")
check("""SELECT count(i.indexrelid::regclass::text) AS n FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace NOT IN (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname IN ('pg_catalog', 'information_schema', 'pg_toast'))""")
check("""SELECT count(*) AS n FROM pg_catalog.pg_class
WHERE relnamespace NOT IN (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname IN ('pg_catalog', 'information_schema', 'pg_toast'))""")
check("SELECT count(*) AS n FROM pg_catalog.pg_description WHERE objoid = 1259 AND classoid = 1259")
check("""SELECT count(*) AS dangling FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_proc p ON p.oid = t.typinput
WHERE t.typinput <> 0 AND p.oid IS NULL""")
check("SELECT collname FROM pg_catalog.pg_collation WHERE oid IN (100, 950, 951) ORDER BY oid")
check("SELECT count(*) > 700 AS populated FROM information_schema.sql_features", "catalog_content")

t.statement("CREATE SCHEMA pgonly")
pgonly("CREATE DOMAIN pgonly.positive_int AS int4 CHECK (VALUE > 0)", 'syntax error at or near "DOMAIN"')
pgonly("CREATE TABLE pgonly.json_tab (id int4 PRIMARY KEY, doc jsonb)", "Type with name jsonb does not exist!")
pgonly("CREATE TABLE pgonly.domain_tab (id int4 PRIMARY KEY, qty pgonly.positive_int)",
       "Type with name positive_int does not exist!")
t.statement("CREATE TABLE pgonly.parent (id int4 PRIMARY KEY)")
pgonly("CREATE TABLE pgonly.cascade_child (id int4 PRIMARY KEY, parent_id int4 REFERENCES pgonly.parent (id) ON DELETE CASCADE)",
       "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT")
pgonly("""CREATE TABLE pgonly.identity_tab (
  id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code varchar(10) UNIQUE,
  qty int4 CHECK (qty BETWEEN 1 AND 100)
)""", 'syntax error at or near "IDENTITY"')
pgonly("CREATE FUNCTION pgonly.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
       'syntax error at or near "RETURN"')
pgonly("CREATE TRIGGER parent_touch BEFORE UPDATE ON pgonly.parent FOR EACH ROW EXECUTE FUNCTION pgonly.touch()",
       'syntax error at or near "EXECUTE"')
pgonly("ALTER TABLE pgonly.parent ENABLE ROW LEVEL SECURITY",
       "Pragma Function with name serenedb_alter_table_row_security does not exist!")
pgonly("CREATE POLICY parent_reader ON pgonly.parent FOR SELECT TO conf_reader USING (id > 0)",
       "Pragma Function with name serenedb_create_policy does not exist!")
pgonly("COMMENT ON SCHEMA pgonly IS 'Objects SereneDB cannot create yet'", "Adding comments to schemas is not implemented")
pgonly("COMMENT ON CONSTRAINT identity_tab_qty_check ON pgonly.identity_tab IS 'Allowed quantities'",
       'syntax error at or near "CONSTRAINT"')
pgonly("COMMENT ON FUNCTION pgonly.touch() IS 'Trigger function'", 'syntax error at or near "\\("')
pgonly("CREATE FOREIGN DATA WRAPPER conf_fdw", 'syntax error at or near "FOREIGN"')
pgonly("CREATE SERVER conf_srv FOREIGN DATA WRAPPER conf_fdw", "LOAD is not supported by SereneDB")

for dump in (pg_class, pg_attribute, pg_attribute_types, pg_type, pg_constraint, pg_sequence, pg_description, pg_depend,
             pg_shdepend, pg_trigger, pg_proc, columns, column_types):
    check(dump(PGONLY), "pgonly")
check(f'''SELECT polname, polrelid::regclass::text AS polrelid, polcmd, polpermissive,
  array_to_string(ARRAY(SELECT pg_catalog.pg_get_userbyid(x) FROM unnest(polroles) x), ',') AS polroles,
  pg_catalog.pg_get_expr(polqual, polrelid) AS polqual, pg_catalog.pg_get_expr(polwithcheck, polrelid) AS polwithcheck
FROM pg_catalog.pg_policy
WHERE polrelid IN {relations(PGONLY)}
ORDER BY polrelid::regclass::text COLLATE "C", polname::text COLLATE "C"''', "pgonly")
check(f'''SELECT fdwname, pg_catalog.pg_get_userbyid(fdwowner) AS fdwowner, fdwhandler::regproc::text AS fdwhandler,
  fdwvalidator::regproc::text AS fdwvalidator, {acl("fdwacl")}, fdwoptions
FROM pg_catalog.pg_foreign_data_wrapper
WHERE fdwname = 'conf_fdw'
ORDER BY fdwname::text COLLATE "C"''', "pgonly")
check(f'''SELECT srvname, pg_catalog.pg_get_userbyid(srvowner) AS srvowner,
  (SELECT x.fdwname::text FROM pg_catalog.pg_foreign_data_wrapper x WHERE x.oid = srvfdw) AS srvfdw, srvtype,
  srvversion, {acl("srvacl")}, srvoptions
FROM pg_catalog.pg_foreign_server
WHERE srvname = 'conf_srv'
ORDER BY srvname::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("domain_catalog")}, domain_schema, domain_name, data_type, character_maximum_length,
  character_octet_length, {db("character_set_catalog")}, character_set_schema, character_set_name,
  {db("collation_catalog")}, collation_schema, collation_name, numeric_precision, numeric_precision_radix,
  numeric_scale, datetime_precision, interval_type, interval_precision, domain_default, {db("udt_catalog")}, udt_schema,
  udt_name, {db("scope_catalog")}, scope_schema, scope_name, maximum_cardinality, dtd_identifier
FROM information_schema.domains
WHERE domain_schema IN {quoted(PGONLY)}
ORDER BY domain_schema::text COLLATE "C", domain_name::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, {db("domain_catalog")},
  domain_schema, domain_name, is_deferrable, initially_deferred
FROM information_schema.domain_constraints
WHERE domain_schema IN {quoted(PGONLY)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("constraint_catalog")}, constraint_schema, constraint_name, {db("unique_constraint_catalog")},
  unique_constraint_schema, unique_constraint_name, match_option, update_rule, delete_rule
FROM information_schema.referential_constraints
WHERE constraint_schema IN {quoted(PGONLY)}
ORDER BY constraint_schema::text COLLATE "C", constraint_name::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("trigger_catalog")}, trigger_schema, trigger_name, event_manipulation, {db("event_object_catalog")},
  event_object_schema, event_object_table, action_order, action_condition, action_statement, action_orientation,
  action_timing, action_reference_old_table, action_reference_new_table, action_reference_old_row,
  action_reference_new_row, created
FROM information_schema.triggers
WHERE trigger_schema IN {quoted(PGONLY)}
ORDER BY trigger_schema::text COLLATE "C", trigger_name::text COLLATE "C", event_manipulation::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("foreign_data_wrapper_catalog")}, foreign_data_wrapper_name, authorization_identifier, library_name,
  foreign_data_wrapper_language
FROM information_schema.foreign_data_wrappers
WHERE foreign_data_wrapper_name = 'conf_fdw'
ORDER BY foreign_data_wrapper_name::text COLLATE "C"''', "pgonly")
check(f'''SELECT {db("foreign_server_catalog")}, foreign_server_name, {db("foreign_data_wrapper_catalog")},
  foreign_data_wrapper_name, foreign_server_type, foreign_server_version, authorization_identifier
FROM information_schema.foreign_servers
WHERE foreign_server_name = 'conf_srv'
ORDER BY foreign_server_name::text COLLATE "C"''', "pgonly")
check("""SELECT
  (SELECT pg_catalog.pg_get_triggerdef(t.oid) FROM pg_catalog.pg_trigger t WHERE t.tgrelid = 'pgonly.parent'::regclass AND NOT t.tgisinternal) AS triggerdef,
  (SELECT c.relrowsecurity FROM pg_catalog.pg_class c WHERE c.oid = 'pgonly.parent'::regclass) AS row_security,
  (SELECT pg_catalog.pg_get_expr(p.polqual, p.polrelid) FROM pg_catalog.pg_policy p WHERE p.polrelid = 'pgonly.parent'::regclass) AS policy_qual,
  pg_catalog.obj_description('pgonly'::regnamespace, 'pg_namespace') AS schema_description""", "pgonly")
for sql in (
        "SELECT 'pgonly.positive_int'::regtype::text AS domain_regtype",
        "SELECT pg_catalog.format_type('pgonly.positive_int'::regtype, NULL) AS domain_format",
        "SELECT (SELECT pg_catalog.pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c WHERE c.contypid = "
        "'pgonly.positive_int'::regtype) AS domain_check",
        "SELECT (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a WHERE a.attrelid = "
        "'pgonly.json_tab'::regclass AND a.attname = 'doc') AS jsonb_column",
        "SELECT (SELECT pg_catalog.pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c WHERE c.conrelid = "
        "'pgonly.cascade_child'::regclass AND c.contype = 'f') AS cascade_fk",
        "SELECT pg_catalog.pg_get_serial_sequence('pgonly.identity_tab', 'id') AS identity_sequence",
        "SELECT (SELECT a.attidentity FROM pg_catalog.pg_attribute a WHERE a.attrelid = 'pgonly.identity_tab'::regclass "
        "AND a.attname = 'id') AS identity_column",
        "SELECT (SELECT pg_catalog.obj_description(c.oid, 'pg_constraint') FROM pg_catalog.pg_constraint c WHERE c.conrelid "
        "= 'pgonly.identity_tab'::regclass AND c.conname = 'identity_tab_qty_check') AS constraint_description",
        "SELECT " + spaced("pg_catalog.pg_get_functiondef('pgonly.touch()'::regprocedure)", "functiondef"),
        "SELECT pg_catalog.obj_description('pgonly.touch()'::regprocedure, 'pg_proc') AS function_description"):
    check(sql, "pgonly")
check(f'''SELECT q."name", q.format_type, q."default", q.not_null, q.table_name, q.comment, q.generated, q.identity_options FROM (
{sqlalchemy_columns("pg_catalog.pg_namespace.nspname = 'pgonly'", ("identity_tab",))}
) q
ORDER BY q."name"::text COLLATE "C", q.table_name::text COLLATE "C"''', "pgonly")
check(f'''SELECT q.attype, q.nullable, q."name", q.visible, q."schema", q."default", q.collname FROM (
{SQLALCHEMY_DOMAINS}
) q
WHERE q."schema" IN {quoted(PGONLY)}
ORDER BY q."name"::text COLLATE "C"''', "pgonly")
asyncpg_check("'app.mood'::regtype::oid, 'app.address'::regtype::oid, 'pgonly.positive_int'::regtype::oid, 1007", "",
              "pgonly")

pgonly("DROP SERVER conf_srv", "Foreign Server with name conf_srv does not exist!")
pgonly("DROP FOREIGN DATA WRAPPER conf_fdw", 'syntax error at or near "FOREIGN"')
for sql in ("DROP SCHEMA pgonly CASCADE", "DROP VIEW app.order_totals", "DROP VIEW active_customers",
            "DROP TABLE app.child_tab", "DROP TABLE app.unique_tab", "DROP TABLE app.json_tab", "DROP TABLE app.all_types",
            "DROP TABLE order_items", "DROP TABLE orders", "DROP TABLE customers", "DROP SEQUENCE plain_seq",
            "DROP SEQUENCE app.ticket_seq", "DROP FUNCTION app.add_one", "DROP TYPE app.address", "DROP TYPE app.mood",
            "DROP SCHEMA app", "DROP ROLE conf_reader"):
    t.statement(sql)

t.write()
