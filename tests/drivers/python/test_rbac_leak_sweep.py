from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest

MARK = "zqhid"
TWIN = "zqnon"
MISSING_OID = 987654321


def setup_statements(workdir: Path) -> list[str]:
    return [
        "CREATE ROLE probe LOGIN PASSWORD 'probe'",
        "CREATE SCHEMA zqhid_schema",
        "CREATE TABLE zqhid_schema.zqhid_t_in_schema (zqhid_col_in_schema INTEGER)",
        "CREATE VIEW zqhid_schema.zqhid_v_in_schema AS SELECT 1 AS zqhid_vcol_in_schema",
        "CREATE SEQUENCE zqhid_schema.zqhid_seq_in_schema",
        "CREATE FUNCTION zqhid_schema.zqhid_f_in_schema(zqhid_arg INTEGER) RETURNS INTEGER LANGUAGE SQL "
        "AS 'SELECT zqhid_arg'",
        "CREATE TYPE zqhid_schema.zqhid_type_in_schema AS ENUM ('zqhid_label_in_schema')",
        "CREATE TYPE zqhid_schema.zqhid_comp_in_schema AS (zqhid_field INTEGER)",
        "CREATE TEXT SEARCH DICTIONARY zqhid_schema.zqhid_dict_in_schema AS split_text() "
        "WITH (frequency, position)",
        "CREATE SCHEMA zqhid_schema.zqhid_nested",
        "CREATE TABLE zqhid_schema.zqhid_nested.zqhid_t_nested (a INTEGER)",
        "CREATE TABLE public.zqhid_t (zqhid_col INTEGER PRIMARY KEY, zqhid_body TEXT, "
        "CONSTRAINT zqhid_check CHECK (zqhid_col > 0))",
        "CREATE TABLE public.zqhid_audit (zqhid_audit_col INTEGER)",
        "COMMENT ON TABLE public.zqhid_t IS 'zqhid_comment_table'",
        "COMMENT ON COLUMN public.zqhid_t.zqhid_col IS 'zqhid_comment_column'",
        "CREATE VIEW public.zqhid_v AS SELECT zqhid_col AS zqhid_vcol FROM public.zqhid_t",
        "COMMENT ON VIEW public.zqhid_v IS 'zqhid_comment_view'",
        "CREATE SEQUENCE public.zqhid_seq",
        "CREATE FUNCTION public.zqhid_f() RETURNS INTEGER LANGUAGE SQL AS 'SELECT 42'",
        "REVOKE EXECUTE ON FUNCTION public.zqhid_f() FROM PUBLIC",
        "COMMENT ON FUNCTION public.zqhid_f() IS 'zqhid_comment_function'",
        "CREATE PROCEDURE public.zqhid_p(zqhid_parg INTEGER) LANGUAGE SQL AS 'SELECT zqhid_parg'",
        "REVOKE EXECUTE ON PROCEDURE public.zqhid_p(INTEGER) FROM PUBLIC",
        "CREATE TYPE public.zqhid_type AS ENUM ('zqhid_label')",
        "REVOKE USAGE ON TYPE public.zqhid_type FROM PUBLIC",
        "CREATE TEXT SEARCH DICTIONARY public.zqvis_dict AS split_text() WITH (frequency, position)",
        "CREATE INDEX zqhid_idx ON public.zqhid_t USING inverted(zqhid_col, zqhid_body zqvis_dict)",
        "CREATE TRIGGER zqhid_trigger AFTER INSERT ON public.zqhid_t FOR EACH ROW "
        "INSERT INTO public.zqhid_audit VALUES (NEW.zqhid_col)",
        "CREATE JOB zqhid_job AFTER 1 HOUR SUSPENDED AS SELECT 'zqhid_job_body'",
        "CREATE SECRET zqhid_secret (TYPE s3, KEY_ID 'zqhid_key', SECRET 'zqhid_secret_value')",
        f"CREATE SERVER zqhid_server FOREIGN DATA WRAPPER duckdb_fdw "
        f"OPTIONS (path '{workdir}/zqhid_remote.duckdb')",
        "CREATE DATABASE zqhid_db",
        "REVOKE CONNECT ON DATABASE zqhid_db FROM PUBLIC",
        "GRANT CREATE, TEMPORARY ON DATABASE zqhid_db TO probe",
        "CREATE SCHEMA zqhid_db.zqhid_dbschema",
        "CREATE TABLE zqhid_db.public.zqhid_dbt (a INTEGER)",
        "CREATE SEQUENCE zqhid_db.public.zqhid_dbseq",
        "CREATE INDEX zqhid_dbidx ON zqhid_db.public.zqhid_dbt USING inverted(a)",
        "GRANT ALL ON SCHEMA zqhid_db.public, zqhid_db.zqhid_dbschema TO probe",
        "GRANT ALL ON zqhid_db.public.zqhid_dbt TO probe",
        "GRANT ALL ON SEQUENCE zqhid_db.public.zqhid_dbseq TO probe",
        "CALL es_create_index('zqhid_esidx', '{\"mappings\":{\"properties\":{\"a\":{\"type\":\"keyword\"}}}}')",
        "GRANT USAGE ON SCHEMA es TO probe",
        "CREATE SUBSCRIPTION zqhid_sub CONNECTION 'host=localhost dbname=zqhid_conn password=zqhid_pw' "
        "PUBLICATION zqhid_pubname WITH (connect = false, slot_name = NONE)",
        "INSERT INTO public.zqhid_t VALUES (1, 'zqhid_row_value')",
        "PREPARE zqhid_prepared AS SELECT 'zqhid_prepared_text'",
        "CREATE SCHEMA probe_schema AUTHORIZATION probe",
    ]


NAME_STATEMENTS = [
    "SELECT * FROM public.{m}_t", "SELECT * FROM {m}_schema.{m}_t_in_schema", "SELECT * FROM public.{m}_v",
    "SELECT * FROM public.{m}_seq", "SELECT * FROM {m}_schema.{m}_v_in_schema", "SELECT * FROM {m}_t",
    "SELECT * FROM {m}_schema.{m}_nested.{m}_t_nested", "SELECT * FROM public.{m}_audit",
    "INSERT INTO public.{m}_t VALUES (2, 'x')", "UPDATE public.{m}_t SET {m}_body = 'y'", "DELETE FROM public.{m}_t",
    "TRUNCATE public.{m}_t", "DROP TABLE public.{m}_t", "DROP TABLE {m}_t", "DROP TABLE IF EXISTS public.{m}_t",
    "DROP VIEW public.{m}_v", "DROP SEQUENCE public.{m}_seq", "ALTER TABLE public.{m}_t ADD COLUMN extra INTEGER",
    "ALTER TABLE public.{m}_t RENAME TO renamed", "ALTER TABLE public.{m}_t RENAME COLUMN {m}_col TO renamed",
    "ALTER TABLE public.{m}_t OWNER TO probe", "COMMENT ON TABLE public.{m}_t IS 'x'",
    "COMMENT ON COLUMN public.{m}_t.{m}_col IS 'x'", "GRANT SELECT ON public.{m}_t TO probe",
    "REVOKE SELECT ON public.{m}_t FROM probe", "SELECT nextval('public.{m}_seq')", "SELECT currval('public.{m}_seq')",
    "ALTER SEQUENCE public.{m}_seq RESTART", "SELECT public.{m}_f()", "SELECT {m}_f()",
    "SELECT {m}_schema.{m}_f_in_schema(1)", "CALL public.{m}_p(1)", "DROP FUNCTION public.{m}_f()",
    "DROP PROCEDURE public.{m}_p(INTEGER)", "ALTER FUNCTION public.{m}_f() RENAME TO renamed",
    "COMMENT ON FUNCTION public.{m}_f() IS 'x'", "GRANT EXECUTE ON FUNCTION public.{m}_f() TO probe",
    "SELECT 'a'::public.{m}_type", "SELECT NULL::{m}_schema.{m}_type_in_schema", "DROP TYPE public.{m}_type",
    "CREATE TABLE probe_schema.probe_t (a public.{m}_type)", "SELECT 'public.{m}_t'::regclass",
    "SELECT to_regclass('public.{m}_t')", "SELECT 'public.{m}_f'::regproc", "SELECT to_regproc('public.{m}_f')",
    "SELECT 'public.{m}_f()'::regprocedure", "SELECT 'public.{m}_type'::regtype", "SELECT to_regtype('public.{m}_type')",
    "SELECT '{m}_schema'::regnamespace", "SELECT to_regnamespace('{m}_schema')",
    "SELECT has_table_privilege('public.{m}_t', 'SELECT')", "SELECT has_any_column_privilege('public.{m}_t', 'SELECT')",
    "SELECT has_column_privilege('public.{m}_t', '{m}_col', 'SELECT')",
    "SELECT has_function_privilege('public.{m}_f()', 'EXECUTE')", "SELECT has_schema_privilege('{m}_schema', 'USAGE')",
    "SELECT has_sequence_privilege('public.{m}_seq', 'USAGE')", "SELECT has_type_privilege('public.{m}_type', 'USAGE')",
    "SELECT has_database_privilege('{m}_db', 'CONNECT')", "SELECT has_foreign_server_privilege('{m}_server', 'USAGE')",
    "SELECT pg_table_size('public.{m}_t')", "SELECT pg_get_viewdef('public.{m}_v')",
    "SELECT pg_get_serial_sequence('public.{m}_t', '{m}_col')", "SELECT obj_description('public.{m}_t'::regclass)",
    "DESCRIBE public.{m}_t", "SELECT * FROM pragma_table_info('public.{m}_t')", "PRAGMA table_info('public.{m}_t')",
    "EXPLAIN SELECT * FROM public.{m}_t", "COPY public.{m}_t TO '/dev/null'",
    "CREATE INDEX probe_idx ON public.{m}_t USING inverted({m}_col)", "DROP SCHEMA {m}_schema",
    "ALTER SCHEMA {m}_schema RENAME TO renamed", "COMMENT ON SCHEMA {m}_schema IS 'x'",
    "CREATE TABLE {m}_schema.probe_t (a INTEGER)", "DROP DATABASE {m}_db", "ALTER DATABASE {m}_db RENAME TO renamed",
    "COMMENT ON DATABASE {m}_db IS 'x'", "SELECT pg_database_size('{m}_db')", "USE {m}_db",
    "SELECT * FROM {m}_db.public.something", "DROP SERVER {m}_server", "ALTER SERVER {m}_server OPTIONS (SET path 'x')",
    "DROP INDEX public.{m}_idx", "ALTER INDEX public.{m}_idx RENAME TO renamed",
    "DROP TEXT SEARCH DICTIONARY {m}_schema.{m}_dict_in_schema", "DROP TRIGGER {m}_trigger ON public.{m}_t",
    "DROP JOB {m}_job", "ALTER JOB {m}_job RESUME", "DROP SECRET {m}_secret",
    "SELECT * FROM which_secret('s3://{m}/x', 's3')", "DROP SUBSCRIPTION {m}_sub", "ALTER SUBSCRIPTION {m}_sub DISABLE",
    "EXECUTE {m}_prepared", "DEALLOCATE {m}_prepared", "SET search_path = {m}_schema, public", "RESET search_path",
    "SELECT * FROM {m}_idx", "SELECT * FROM public.{m}_idx", "SELECT * FROM {m}_db.public.{m}_dbt",
    "INSERT INTO {m}_db.public.{m}_dbt VALUES (1)", "UPDATE {m}_db.public.{m}_dbt SET a = 2",
    "DELETE FROM {m}_db.public.{m}_dbt", "TRUNCATE {m}_db.public.{m}_dbt", "DROP TABLE {m}_db.public.{m}_dbt",
    "DROP TABLE IF EXISTS {m}_db.public.{m}_dbt", "ALTER TABLE {m}_db.public.{m}_dbt ADD COLUMN b INTEGER",
    "COMMENT ON TABLE {m}_db.public.{m}_dbt IS 'x'", "VACUUM {m}_db.public.{m}_dbt",
    "VACUUM (FLUSH_TABLE) {m}_db.public.{m}_dbt", "ANALYZE {m}_db.public.{m}_dbt", "REINDEX TABLE {m}_db.public.{m}_dbt",
    "REINDEX INDEX {m}_db.public.{m}_dbidx", "DROP INDEX {m}_db.public.{m}_dbidx",
    "CREATE INDEX probe_idx ON {m}_db.public.{m}_dbt USING inverted(a)",
    "CREATE TABLE {m}_db.public.probe_t (a INTEGER)", "CREATE VIEW {m}_db.public.probe_v AS SELECT 1 AS a",
    "CREATE SEQUENCE {m}_db.public.probe_seq", "CREATE SCHEMA {m}_db.probe_schema",
    "ALTER SCHEMA {m}_db.{m}_dbschema RENAME TO renamed", "DROP SCHEMA {m}_db.{m}_dbschema",
    "COMMENT ON SCHEMA {m}_db.{m}_dbschema IS 'x'", "GRANT USAGE ON SCHEMA {m}_db.{m}_dbschema TO probe",
    "SET search_path TO {m}_db.public", "RESET search_path", "SET search_path TO public, {m}_db.public",
    "RESET search_path", "USE {m}_db.public", "PRAGMA table_info('{m}_db.public.{m}_dbt')",
    "DESCRIBE {m}_db.public.{m}_dbt", "SELECT * FROM seq_scan(['{m}_db', 'public', '{m}_dbt'])",
    "SELECT '{m}_db.public.{m}_dbt'::regclass", "SELECT has_table_privilege('{m}_db.public.{m}_dbt', 'SELECT')",
    "SELECT nextval('{m}_db.public.{m}_dbseq')", "SELECT * FROM {m}_db.information_schema.tables",
    "GRANT CONNECT ON DATABASE {m}_db TO probe", "ALTER DATABASE {m}_db OWNER TO probe", "DETACH {m}_db",
    "CALL es_refresh('{m}_esidx')", "CALL es_drop_index('{m}_esidx')", "CALL es_mapping('{m}_esidx')",
    "SELECT * FROM es_doc('{m}_esidx', '1', '{{\"a\":\"1\"}}')",
    "GRANT USAGE ON FOREIGN SERVER {m}_server TO probe", "ALTER SERVER {m}_server OWNER TO probe",
    "ALTER SCHEMA {m}_schema OWNER TO probe", "ALTER FUNCTION public.{m}_f() OWNER TO probe",
    "ALTER PROCEDURE public.{m}_p(INTEGER) OWNER TO probe", "ALTER TYPE public.{m}_type OWNER TO probe",
    "ALTER SEQUENCE public.{m}_seq OWNER TO probe", "ALTER VIEW public.{m}_v OWNER TO probe",
    "ALTER SUBSCRIPTION {m}_sub OWNER TO probe", "ALTER TEXT SEARCH DICTIONARY {m}_schema.{m}_dict_in_schema OWNER TO probe",
    "GRANT USAGE ON TYPE public.{m}_type TO probe", "GRANT USAGE ON SEQUENCE public.{m}_seq TO probe",
    "GRANT USAGE ON SCHEMA {m}_schema TO probe", "GRANT EXECUTE ON PROCEDURE public.{m}_p(INTEGER) TO probe",
    "REVOKE CONNECT ON DATABASE {m}_db FROM probe", "ALTER DATABASE {m}_db SET search_path = public",
    "ALTER DATABASE {m}_db RESET ALL", "ALTER DATABASE {m}_db WITH CONNECTION LIMIT 5",
    "ALTER ROLE probe IN DATABASE {m}_db SET search_path = public",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA {m}_schema GRANT SELECT ON TABLES TO probe",
    "VACUUM public.{m}_t", "ANALYZE public.{m}_t", "VACUUM (FLUSH_TABLE) public.{m}_t", "REINDEX TABLE public.{m}_t",
    "REINDEX INDEX public.{m}_idx", "COMMENT ON INDEX public.{m}_idx IS 'x'", "COMMENT ON SEQUENCE public.{m}_seq IS 'x'",
    "COMMENT ON TYPE public.{m}_type IS 'x'", "COMMENT ON SERVER {m}_server IS 'x'",
    "COMMENT ON SUBSCRIPTION {m}_sub IS 'x'", "COMMENT ON TRIGGER {m}_trigger ON public.{m}_t IS 'x'",
    "COMMENT ON CONSTRAINT {m}_check ON public.{m}_t IS 'x'", "ALTER TRIGGER {m}_trigger ON public.{m}_t RENAME TO renamed",
    "ALTER TABLE public.{m}_t DROP CONSTRAINT {m}_check", "ALTER TABLE public.{m}_t ALTER COLUMN {m}_col SET DEFAULT 1",
    "ALTER TABLE public.{m}_t SET SCHEMA probe_schema", "ALTER TYPE public.{m}_type ADD VALUE 'x'",
    "ALTER TYPE public.{m}_type RENAME TO renamed", "ALTER SEQUENCE public.{m}_seq RENAME TO renamed",
    "ALTER VIEW public.{m}_v RENAME TO renamed", "CREATE OR REPLACE VIEW public.{m}_v AS SELECT 1 AS a",
    "CREATE TABLE public.{m}_t (a INTEGER)", "SELECT setval('public.{m}_seq', 1)", "SUMMARIZE public.{m}_t",
    "SELECT {m}_col FROM public.{m}_t", "DROP SCHEMA {m}_schema CASCADE", "USE {m}_schema",
    "ATTACH ':memory:' AS {m}_db", "SELECT pg_get_indexdef('public.{m}_idx'::regclass)",
    "DROP FUNCTION IF EXISTS public.{m}_f()", "DROP PROCEDURE IF EXISTS public.{m}_p(INTEGER)",
    "DROP VIEW IF EXISTS public.{m}_v", "DROP SEQUENCE IF EXISTS public.{m}_seq", "DROP TYPE IF EXISTS public.{m}_type",
    "DROP SCHEMA IF EXISTS {m}_schema", "DROP INDEX IF EXISTS public.{m}_idx", "DROP SERVER IF EXISTS {m}_server",
    "DROP SUBSCRIPTION IF EXISTS {m}_sub", "DROP DATABASE IF EXISTS {m}_db", "DROP JOB IF EXISTS {m}_job",
    "DROP TRIGGER IF EXISTS {m}_trigger ON public.{m}_t", "DROP SECRET IF EXISTS {m}_secret",
    "DROP TEXT SEARCH DICTIONARY IF EXISTS {m}_schema.{m}_dict_in_schema", "COPY public.{m}_t TO STDOUT",
    "COPY (SELECT * FROM public.{m}_t) TO STDOUT", "TABLE public.{m}_t", "SELECT nextval('{m}_schema.{m}_seq_in_schema')",
    "CREATE TABLE probe_schema.probe_fk (a INTEGER REFERENCES public.{m}_t ({m}_col))",
    "CREATE VIEW probe_schema.probe_view AS SELECT * FROM public.{m}_t",
    "SELECT ts_lexize('{m}_schema.{m}_dict_in_schema', 'x')",
    "SELECT ts_lexize(d, 'x') FROM (VALUES ('{m}_schema.{m}_dict_in_schema')) AS t(d)",
    "SELECT to_tsvector('{m}_schema.{m}_dict_in_schema', 'x')",
]

OID_LOOKUPS = {
    "class": ["SELECT {o}::regclass::text", "SELECT pg_get_viewdef({o})", "SELECT obj_description({o}, 'pg_class')",
              "SELECT col_description({o}, 1)", "SELECT pg_describe_object('pg_class'::regclass, {o}, 0)",
              "SELECT * FROM pg_identify_object('pg_class'::regclass, {o}, 0)", "SELECT pg_table_size({o})",
              "SELECT pg_total_relation_size({o})", "SELECT pg_relation_filepath({o})", "SELECT pg_get_indexdef({o})",
              "SELECT has_table_privilege({o}, 'SELECT')", "SELECT has_any_column_privilege({o}, 'SELECT')",
              "SELECT pg_get_acl('pg_class'::regclass, {o}, 0)", "SELECT pg_sequence_last_value({o})",
              "SELECT * FROM pg_sequence_parameters({o})"],
    "proc": ["SELECT {o}::regproc::text", "SELECT {o}::regprocedure::text", "SELECT pg_get_functiondef({o})",
             "SELECT pg_get_function_arguments({o})", "SELECT pg_get_function_result({o})",
             "SELECT pg_get_function_identity_arguments({o})", "SELECT obj_description({o}, 'pg_proc')",
             "SELECT pg_describe_object('pg_proc'::regclass, {o}, 0)", "SELECT has_function_privilege({o}, 'EXECUTE')",
             "SELECT pg_get_acl('pg_proc'::regclass, {o}, 0)"],
    "type": ["SELECT {o}::regtype::text", "SELECT format_type({o}, NULL)", "SELECT has_type_privilege({o}, 'USAGE')",
             "SELECT pg_describe_object('pg_type'::regclass, {o}, 0)",
             "SELECT * FROM pg_identify_object('pg_type'::regclass, {o}, 0)"],
    "namespace": ["SELECT {o}::regnamespace::text", "SELECT has_schema_privilege({o}, 'USAGE')",
                  "SELECT pg_describe_object('pg_namespace'::regclass, {o}, 0)",
                  "SELECT obj_description({o}, 'pg_namespace')"],
    "database": ["SELECT has_database_privilege({o}, 'CONNECT')", "SELECT pg_database_size({o})",
                 "SELECT pg_describe_object('pg_database'::regclass, {o}, 0)",
                 "SELECT shobj_description({o}, 'pg_database')", "SELECT pg_get_acl('pg_database'::regclass, {o}, 0)"],
    "server": ["SELECT has_foreign_server_privilege({o}, 'USAGE')",
               "SELECT pg_describe_object('pg_foreign_server'::regclass, {o}, 0)"],
    "constraint": ["SELECT pg_get_constraintdef({o})", "SELECT pg_describe_object('pg_constraint'::regclass, {o}, 0)"],
    "trigger": ["SELECT pg_get_triggerdef({o})", "SELECT pg_describe_object('pg_trigger'::regclass, {o}, 0)"],
    "dictionary": ["SELECT {o}::regdictionary::text", "SELECT pg_describe_object('pg_ts_dict'::regclass, {o}, 0)"],
    "subscription": ["SELECT pg_describe_object('pg_subscription'::regclass, {o}, 0)"],
}

OIDS = {
    "class": ["SELECT 'public.zqhid_t'::regclass::oid", "SELECT 'public.zqhid_v'::regclass::oid",
              "SELECT 'public.zqhid_seq'::regclass::oid", "SELECT 'zqhid_schema.zqhid_t_in_schema'::regclass::oid",
              "SELECT oid FROM pg_class WHERE relname = 'zqhid_idx'"],
    "proc": ["SELECT 'public.zqhid_f'::regproc::oid", "SELECT oid FROM pg_proc WHERE proname = 'zqhid_p'",
             "SELECT oid FROM pg_proc WHERE proname = 'zqhid_f_in_schema'"],
    "type": ["SELECT 'public.zqhid_type'::regtype::oid",
             "SELECT oid FROM pg_type WHERE typname = 'zqhid_type_in_schema'"],
    "namespace": ["SELECT 'zqhid_schema'::regnamespace::oid"],
    "database": ["SELECT oid FROM pg_database WHERE datname = 'zqhid_db'"],
    "server": ["SELECT oid FROM pg_foreign_server WHERE srvname = 'zqhid_server'"],
    "constraint": ["SELECT oid FROM pg_constraint WHERE conname = 'zqhid_check'"],
    "trigger": ["SELECT oid FROM pg_trigger WHERE tgname = 'zqhid_trigger'"],
    "dictionary": ["SELECT oid FROM pg_ts_dict WHERE dictname = 'zqhid_dict_in_schema'"],
    "subscription": ["SELECT oid FROM pg_subscription WHERE subname = 'zqhid_sub'"],
}

HINT_STATEMENTS = [
    "SELECT * FROM public.zqhid_tt", "SELECT * FROM public.zqhid_", "SELECT zqhid_ff()", "SELECT public.zqhid_ff()",
    "SELECT * FROM zqhid_schema_.zqhid_t_in_schema", "SELECT NULL::zqhid_typ", "SELECT * FROM zqhid_vv",
    "SELECT nextval('zqhid_seqq')", "DROP DATABASE zqhid_dbb", "SELECT 'zqhid_tt'::regclass",
    "SELECT zqhid_bod FROM public.zqhid_t", "SELECT * FROM zqhid_t_in_schemaa",
]

SKIPPED_TABLE_FUNCTIONS = {"checkpoint", "force_checkpoint"}


@dataclass
class Session:
    conn: psycopg.Connection
    notices: list[str] = field(default_factory=list)

    def run(self, sql: str) -> tuple[str, str]:
        self.notices.clear()
        try:
            cur = self.conn.execute(sql)
            rows = cur.fetchall() if cur.description else []
            text = repr(rows)
            if self.notices:
                text += " NOTICE: " + " ; ".join(self.notices)
            return "ok", text
        except psycopg.Error as error:
            diag = error.diag
            parts = (diag.message_primary, diag.message_detail, diag.message_hint)
            text = " | ".join(part for part in parts if part) or str(error)
            if self.notices:
                text += " NOTICE: " + " ; ".join(self.notices)
            return "error", text


def connect(port: int, user: str, password: str | None = None, dbname: str = "postgres") -> Session:
    conn = psycopg.connect(host="127.0.0.1", port=port, user=user, password=password, dbname=dbname,
                           autocommit=True)
    session = Session(conn)
    conn.add_notice_handler(
        lambda diag: session.notices.append(
            " | ".join(part for part in (diag.message_primary, diag.message_detail) if part)))
    return session


def normalize(text: str) -> str:
    text = re.sub(r"\nDid you mean \"[^\"]*\"\?", "", text)
    text = text.replace(MARK, "<name>").replace(TWIN, "<name>")
    return re.sub(r"\b\d{5,}\b", "<oid>", text)


@dataclass
class Sweep:
    port: int
    superuser: Session
    probe: Session
    relations: list[tuple[str, str]]
    table_functions: list[str]
    oids: dict[str, list[int]]


@pytest.fixture(scope="module")
def sweep(own_server_module, tmp_path_factory) -> Sweep:
    port = own_server_module.port
    superuser = connect(port, "postgres")
    workdir = tmp_path_factory.mktemp("leak_sweep")
    for sql in setup_statements(workdir):
        assert superuser.run(sql)[0] == "ok", sql
    activity = connect(port, "postgres")
    activity.run("SELECT 'zqhid_activity_text'")
    relations = superuser.conn.execute(
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('pg_catalog', 'information_schema') AND c.relkind IN ('r', 'v', 'm', 'p', 'f') "
        "ORDER BY 1, 2").fetchall()
    table_functions = [
        name for name, no_arguments in superuser.conn.execute(
            "SELECT function_name, bool_or(len(parameters) = 0) FROM sdb_functions() "
            "WHERE function_type = 'table' GROUP BY function_name ORDER BY 1").fetchall()
        if no_arguments and name not in SKIPPED_TABLE_FUNCTIONS
    ]
    oids = {kind: [superuser.conn.execute(sql).fetchone()[0] for sql in sqls] for kind, sqls in OIDS.items()}
    probe = connect(port, "probe", "probe")
    yield Sweep(port, superuser, probe, relations, table_functions, oids)
    probe.conn.close()
    activity.conn.close()
    superuser.conn.close()


def test_listings_show_no_hidden_object(sweep):
    surfaces = [(f"{schema}.{name}", f'SELECT * FROM "{schema}"."{name}"') for schema, name in sweep.relations]
    surfaces += [(f"{name}()", f'SELECT * FROM "{name}"()') for name in sweep.table_functions]
    surfaces += [(sql, sql) for sql in ("SHOW TABLES", "SHOW ALL TABLES", "SHOW DATABASES", "SHOW SCHEMAS",
                                        "SELECT current_schemas(true)", "SELECT * FROM pg_settings")]
    assert len(sweep.relations) > 100 and len(sweep.table_functions) > 20
    leaks = []
    for surface, sql in surfaces:
        status, text = sweep.probe.run(sql)
        if MARK in text:
            leaks.append(f"{surface}: {text[:300]}")
    assert not leaks, "\n".join(leaks)


SURVIVORS = [
    "SELECT count(*) = 1 FROM pg_namespace WHERE nspname = 'zqhid_schema'",
    "SELECT count(*) = 1 FROM pg_database WHERE datname = 'zqhid_db'",
    "SELECT count(*) = 1 FROM sdb_schemas() WHERE database_name = 'zqhid_db' AND schema_name = 'zqhid_dbschema'",
    "SELECT count(*) = 1 FROM sdb_tables() WHERE database_name = 'zqhid_db' AND table_name = 'zqhid_dbt'",
    "SELECT count(*) = 0 FROM sdb_tables() WHERE database_name = 'zqhid_db' AND table_name LIKE 'probe%'",
    "SELECT count(*) = 0 FROM sdb_views() WHERE database_name = 'zqhid_db' AND view_name LIKE 'probe%'",
    "SELECT count(*) = 1 FROM sdb_indexes() WHERE database_name = 'zqhid_db' AND index_name = 'zqhid_dbidx'",
    "SELECT count(*) = 1 FROM pg_tables WHERE schemaname = 'es' AND tablename = 'zqhid_esidx'",
    "SELECT count(*) = 1 FROM sdb_jobs() WHERE job_name = 'zqhid_job'",
    "SELECT count(*) = 1 FROM sdb_secrets() WHERE name = 'zqhid_secret'",
    "SELECT count(*) = 1 FROM pg_foreign_server WHERE srvname = 'zqhid_server'",
    "SELECT count(*) = 1 FROM pg_subscription WHERE subname = 'zqhid_sub'",
    "SELECT count(*) = 4 FROM pg_class WHERE relname IN ('zqhid_t', 'zqhid_v', 'zqhid_seq', 'zqhid_idx')",
    "SELECT count(*) = 0 FROM pg_class WHERE relname IN ('renamed', 'probe_idx')",
    "SELECT count(*) = 2 FROM pg_proc WHERE proname IN ('zqhid_f', 'zqhid_p')",
    "SELECT count(*) = 1 FROM pg_type WHERE typname = 'zqhid_type'",
    "SELECT count(*) = 1 FROM pg_trigger WHERE tgname = 'zqhid_trigger'",
    "SELECT count(*) = 1 FROM pg_ts_dict WHERE dictname = 'zqhid_dict_in_schema'",
    "SELECT count(*) = 2 FROM information_schema.columns WHERE table_name = 'zqhid_t'",
    "SELECT count(*) = 1 FROM public.zqhid_t WHERE zqhid_body = 'zqhid_row_value'",
    "SELECT obj_description('public.zqhid_t'::regclass, 'pg_class') = 'zqhid_comment_table'",
    "SELECT col_description('public.zqhid_t'::regclass, 1) = 'zqhid_comment_column'",
    "SELECT obj_description('zqhid_schema'::regnamespace, 'pg_namespace') IS NULL",
    "SELECT shobj_description(oid, 'pg_database') IS NULL FROM pg_database WHERE datname = 'zqhid_db'",
    "SELECT tableowner = 'postgres' FROM pg_tables WHERE tablename = 'zqhid_t'",
    "SELECT NOT has_table_privilege('probe', 'public.zqhid_t', 'SELECT')",
    "SELECT NOT has_function_privilege('probe', 'public.zqhid_f()', 'EXECUTE')",
]


def test_hidden_names_answer_as_missing(sweep):
    differences = []
    for template in NAME_STATEMENTS:
        hidden = sweep.probe.run(template.format(m=MARK))
        missing = sweep.probe.run(template.format(m=TWIN))
        if (hidden[0], normalize(hidden[1])) != (missing[0], normalize(missing[1])):
            differences.append(f"{template.format(m=MARK)}\n  hidden:  {hidden}\n  missing: {missing}")
    assert not differences, "\n".join(differences)
    for sql in SURVIVORS:
        assert sweep.superuser.run(sql) == ("ok", "[(True,)]"), sql


def test_hidden_oids_answer_as_missing(sweep):
    differences = []
    for kind, oids in sweep.oids.items():
        for oid in oids:
            for template in OID_LOOKUPS[kind]:
                hidden = sweep.probe.run(template.format(o=oid))
                missing = sweep.probe.run(template.format(o=MISSING_OID))
                hidden_text = normalize(hidden[1]).replace(str(oid), "<oid>")
                missing_text = normalize(missing[1]).replace(str(MISSING_OID), "<oid>")
                if (hidden[0], hidden_text) != (missing[0], missing_text) or MARK in hidden[1]:
                    differences.append(f"{template.format(o=oid)}\n  hidden:  {hidden}\n  missing: {missing}")
    assert not differences, "\n".join(differences)


def test_hints_suggest_no_hidden_object(sweep):
    leaks = []
    for sql in HINT_STATEMENTS:
        status, text = sweep.probe.run(sql)
        if status == "error" and set(re.findall(r"zqhid_\w+", text)) - set(re.findall(r"zqhid_\w+", sql)):
            leaks.append(f"{sql}: {text}")
    assert not leaks, "\n".join(leaks)


def test_drop_counts_hidden_dependents(sweep):
    for sql in ("CREATE TABLE probe_schema.probe_own (a INTEGER)",
                "CREATE TABLE probe_schema.probe_own2 (a INTEGER)"):
        assert sweep.probe.run(sql)[0] == "ok", sql
    for sql in ("CREATE VIEW public.zqhid_dep_view AS SELECT a AS zqhid_dep_col FROM probe_schema.probe_own",
                "CREATE VIEW public.zqhid_dep_view2 AS SELECT a FROM probe_schema.probe_own2"):
        assert sweep.superuser.run(sql)[0] == "ok", sql
    assert sweep.probe.run("DROP TABLE probe_schema.probe_own") == (
        "error",
        "cannot drop table probe_schema.probe_own because other objects depend on it"
        " | 1 other object depends on table probe_schema.probe_own"
        " | Use DROP ... CASCADE to drop the dependent objects too.")
    assert sweep.probe.run("DROP TABLE probe_schema.probe_own2 CASCADE") == (
        "ok", "[] NOTICE: drop cascades to 1 other object")


def test_search_path_into_revoked_database_answers_as_dropped(sweep):
    answers = []
    for db, cut in (("zqrev_db", "REVOKE CONNECT ON DATABASE zqrev_db FROM PUBLIC"), ("zqcut_db", "DROP DATABASE zqcut_db")):
        for sql in (f"CREATE DATABASE {db}", f"CREATE TABLE {db}.public.zqsp_t (a INTEGER)",
                    f"GRANT SELECT ON {db}.public.zqsp_t TO probe"):
            assert sweep.superuser.run(sql)[0] == "ok", sql
        session = connect(sweep.port, "probe", "probe")
        try:
            assert session.run(f"SET search_path TO {db}.public")[0] == "ok"
            assert session.run("SELECT count(*) FROM zqsp_t") == ("ok", "[(0,)]")
            assert sweep.superuser.run(cut)[0] == "ok", cut
            status, text = session.run("SELECT count(*) FROM zqsp_t")
            answers.append((status, text.replace(db, "<db>")))
        finally:
            session.conn.close()
    assert answers[0] == answers[1], answers
    assert answers[0][0] == "error", answers


def test_connecting_to_hidden_database_answers_missing(sweep):
    answers = []
    for dbname in ("zqhid_db", "zqnon_db"):
        try:
            connect(sweep.port, "probe", "probe", dbname).conn.close()
            answers.append("connected")
        except psycopg.Error as error:
            answers.append(normalize(str(error)))
    assert answers[0] == answers[1]
