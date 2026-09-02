TABLE = "table"
SEQUENCE = "sequence"
VIEW = "view"
INDEX = "index"
TOKENIZER = "tokenizer"
SERVER = "server"
DATABASE = "database"
ATTACHMENT = "attachment"

# Kinds that cannot carry a COMMENT ON token, verified: COMMENT ON TEXT SEARCH
# DICTIONARY and COMMENT ON SERVER are both 42601, and pg_shdescription is empty.
# Their identity is proven by oid instead, which the oracle checks per incarnation.
# COMMENT ON DATABASE is 0A000, so a database has no token channel either.
OID_IDENTITY_KINDS = frozenset({TOKENIZER, SERVER, DATABASE})


class Op:
    __slots__ = ("kind", "statements", "creates", "drops", "scope", "token",
                 "key", "needs", "cascade", "rows_added", "rows_removed")

    def __init__(self, kind, statements, creates=(), drops=(), scope="private",
                 token=None, key=None, needs=(), cascade=(), rows_added=(),
                 rows_removed=()):
        self.kind = kind
        self.statements = list(statements)
        self.creates = list(creates)
        self.drops = list(drops)
        self.scope = scope
        self.token = token
        self.key = key
        self.needs = list(needs)
        self.cascade = list(cascade)
        self.rows_added = list(rows_added)
        self.rows_removed = list(rows_removed)

    def as_record(self):
        return {
            "kind": self.kind,
            "scope": self.scope,
            "key": list(self.key) if self.key else None,
            "token": self.token,
            "sql": self.statements[0][:120] if self.statements else "",
            "rows_added": self.rows_added or None,
            "rows_removed": self.rows_removed or None,
        }


class NameGen:
    def __init__(self, run_tag, worker, arena=8):
        self.run_tag = run_tag
        self.prefix = f"s{run_tag}_w{worker}"
        self.arena_size = arena
        self._n = 0

    def fresh(self, what):
        self._n += 1
        return f"{self.prefix}_{what}{self._n}"

    def token(self):
        self._n += 1
        return f"{self.prefix}_tok{self._n}"

    def pool(self, what, slot):
        return f"{self.prefix}_{what}{slot}"

    def shared(self, what, slot):
        return f"s{self.run_tag}_w0_{what}{9000 + slot}"


def key_of(kind, name):
    return (kind, name)


def create_table_named(names, name, serial=False, scope="private"):
    token = names.token()
    col = "id SERIAL PRIMARY KEY" if serial else "id INT PRIMARY KEY"
    guard = "IF NOT EXISTS " if scope == "shared" else ""
    stmts = [
        f"CREATE TABLE {guard}{name}({col}, v INT CHECK (v >= 0), label TEXT)",
        f"COMMENT ON TABLE {name} IS '{token}'",
    ]
    return Op("recreate_table" if scope == "private" else "create_table_shared",
              stmts, creates=[(key_of(TABLE, name), token)], token=token,
              key=key_of(TABLE, name), scope=scope)


def drop_table_named(name, scope="private"):
    guard = "IF EXISTS " if scope == "shared" else ""
    return Op("drop_table_shared" if scope == "shared" else "drop_table_reuse",
              [f"DROP TABLE {guard}{name} CASCADE"],
              drops=[key_of(TABLE, name)], key=key_of(TABLE, name), scope=scope)


def create_table(names, serial=False):
    name = names.fresh("t")
    token = names.token()
    col = "id SERIAL PRIMARY KEY" if serial else "id INT PRIMARY KEY"
    stmts = [
        f"CREATE TABLE {name}({col}, v INT CHECK (v >= 0), label TEXT)",
        f"COMMENT ON TABLE {name} IS '{token}'",
    ]
    rows = []
    if serial:
        rows = [names.token(), names.token()]
        vals = ", ".join(f"(1, '{r}')" for r in rows)
        stmts.append(f"INSERT INTO {name}(v, label) VALUES {vals}")
    return Op("create_table_serial" if serial else "create_table", stmts,
              creates=[(key_of(TABLE, name), token)], token=token,
              key=key_of(TABLE, name), rows_added=rows)


def slow_ctas(names, scan_rows=2000000000):
    name = names.fresh("t")
    token = names.token()
    row = names.token()
    # Same column shape as every other table the generator makes, so the DML ops
    # can target it; the cost is in the scan, not the result, which is one row.
    return Op("slow_ctas", [
        f"CREATE TABLE {name} AS SELECT count(*)::INT AS id, 0 AS v, "
        f"'{row}' AS label FROM generate_series(1, {scan_rows}) g",
        f"COMMENT ON TABLE {name} IS '{token}'",
    ], creates=[(key_of(TABLE, name), token)], token=token,
        key=key_of(TABLE, name), rows_added=[row])


def drop_table(key):
    name = key[1]
    return Op("drop_table", [f"DROP TABLE {name} CASCADE"], drops=[key],
              key=key)


def create_sequence(names):
    name = names.fresh("q")
    token = names.token()
    return Op("create_sequence", [
        f"CREATE SEQUENCE {name}",
        f"COMMENT ON SEQUENCE {name} IS '{token}'",
    ], creates=[(key_of(SEQUENCE, name), token)], token=token,
        key=key_of(SEQUENCE, name))


def nextval(key):
    name = key[1]
    return Op("nextval", [f"SELECT nextval('{name}')"], key=key)


def drop_sequence(key):
    name = key[1]
    return Op("drop_sequence", [f"DROP SEQUENCE {name}"], drops=[key], key=key)


def create_view(names, table_key):
    name = names.fresh("v")
    token = names.token()
    return Op("create_view", [
        f"CREATE VIEW {name} AS SELECT id, v FROM {table_key[1]}",
        f"COMMENT ON VIEW {name} IS '{token}'",
    ], creates=[(key_of(VIEW, name), token)], token=token,
        key=key_of(VIEW, name), needs=[table_key])


def drop_view(key):
    name = key[1]
    return Op("drop_view", [f"DROP VIEW {name}"], drops=[key], key=key)


def create_index(names, table_key):
    name = names.fresh("i")
    token = names.token()
    return Op("create_index", [
        f"CREATE INDEX {name} ON {table_key[1]}(v)",
        f"COMMENT ON INDEX {name} IS '{token}'",
    ], creates=[(key_of(INDEX, name), token)], token=token,
        key=key_of(INDEX, name), needs=[table_key])


def drop_index(key):
    name = key[1]
    return Op("drop_index", [f"DROP INDEX {name}"], drops=[key], key=key)


def dml_insert(names, table_key, has_serial):
    label = names.token()
    if has_serial:
        sql = f"INSERT INTO {table_key[1]}(v, label) VALUES (7, '{label}')"
    else:
        sql = (f"INSERT INTO {table_key[1]}(id, v, label) "
               f"SELECT COALESCE(max(id), 0) + 1, 7, '{label}' FROM {table_key[1]}")
    return Op("dml_insert", [sql], key=table_key, rows_added=[label])


def dml_update(table_key):
    return Op("dml_update",
              [f"UPDATE {table_key[1]} SET v = v + 1 WHERE v < 100"],
              key=table_key)


def dml_delete(table_key, label):
    return Op("dml_delete",
              [f"DELETE FROM {table_key[1]} WHERE label = '{label}'"],
              key=table_key, rows_removed=[label])


def read_table(table_key):
    return Op("read", [f"SELECT count(*) FROM {table_key[1]}"], key=table_key)


def create_tokenizer(names):
    name = names.fresh("d")
    return Op("create_tokenizer", [
        f"CREATE TEXT SEARCH DICTIONARY {name}("
        f"template = 'text', locale = 'en_US.UTF-8', case = 'lower', "
        f"stemming = false, accent = false)",
    ], creates=[(key_of(TOKENIZER, name), None)], key=key_of(TOKENIZER, name))


def drop_tokenizer(key):
    return Op("drop_tokenizer", [f"DROP TEXT SEARCH DICTIONARY {key[1]}"],
              drops=[key], key=key)


def create_iceberg_view(names, fixture_path):
    name = names.fresh("v")
    token = names.token()
    return Op("create_iceberg_view", [
        f"CREATE VIEW {name} AS SELECT * FROM iceberg_scan("
        f"'{fixture_path}', allow_moved_paths=true)",
        f"COMMENT ON VIEW {name} IS '{token}'",
    ], creates=[(key_of(VIEW, name), token)], token=token,
        key=key_of(VIEW, name))


def create_inverted_index(names, target_key, dict_key=None):
    name = names.fresh("i")
    token = names.token()
    cols = "body" if target_key[0] == VIEW else "label"
    spec = f"{cols} {dict_key[1]}" if dict_key else cols
    return Op("create_inverted_index", [
        f"CREATE INDEX {name} ON {target_key[1]} USING inverted({spec})",
        f"COMMENT ON INDEX {name} IS '{token}'",
    ], creates=[(key_of(INDEX, name), token)], token=token,
        key=key_of(INDEX, name), needs=[target_key] + ([dict_key] if dict_key else []))


def search_index(index_key):
    # A search predicate only lowers when the INDEX relation is the FROM target;
    # querying the base table raises 0A000. Verified.
    return Op("search_index",
              [f"SELECT count(*) FROM {index_key[1]} WHERE body @@ 'alpha'"],
              key=index_key)


def reindex_index(index_key):
    return Op("reindex_index", [f"REINDEX INDEX {index_key[1]}"], key=index_key)


def vacuum_refresh_index(index_key):
    return Op("vacuum_refresh_index",
              [f"VACUUM (REFRESH_INDEX) {index_key[1]}"], key=index_key)


def create_foreign_server(names, host, port):
    name = names.fresh("s")
    return Op("create_foreign_server", [
        f"CREATE SERVER {name} FOREIGN DATA WRAPPER postgres_fdw "
        f"OPTIONS (host '{host}', port '{port}', dbname 'postgres', "
        f"user 'postgres')",
    ], creates=[(key_of(SERVER, name), None)], key=key_of(SERVER, name))


def drop_foreign_server(key, scope="private"):
    guard = "IF EXISTS " if scope == "shared" else ""
    return Op("drop_foreign_server", [f"DROP SERVER {guard}{key[1]}"],
              drops=[key], key=key, scope=scope)


def create_database(names, scope="private"):
    name = names.fresh("b")
    guard = "IF NOT EXISTS " if scope == "shared" else ""
    return Op("create_database", [f"CREATE DATABASE {guard}{name}"],
              creates=[(key_of(DATABASE, name), None)],
              key=key_of(DATABASE, name), scope=scope)


def create_database_named(name, scope="shared"):
    return Op("create_database_shared", [f"CREATE DATABASE IF NOT EXISTS {name}"],
              creates=[(key_of(DATABASE, name), None)],
              key=key_of(DATABASE, name), scope=scope)


def drop_database(key, scope="private"):
    guard = "IF EXISTS " if scope == "shared" else ""
    return Op("drop_database", [f"DROP DATABASE {guard}{key[1]}"],
              drops=[key], key=key, scope=scope)


def cross_db_create_table(names, db_key, scope="shared"):
    name = names.fresh("t")
    return Op("cross_db_create_table", [
        f"CREATE TABLE IF NOT EXISTS {db_key[1]}.public.{name}(a INT)",
    ], key=db_key, scope=scope, needs=[db_key])


def cross_db_read(db_key, scope="shared"):
    return Op("cross_db_read",
              [f"SELECT count(*) FROM {db_key[1]}.information_schema.tables"],
              key=db_key, scope=scope)


def attach_duckdb(names, root, scope="shared"):
    name = names.fresh("a")
    path = f"{root}/{name}.duckdb"
    return Op("attach_duckdb", [f"ATTACH IF NOT EXISTS '{path}' AS {name}"],
              key=key_of(ATTACHMENT, name), scope=scope)


def detach_duckdb(key, scope="shared"):
    return Op("detach_duckdb", [f"DETACH IF EXISTS {key[1]}"],
              key=key, scope=scope)


def attachment_write(key, scope="shared"):
    return Op("attachment_write",
              [f"CREATE TABLE IF NOT EXISTS {key[1]}.main.t(a INT)"],
              key=key, scope=scope)


def server_in_use(key, scope="shared"):
    # Reads the catalog rows for a server that another worker may be dropping
    # underneath this statement.
    return Op("server_in_use", [
        f"SELECT count(*) FROM pg_foreign_server WHERE srvname = '{key[1]}'",
    ], key=key, scope=scope)


def create_foreign_server_named(name, host, port, scope="shared"):
    return Op("create_foreign_server_shared", [
        f"CREATE SERVER IF NOT EXISTS {name} FOREIGN DATA WRAPPER postgres_fdw "
        f"OPTIONS (host '{host}', port '{port}', dbname 'postgres', user 'postgres')",
    ], creates=[(key_of(SERVER, name), None)], key=key_of(SERVER, name), scope=scope)


def catalog_read():
    return Op("catalog_read", [
        "SELECT count(*) FROM pg_class WHERE relname LIKE 's%'",
    ], scope="shared")
