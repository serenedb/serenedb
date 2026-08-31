TABLE = "table"
SEQUENCE = "sequence"
VIEW = "view"
INDEX = "index"


class Op:
    __slots__ = ("kind", "statements", "creates", "drops", "scope", "token",
                 "key", "needs", "cascade")

    def __init__(self, kind, statements, creates=(), drops=(), scope="private",
                 token=None, key=None, needs=(), cascade=()):
        self.kind = kind
        self.statements = list(statements)
        self.creates = list(creates)
        self.drops = list(drops)
        self.scope = scope
        self.token = token
        self.key = key
        self.needs = list(needs)
        self.cascade = list(cascade)

    def as_record(self):
        return {
            "kind": self.kind,
            "scope": self.scope,
            "key": list(self.key) if self.key else None,
            "token": self.token,
            "sql": self.statements[0][:120] if self.statements else "",
        }


class NameGen:
    def __init__(self, run_tag, worker):
        self.prefix = f"s{run_tag}_w{worker}"
        self._n = 0

    def fresh(self, what):
        self._n += 1
        return f"{self.prefix}_{what}{self._n}"

    def token(self):
        self._n += 1
        return f"{self.prefix}_tok{self._n}"


def key_of(kind, name):
    return (kind, name)


def create_table(names, serial=False):
    name = names.fresh("t")
    token = names.token()
    col = "id SERIAL PRIMARY KEY" if serial else "id INT PRIMARY KEY"
    stmts = [
        f"CREATE TABLE {name}({col}, v INT CHECK (v >= 0), label TEXT)",
        f"COMMENT ON TABLE {name} IS '{token}'",
    ]
    if serial:
        stmts.append(f"INSERT INTO {name}(v, label) VALUES (1, '{token}'), (2, '{token}')")
    return Op("create_table_serial" if serial else "create_table", stmts,
              creates=[(key_of(TABLE, name), token)], token=token,
              key=key_of(TABLE, name))


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
    return Op("dml_insert", [sql], key=table_key)


def dml_update(table_key):
    return Op("dml_update", [f"UPDATE {table_key[1]} SET v = v + 1 WHERE v < 100"],
              key=table_key)


def dml_delete(table_key):
    return Op("dml_delete", [f"DELETE FROM {table_key[1]} WHERE v > 100"],
              key=table_key)


def read_table(table_key):
    return Op("read", [f"SELECT count(*) FROM {table_key[1]}"], key=table_key)


def catalog_read():
    return Op("catalog_read", [
        "SELECT count(*) FROM pg_class WHERE relname LIKE 's%'",
    ], scope="shared")
