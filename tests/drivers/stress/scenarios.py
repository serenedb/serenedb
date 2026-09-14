import ops


class WorkerState:
    def __init__(self, names, table_cap=12, other_cap=8, env=None):
        self.names = names
        self.table_cap = table_cap
        self.other_cap = other_cap
        self.tables = []
        self.sequences = []
        self.views = []
        self.indexes = []
        self.serial = set()
        self.parent = {}
        self.rows = {}
        self.env = env or {}
        self.tokenizers = []
        self.servers = []
        self.databases = []
        self.attachments = []
        # break_everything mixes populations the single-purpose scenarios each
        # assume are uniform: a ddl_churn view is SELECT id, v with no body
        # column, an iceberg view has one, and an ART index is over label. The
        # searchable set and the per-index column have to be tracked, or the
        # iceberg ops target the wrong objects.
        self.iceberg_views = []
        self.search_col = {}
        self.uses_dict = {}

    def note_created(self, op):
        for key, _token in op.creates:
            kind = key[0]
            if kind == ops.TABLE:
                if key not in self.tables:
                    self.tables.append(key)
                self.rows[key] = set(op.rows_added)
                if op.kind == "create_table_serial":
                    self.serial.add(key)
            elif kind == ops.SEQUENCE:
                if key not in self.sequences:
                    self.sequences.append(key)
            elif kind == ops.VIEW:
                if key not in self.views:
                    self.views.append(key)
                if op.kind == "create_iceberg_view" \
                        and key not in self.iceberg_views:
                    self.iceberg_views.append(key)
                if op.needs:
                    self.parent[key] = op.needs[0]
            elif kind == ops.INDEX:
                if key not in self.indexes:
                    self.indexes.append(key)
                if op.needs:
                    self.parent[key] = op.needs[0]
                for need in op.needs[1:]:
                    if need[0] == ops.TOKENIZER:
                        self.uses_dict[key] = need
                if op.search_column:
                    self.search_col[key] = op.search_column
            elif kind == ops.TOKENIZER:
                if key not in self.tokenizers:
                    self.tokenizers.append(key)
            elif kind == ops.SERVER:
                if key not in self.servers:
                    self.servers.append(key)
            elif kind == ops.DATABASE:
                if key not in self.databases:
                    self.databases.append(key)

    def note_rows(self, op):
        if op.key is None or op.key[0] != ops.TABLE:
            return
        bucket = self.rows.get(op.key)
        if bucket is None:
            return
        bucket.difference_update(op.rows_removed)
        bucket.update(op.rows_added)

    def a_row_of(self, rng, table_key):
        bucket = sorted(self.rows.get(table_key) or ())
        return rng.choice(bucket) if bucket else None

    def note_dropped(self, op):
        for key in list(op.drops) + list(op.cascade):
            for bucket in (self.tables, self.sequences, self.views, self.indexes,
                           self.tokenizers, self.servers, self.databases,
                           self.attachments):
                if key in bucket:
                    bucket.remove(key)
            self.serial.discard(key)
            self.parent.pop(key, None)
            self.rows.pop(key, None)
            self.uses_dict.pop(key, None)
            self.search_col.pop(key, None)
            if key in self.iceberg_views:
                self.iceberg_views.remove(key)

    def resync_from(self, model):
        present = model.expected_present()
        self.tables = [k for k in present if k[0] == ops.TABLE]
        self.tokenizers = [k for k in present if k[0] == ops.TOKENIZER]
        self.servers = [k for k in present if k[0] == ops.SERVER]
        self.databases = [k for k in present if k[0] == ops.DATABASE]
        self.iceberg_views = [k for k in self.iceberg_views if k in present]
        self.search_col = {k: v for k, v in self.search_col.items() if k in present}
        self.uses_dict = {k: v for k, v in self.uses_dict.items()
                          if k in present and v in present}
        self.sequences = [k for k in present if k[0] == ops.SEQUENCE]
        self.views = [k for k in present if k[0] == ops.VIEW]
        self.indexes = [k for k in present if k[0] == ops.INDEX]
        live = set(present)
        self.serial = {k for k in self.serial if k in live}
        self.parent = {k: v for k, v in self.parent.items()
                       if k in live and v in live}
        self.rows = {}
        for key, state in present.items():
            if key[0] == ops.TABLE:
                self.rows[key] = set(getattr(state, "rows", ()) or ())
        return len(live)

    def note_attachment(self, op):
        if op.kind == "attach_duckdb" and op.key not in self.attachments:
            self.attachments.append(op.key)
        elif op.kind == "detach_duckdb" and op.key in self.attachments:
            self.attachments.remove(op.key)

    def searchable_indexes(self):
        return [k for k in self.indexes if k in self.search_col]

    def free_tokenizers(self):
        # DROP TEXT SEARCH DICTIONARY is refused with 2BP01 while any index still
        # references it, and its CASCADE form is a syntax error, so a tokenizer is
        # only droppable once every index over it is gone.
        referenced = set(self.uses_dict.values())
        return [k for k in self.tokenizers if k not in referenced]

    def dependents_of(self, table_key):
        return [k for k, parent in self.parent.items() if parent == table_key]

    def has_serial(self, table_key):
        return table_key in self.serial


def _table_ops(rng, st, serial_weight):
    choices = []
    if len(st.tables) < st.table_cap:
        choices.append(("create_table", 10))
        if serial_weight:
            choices.append(("create_table_serial", serial_weight))
    if st.tables:
        choices.append(("drop_table", 8 if len(st.tables) > 2 else 2))
    return choices


def pick_ddl_churn(rng, st):
    choices = _table_ops(rng, st, serial_weight=6)
    if len(st.sequences) < st.other_cap:
        choices.append(("create_sequence", 5))
    if st.sequences:
        choices.append(("nextval", 6))
        choices.append(("drop_sequence", 4))
    if st.tables and len(st.views) < st.other_cap:
        choices.append(("create_view", 4))
    if st.views:
        choices.append(("drop_view", 3))
    if st.tables and len(st.indexes) < st.other_cap:
        choices.append(("create_index", 4))
    if st.indexes:
        choices.append(("drop_index", 3))
    choices.append(("catalog_read", 2))
    return _build(rng, st, rng.weighted(choices))


def pick_serial_churn(rng, st):
    choices = []
    if len(st.tables) < st.table_cap:
        choices.append(("create_table_serial", 12))
    if st.tables:
        choices.append(("drop_table", 10))
        choices.append(("dml_insert", 4))
    if len(st.sequences) < st.other_cap:
        choices.append(("create_sequence", 6))
    if st.sequences:
        choices.append(("nextval", 8))
        choices.append(("drop_sequence", 5))
    return _build(rng, st, rng.weighted(choices))


def pick_ddl_dml_race(rng, st):
    choices = _table_ops(rng, st, serial_weight=5)
    if st.tables:
        choices.extend([("dml_insert", 10), ("dml_update", 6),
                        ("dml_delete", 4), ("read", 5)])
    if st.tables and len(st.indexes) < st.other_cap:
        choices.append(("create_index", 5))
    if st.indexes:
        choices.append(("drop_index", 4))
    choices.append(("catalog_read", 2))
    return _build(rng, st, rng.weighted(choices))


def pick_iceberg_views(rng, st):
    fixtures = st.env.get("iceberg_fixtures") or ()
    choices = [("catalog_read", 2)]
    if not st.tokenizers:
        choices.append(("create_tokenizer", 20))
    elif len(st.tokenizers) < 3:
        choices.append(("create_tokenizer", 3))
    if fixtures and len(st.iceberg_views) < st.other_cap:
        choices.append(("create_iceberg_view", 8))
    if st.iceberg_views:
        choices.append(("drop_iceberg_view", 3))
        if st.tokenizers and len(st.indexes) < st.other_cap:
            choices.append(("create_inverted_index", 8))
    if st.searchable_indexes():
        choices.extend([("search_index", 6), ("reindex_index", 4),
                        ("vacuum_refresh_index", 4), ("drop_search_index", 4)])
    if st.free_tokenizers():
        choices.append(("drop_tokenizer", 2))
    return _build(rng, st, rng.weighted(choices))


def pick_foreign_servers(rng, st):
    choices = [("catalog_read", 2)]
    if len(st.servers) < st.other_cap:
        choices.append(("create_foreign_server", 10))
    if st.servers:
        choices.append(("drop_foreign_server", 8))
    if len(st.tables) < st.table_cap:
        choices.append(("create_table", 4))
    if st.tables:
        choices.append(("drop_table", 3))
    return _build(rng, st, rng.weighted(choices))


def pick_attach_churn(rng, st):
    root = st.env.get("attach_root")
    choices = [("catalog_read", 2)]
    if len(st.databases) < 6:
        choices.append(("create_database", 8))
    if st.databases:
        choices.extend([("drop_database", 6), ("cross_db_create_table", 6),
                        ("cross_db_read", 4)])
    if root and len(st.attachments) < 4:
        choices.append(("attach_duckdb", 6))
    if st.attachments:
        choices.extend([("attachment_write", 5), ("detach_duckdb", 5)])
    return _build(rng, st, rng.weighted(choices))


def pick_server_race(rng, st):
    # Every worker races on the SAME few server names, so create / drop / use of a
    # live server overlap by construction. The keys are shared, so the model makes
    # no existence claim about them; a crash, a hang or an unclassified error is
    # still a finding.
    slot = rng.below(3)
    name = st.names.shared("sr", slot)
    key = ops.key_of(ops.SERVER, name)
    host = st.env.get("host", "127.0.0.1")
    port = st.env.get("port", 5432)
    pick = rng.weighted([("create", 5), ("drop", 4), ("use", 6), ("read", 2)])
    if pick == "create":
        return ops.create_foreign_server_named(name, host, port)
    if pick == "drop":
        return ops.drop_foreign_server(key, scope="shared")
    if pick == "use":
        return ops.server_in_use(key)
    return ops.catalog_read()


def pick_break_everything(rng, st):
    # Everything at once, on shared names wherever a race is the point.
    lane = rng.weighted([
        ("ddl", 8), ("dml", 5), ("index", 5), ("db", 4), ("attach", 3),
        ("server", 3), ("iceberg", 3), ("maint", 2), ("read", 2),
    ])
    if lane == "ddl":
        return pick_ddl_churn(rng, st)
    if lane == "dml" and st.tables:
        return _reuse_side(rng, st)
    if lane == "index" and st.tables:
        if st.indexes and rng.fraction() < 0.4:
            return ops.drop_index(rng.choice(st.indexes))
        if len(st.indexes) < st.other_cap:
            return ops.create_index(st.names, rng.choice(st.tables))
    if lane == "db":
        return pick_attach_churn(rng, st)
    if lane == "attach" and st.env.get("attach_root"):
        return pick_attach_churn(rng, st)
    if lane == "server":
        return pick_server_race(rng, st)
    if lane == "iceberg" and st.env.get("iceberg_fixtures"):
        return pick_iceberg_views(rng, st)
    if lane == "maint" and st.searchable_indexes():
        return rng.choice([ops.vacuum_refresh_index, ops.reindex_index])(
            rng.choice(st.searchable_indexes()))
    return ops.catalog_read()


def pick_cancel_bait(rng, st):
    choices = [("catalog_read", 2)]
    if len(st.tables) < st.table_cap:
        choices.extend([("slow_ctas", 6), ("create_table", 4)])
    if st.tables:
        choices.extend([("drop_table", 6), ("dml_insert", 3), ("read", 2)])
    return _build(rng, st, rng.weighted(choices))


def pick_name_reuse(rng, st):
    slot = rng.below(4)
    name = st.names.pool("rt", slot)
    key = ops.key_of(ops.TABLE, name)
    live = key in st.tables
    if live and rng.fraction() < 0.55:
        return ops.drop_table_named(name)
    if not live:
        return ops.create_table_named(st.names, name,
                                      serial=rng.fraction() < 0.4)
    if st.tables:
        return _reuse_side(rng, st)
    return ops.catalog_read()


def _reuse_side(rng, st):
    key = rng.choice(st.tables)
    if rng.fraction() < 0.6:
        return ops.dml_insert(st.names, key, st.has_serial(key))
    return ops.read_table(key)


def pick_shared_arena(rng, st):
    slot = rng.below(st.names.arena_size)
    name = st.names.shared("sh", slot)
    key = ops.key_of(ops.TABLE, name)
    if rng.fraction() < 0.5:
        return ops.create_table_named(st.names, name, scope="shared")
    return ops.drop_table_named(name, scope="shared")


def pick_tables_only(rng, st):
    choices = _table_ops(rng, st, serial_weight=0)
    if st.tables:
        choices.extend([("dml_insert", 8), ("dml_update", 5), ("read", 4)])
    if st.tables and len(st.views) < st.other_cap:
        choices.append(("create_view", 5))
    if st.views:
        choices.append(("drop_view", 4))
    if st.tables and len(st.indexes) < st.other_cap:
        choices.append(("create_index", 5))
    if st.indexes:
        choices.append(("drop_index", 4))
    choices.append(("catalog_read", 2))
    return _build(rng, st, rng.weighted(choices))


def pick_dependency_churn(rng, st):
    choices = _table_ops(rng, st, serial_weight=3)
    if st.tables and len(st.views) < st.other_cap:
        choices.append(("create_view", 10))
    if st.views:
        choices.append(("drop_view", 6))
    if st.tables and len(st.indexes) < st.other_cap:
        choices.append(("create_index", 8))
    if st.indexes:
        choices.append(("drop_index", 5))
    choices.append(("catalog_read", 3))
    return _build(rng, st, rng.weighted(choices))


def _build(rng, st, what):
    n = st.names
    if what == "slow_ctas":
        return ops.slow_ctas(n)
    if what == "create_table":
        return ops.create_table(n, serial=False)
    if what == "create_table_serial":
        return ops.create_table(n, serial=True)
    if what == "drop_table":
        key = rng.choice(st.tables)
        op = ops.drop_table(key)
        op.cascade = st.dependents_of(key)
        return op
    if what == "create_sequence":
        return ops.create_sequence(n)
    if what == "nextval":
        return ops.nextval(rng.choice(st.sequences))
    if what == "drop_sequence":
        return ops.drop_sequence(rng.choice(st.sequences))
    if what == "create_view":
        return ops.create_view(n, rng.choice(st.tables))
    if what == "drop_view":
        key = rng.choice(st.views)
        op = ops.drop_view(key)
        # DROP VIEW silently takes every inverted index over that view with it --
        # no CASCADE asked for, no warning, and the index's pg_description row
        # goes too. The model has to know, or every later op against that index
        # reports a vanished private key.
        op.cascade = st.dependents_of(key)
        return op
    if what == "create_index":
        return ops.create_index(n, rng.choice(st.tables))
    if what == "drop_index":
        return ops.drop_index(rng.choice(st.indexes))
    if what == "dml_insert":
        key = rng.choice(st.tables)
        return ops.dml_insert(n, key, st.has_serial(key))
    if what == "dml_update":
        return ops.dml_update(rng.choice(st.tables))
    if what == "dml_delete":
        key = rng.choice(st.tables)
        label = st.a_row_of(rng, key)
        if label is None:
            return ops.read_table(key)
        return ops.dml_delete(key, label)
    if what == "read":
        return ops.read_table(rng.choice(st.tables))
    if what == "create_tokenizer":
        return ops.create_tokenizer(n)
    if what == "drop_tokenizer":
        return ops.drop_tokenizer(rng.choice(st.free_tokenizers()))
    if what == "create_iceberg_view":
        return ops.create_iceberg_view(n, rng.choice(st.env["iceberg_fixtures"]))
    if what == "create_inverted_index":
        return ops.create_inverted_index(
            n, rng.choice(st.iceberg_views), rng.choice(st.tokenizers),
            column="body")
    if what == "drop_iceberg_view":
        key = rng.choice(st.iceberg_views)
        op = ops.drop_view(key)
        op.cascade = st.dependents_of(key)
        return op
    if what == "drop_search_index":
        return ops.drop_index(rng.choice(st.searchable_indexes()))
    if what == "search_index":
        key = rng.choice(st.searchable_indexes())
        return ops.search_index(key, st.search_col[key])
    if what == "reindex_index":
        return ops.reindex_index(rng.choice(st.searchable_indexes()))
    if what == "vacuum_refresh_index":
        return ops.vacuum_refresh_index(rng.choice(st.searchable_indexes()))
    if what == "create_foreign_server":
        return ops.create_foreign_server(n, st.env.get("host", "127.0.0.1"),
                                         st.env.get("port", 5432))
    if what == "drop_foreign_server":
        return ops.drop_foreign_server(rng.choice(st.servers))
    if what == "create_database":
        return ops.create_database(n)
    if what == "drop_database":
        return ops.drop_database(rng.choice(st.databases))
    if what == "cross_db_create_table":
        return ops.cross_db_create_table(n, rng.choice(st.databases))
    if what == "cross_db_read":
        return ops.cross_db_read(rng.choice(st.databases))
    if what == "attach_duckdb":
        return ops.attach_duckdb(n, st.env["attach_root"])
    if what == "detach_duckdb":
        return ops.detach_duckdb(rng.choice(st.attachments))
    if what == "attachment_write":
        return ops.attachment_write(rng.choice(st.attachments))
    if what == "catalog_read":
        return ops.catalog_read()
    raise AssertionError(f"unhandled op {what}")


SCENARIOS = {
    "ddl_churn": pick_ddl_churn,
    "ddl_dml_race": pick_ddl_dml_race,
    "serial_churn": pick_serial_churn,
    "dependency_churn": pick_dependency_churn,
    "tables_only": pick_tables_only,
    "name_reuse": pick_name_reuse,
    "shared_arena": pick_shared_arena,
    "cancel_bait": pick_cancel_bait,
    "iceberg_views": pick_iceberg_views,
    "foreign_servers": pick_foreign_servers,
    "attach_churn": pick_attach_churn,
    "server_race": pick_server_race,
    "break_everything": pick_break_everything,
}


def resolve(name):
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario '{name}'; have {sorted(SCENARIOS)}")
    return SCENARIOS[name]
