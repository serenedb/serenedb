import ops


class WorkerState:
    def __init__(self, names, table_cap=12, other_cap=8):
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
                if op.needs:
                    self.parent[key] = op.needs[0]
            elif kind == ops.INDEX:
                if key not in self.indexes:
                    self.indexes.append(key)
                if op.needs:
                    self.parent[key] = op.needs[0]

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
            for bucket in (self.tables, self.sequences, self.views, self.indexes):
                if key in bucket:
                    bucket.remove(key)
            self.serial.discard(key)
            self.parent.pop(key, None)
            self.rows.pop(key, None)

    def resync_from(self, model):
        present = model.expected_present()
        self.tables = [k for k in present if k[0] == ops.TABLE]
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


def pick_cancel_bait(rng, st):
    choices = [("slow_ctas", 6), ("catalog_read", 2)]
    if len(st.tables) < st.table_cap:
        choices.append(("create_table", 4))
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
        return ops.drop_view(rng.choice(st.views))
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
}


def resolve(name):
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario '{name}'; have {sorted(SCENARIOS)}")
    return SCENARIOS[name]
