import json
import os
import random
import threading
import time

import psycopg

import ops as ops_mod

VOCAB = (
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
    "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
    "yankee", "zulu", "pudge", "anchin", "techies", "mines", "courier", "blink",
)
CATEGORIES = ("hero", "item", "spell", "unit")
WORDS_PER_DOC = 3
READ_JOBS = (("search_bm25", 20), ("search_ann", 20), ("search_hybrid", 10),
             ("lookup", 8), ("facet", 4), ("count_filtered", 3))
WRITE_JOBS = (("ingest_batch", 15), ("delete_docs", 4), ("update_docs", 3),
              ("metrics_probe", 1))
# One ingest in BULK_EVERY is a bulk load: BULK_ROWS rows arriving as several
# files, the way a real backfill lands. Same op kind as an ordinary batch, so
# a run whose cap blocks the big ones does not look like it lost coverage.
BULK_EVERY = 8
BULK_ROWS = (8000, 12000)
BULK_CHUNK = 2000


class WorkloadOp(ops_mod.Op):
    __slots__ = ("apply",)

    def __init__(self, kind, statements, apply=None):
        super().__init__(kind, statements)
        self.apply = apply


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def words_for(doc_id, ver):
    return [VOCAB[(doc_id * 7 + ver * 3 + k) % len(VOCAB)] for k in range(WORDS_PER_DOC)]


def body_for(doc_id, ver):
    return f"doc {doc_id} " + " ".join(words_for(doc_id, ver))


def body_sql(id_expr, ver_expr):
    vocab = "[" + ", ".join(lit(w) for w in VOCAB) + "]"
    parts = [f"{vocab}[(({id_expr}) * 7 + ({ver_expr}) * 3 + {k}) % {len(VOCAB)} + 1]"
             for k in range(WORDS_PER_DOC)]
    return f"'doc ' || ({id_expr}) || ' ' || " + " || ' ' || ".join(parts)


def emb_sql(id_expr, ver_expr, dim):
    return f"[sin(({id_expr}) * 0.1 + i * 0.01 + ({ver_expr}))::FLOAT FOR i IN range({dim})]"


def vec_sql(doc_id, ver, dim):
    return f"{emb_sql(str(doc_id), str(ver), dim)}::FLOAT[{dim}]"


def category_sql(id_expr):
    cases = " ".join(f"WHEN {i} THEN {lit(c)}" for i, c in enumerate(CATEGORIES))
    return f"CASE ({id_expr}) % {len(CATEGORIES)} {cases} END"


def backend_sql(backend, env):
    if backend == "biglake":
        project = os.environ["BIGLAKE_PROJECT"]
        catalog = os.environ["BIGLAKE_CATALOG"]
        service_account = None
        if os.environ.get("BIGLAKE_CLIENT_EMAIL"):
            service_account = (os.environ["BIGLAKE_CLIENT_EMAIL"],
                               os.environ["BIGLAKE_PRIVATE_KEY"],
                               os.environ.get("BIGLAKE_PRIVATE_KEY_ID", ""))
        else:
            adc_path = os.environ.get(
                "GOOGLE_APPLICATION_CREDENTIALS",
                os.path.expanduser("~/.config/gcloud/application_default_credentials.json"))
            with open(adc_path) as fh:
                adc = json.load(fh)
            if adc.get("type") == "service_account":
                service_account = (adc["client_email"], adc["private_key"],
                                   adc.get("private_key_id", ""))
        if service_account:
            email, key, key_id = service_account
            body = (f"TYPE ICEBERG, PROVIDER google, "
                    f"CLIENT_EMAIL {lit(email)}, PRIVATE_KEY {lit(key)}, "
                    f"PRIVATE_KEY_ID {lit(key_id)}, "
                    f"EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {lit(project)}}}")
        else:
            body = (f"TYPE ICEBERG, OAUTH2_GRANT_TYPE 'refresh_token', "
                    f"OAUTH2_SERVER_URI 'https://oauth2.googleapis.com/token', "
                    f"CLIENT_ID {lit(adc['client_id'])}, CLIENT_SECRET {lit(adc['client_secret'])}, "
                    f"REFRESH_TOKEN {lit(adc['refresh_token'])}, "
                    f"EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {lit(project)}}}")
        bootstrap = f"CREATE OR REPLACE PERSISTENT SECRET iceberg_ci_catalog ({body})"
        options = (f"warehouse {lit(f'bl://projects/{project}/catalogs/{catalog}')}, "
                   f"endpoint 'https://biglake.googleapis.com/iceberg/v1/restcatalog', "
                   f"secret 'iceberg_ci_catalog'")
        return bootstrap, options
    bootstrap = (f"CREATE OR REPLACE PERSISTENT SECRET iceberg_ci_storage (TYPE S3, "
                 f"KEY_ID {lit(env['MINIO_ACCESS_KEY'])}, SECRET {lit(env['MINIO_SECRET_KEY'])}, "
                 f"ENDPOINT {lit(env['MINIO_HOST'] + ':' + str(env['MINIO_PORT']))}, "
                 f"URL_STYLE 'path', USE_SSL false, "
                 f"SCOPE {lit('s3://' + env['MINIO_BUCKET'] + '/warehouse/')})")
    options = (f"warehouse {lit(env['ICEBERG_WAREHOUSE'])}, "
               f"endpoint {lit(env['ICEBERG_REST_URL'])}, authorization_type 'none'")
    return bootstrap, options


def worker_index(state):
    return int(state.names.prefix.rsplit("_w", 1)[1])


class Workload:
    def __init__(self, dsn, run_tag, dim, seed_docs, docs_cap, backend, env,
                 compaction_interval=None):
        self.dsn = dsn
        self.dim = dim
        self.seed_docs = seed_docs
        self.docs_cap = docs_cap
        self.compaction_interval = compaction_interval
        self.server = f"gd{run_tag}"
        self.schema = f"gd_{run_tag}"
        self.table = f"{self.server}.{self.schema}.docs"
        self.view = f"gd{run_tag}_v"
        self.index = f"gd{run_tag}_idx"
        self.dictionary = f"gd{run_tag}_en"
        self.bootstrap, self.server_options = backend_sql(backend, env)
        self.docs = {}
        self.next_id = 1
        self.lock = threading.RLock()
        self.checks = 0
        self.summary = {"ingested": 0, "deleted": 0, "updated": 0, "rebuilds": 0, "checks": 0}

    def connect(self):
        conn = psycopg.connect(self.dsn)
        conn.autocommit = True
        return conn

    def create_index_sql(self):
        options = "reindex_interval=500"
        if self.compaction_interval is not None:
            options += f", compaction_interval={self.compaction_interval}"
        return (f"CREATE INDEX {self.index} ON {self.view} USING inverted("
                f"id, body {self.dictionary}, emb ivf (metric = 'l2')) "
                f"WITH ({options})")

    def ingest_sql(self, first, count):
        return (f"INSERT INTO {self.table} SELECT s::INTEGER, {category_sql('s')}, "
                f"{body_sql('s', '0')}, 0, {emb_sql('s', '0', self.dim)} "
                f"FROM generate_series({first}, {first + count - 1}) t(s)")

    def setup(self):
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(self.bootstrap)
            cur.execute(f"CREATE SERVER {self.server} FOREIGN DATA WRAPPER iceberg_fdw "
                        f"OPTIONS ({self.server_options})")
            cur.execute(f"CREATE SCHEMA {self.server}.{self.schema}")
            cur.execute(f"CREATE TABLE {self.table} (id INTEGER, category TEXT, body TEXT, "
                        f"ver INTEGER, emb FLOAT[])")
            cur.execute(f"CREATE TEXT SEARCH DICTIONARY {self.dictionary} AS "
                        f"split_text() | normalize_tokens('en_US.UTF-8', accent := false) "
                        f"WITH (frequency, position)")
            cur.execute(self.ingest_sql(1, self.seed_docs))
            cur.execute(f"CREATE VIEW {self.view} AS SELECT id, category, body, ver, "
                        f"emb::FLOAT[{self.dim}] AS emb FROM {self.table}")
            cur.execute(self.create_index_sql())
        with self.lock:
            self.docs = {i: 0 for i in range(1, self.seed_docs + 1)}
            self.next_id = self.seed_docs + 1

    def teardown(self):
        try:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute(f"DROP INDEX IF EXISTS {self.index}")
                cur.execute(f"DROP VIEW IF EXISTS {self.view}")
                cur.execute(f"DROP TABLE IF EXISTS {self.table}")
                cur.execute(f"DROP SCHEMA IF EXISTS {self.server}.{self.schema}")
                cur.execute(f"DROP SERVER IF EXISTS {self.server}")
        except Exception:
            pass

    def is_writer(self, state):
        return worker_index(state) == 0

    def pick(self, rng, state):
        choices = list(READ_JOBS)
        if self.is_writer(state):
            choices.extend(WRITE_JOBS)
        return self.build(rng.weighted(choices), rng)

    def _sample_alive(self, rng, n):
        with self.lock:
            ids = sorted(self.docs)
        if not ids:
            return []
        return sorted({rng.choice(ids) for _ in range(n)})

    def _one(self, rng):
        with self.lock:
            ids = sorted(self.docs)
            if not ids:
                return 1, 0
            k = rng.choice(ids)
            return k, self.docs[k]

    def build(self, kind, rng):
        if kind == "ingest_batch":
            bulk = rng.below(BULK_EVERY) == 0
            n = (BULK_ROWS[0] + rng.below(BULK_ROWS[1] - BULK_ROWS[0] + 1)
                 if bulk else 20 + rng.below(181))
            with self.lock:
                if len(self.docs) + n > self.docs_cap:
                    return self.build("delete_docs", rng)
                first = self.next_id
                self.next_id += n

            def apply():
                with self.lock:
                    for i in range(first, first + n):
                        self.docs[i] = 0
                    self.summary["ingested"] += n
            # A bulk load arrives as several files rather than one huge
            # statement, which is both how a real one lands and what gives the
            # reindex something to diff.
            step = BULK_CHUNK if bulk else n
            return WorkloadOp(kind, [self.ingest_sql(first + off, min(step, n - off))
                                     for off in range(0, n, step)], apply)
        if kind == "delete_docs":
            ids = self._sample_alive(rng, 1 + rng.below(5))
            if not ids:
                return self.build("count_filtered", rng)

            def apply():
                with self.lock:
                    for i in ids:
                        self.docs.pop(i, None)
                    self.summary["deleted"] += len(ids)
            return WorkloadOp(kind, [f"DELETE FROM {self.table} WHERE id IN "
                                     f"({', '.join(map(str, ids))})"], apply)
        if kind == "update_docs":
            ids = self._sample_alive(rng, 1 + rng.below(3))
            if not ids:
                return self.build("count_filtered", rng)

            def apply():
                with self.lock:
                    for i in ids:
                        if i in self.docs:
                            self.docs[i] += 1
                    self.summary["updated"] += len(ids)
            return WorkloadOp(kind, [
                f"UPDATE {self.table} SET body = {body_sql('id', 'ver + 1')}, "
                f"emb = {emb_sql('id', 'ver + 1', self.dim)}, ver = ver + 1 "
                f"WHERE id IN ({', '.join(map(str, ids))})"], apply)
        word = rng.choice(VOCAB)
        k, ver = self._one(rng)
        if kind == "search_bm25":
            sql = (f"SELECT id FROM {self.index} WHERE body @@ ts_phrase({lit(word)}) "
                   f"ORDER BY bm25(tableoid) DESC, id LIMIT 10")
        elif kind == "search_ann":
            sql = (f"SELECT id FROM {self.index} ORDER BY emb <-> "
                   f"{vec_sql(k, ver, self.dim)} LIMIT 10")
        elif kind == "search_hybrid":
            sql = (f"SELECT id FROM {self.index} WHERE body @@ ts_phrase({lit(word)}) "
                   f"ORDER BY emb <-> {vec_sql(k, ver, self.dim)} LIMIT 10")
        elif kind == "lookup":
            sql = f"SELECT body FROM {self.index} WHERE id = {k}"
        elif kind == "facet":
            sql = f"SELECT category, count(*) FROM {self.index} GROUP BY category"
        elif kind == "count_filtered":
            sql = f"SELECT count(*) FROM {self.index} WHERE body @@ ts_phrase({lit(word)})"
        elif kind == "metrics_probe":
            sql = (f"SELECT metric, value FROM sdb_metrics WHERE relation_id = "
                   f"(SELECT oid FROM pg_class WHERE relname = {lit(self.index)})")
        else:
            raise ValueError(kind)
        return WorkloadOp(kind, [sql])

    def check(self, label):
        findings = []

        def bad(kind, detail):
            findings.append({"kind": kind, "key": None,
                             "detail": f"{label}: {detail}"[:400],
                             "candidates": None, "observed": None})

        def one(cur, sql):
            cur.execute(sql)
            return cur.fetchone()[0]

        def column(cur, sql):
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]

        def converge(cur):
            action = None
            for _ in range(60):
                try:
                    action = one(cur, f"SELECT action FROM serenedb_reindex({lit(self.index)})")
                except psycopg.errors.ObjectInUse:
                    time.sleep(0.5)
                    continue
                if action == "up_to_date":
                    return
                time.sleep(0.5)
            bad("reindex_never_converged", f"last action {action}")

        def compare(cur, stage):
            c_idx = one(cur, f"SELECT count(*) FROM {self.index}")
            c_proj = one(cur, f"SELECT count(*) FROM (SELECT id FROM {self.index})")
            c_view = one(cur, f"SELECT count(*) FROM {self.view}")
            if c_idx != c_proj:
                bad("hollow_index", f"{stage}: count(*) is {c_idx} but the projection "
                                    f"returns {c_proj} rows")
            if c_idx != c_view:
                bad("index_view_count_mismatch", f"{stage}: index {c_idx} vs view {c_view}")
            cols = "id, category, body, ver"
            diff = one(cur, f"SELECT count(*) FROM ((SELECT {cols} FROM {self.index} EXCEPT "
                            f"SELECT {cols} FROM {self.view}) UNION ALL (SELECT {cols} FROM "
                            f"{self.view} EXCEPT SELECT {cols} FROM {self.index}))")
            if diff:
                cur.execute(f"SELECT 'index_only', {cols} FROM (SELECT {cols} FROM {self.index} "
                            f"EXCEPT SELECT {cols} FROM {self.view}) UNION ALL "
                            f"SELECT 'view_only', {cols} FROM (SELECT {cols} FROM {self.view} "
                            f"EXCEPT SELECT {cols} FROM {self.index}) LIMIT 10")
                rows = cur.fetchall()
                ids = sorted({r[1] for r in rows})
                both = []
                for k in ids:
                    cur.execute(f"SELECT 'index', id, ver FROM {self.index} WHERE id = {k} "
                                f"UNION ALL SELECT 'view', id, ver FROM {self.view} WHERE id = {k}")
                    both.extend(cur.fetchall())
                cur.execute(f"VACUUM (REFRESH_INDEX) {self.index}")
                after = []
                for k in ids:
                    cur.execute(f"SELECT 'index', id, ver FROM {self.index} WHERE id = {k}")
                    after.extend(cur.fetchall())
                bad("index_view_rows_mismatch",
                    f"{stage}: {diff} differing rows: {rows}; per id: {both}; "
                    f"index after VACUUM (REFRESH_INDEX): {after}")
            return c_view

        try:
            with self.connect() as conn, conn.cursor() as cur:
                converge(cur)
                c_view = compare(cur, "after converge")
                cur.execute(f"SELECT id, ver FROM {self.view}")
                truth = dict(cur.fetchall())
                with self.lock:
                    self.docs = dict(truth)
                    self.next_id = max(self.next_id, (max(truth) + 1) if truth else 1)
                sampler = random.Random(self.checks)
                for word in sampler.sample(VOCAB, 6):
                    expected = sum(1 for i, v in truth.items() if word in words_for(i, v))
                    got = one(cur, f"SELECT count(*) FROM {self.index} WHERE body @@ "
                                   f"ts_phrase({lit(word)})")
                    if got != expected:
                        bad("fts_count_mismatch", f"'{word}': index {got}, table {expected}")
                alive = sorted(truth)
                for k in sampler.sample(alive, min(5, len(alive))):
                    top = column(cur, f"SELECT id FROM {self.index} ORDER BY emb <-> "
                                      f"{vec_sql(k, truth[k], self.dim)} LIMIT 5")
                    if k not in top:
                        bad("ann_exact_vector_miss", f"exact vector of {k} not in top-5 {top}")
                for k in sampler.sample(alive, min(3, len(alive))):
                    vec = vec_sql(k, truth[k], self.dim)
                    got = set(column(cur, f"SELECT id FROM {self.index} ORDER BY emb <-> {vec} LIMIT 10"))
                    want = set(column(cur, f"SELECT id FROM {self.view} ORDER BY emb <-> {vec} LIMIT 10"))
                    recall = len(got & want) / max(len(want), 1)
                    if recall < 0.9:
                        bad("ann_recall_below", f"query {k}: recall@10 {recall:.2f}")
                for k in sampler.sample(alive, min(20, len(alive))):
                    rows = column(cur, f"SELECT body FROM {self.index} WHERE id = {k}")
                    if rows != [body_for(k, truth[k])]:
                        bad("lookup_mismatch", f"id {k}: {rows!r} vs {body_for(k, truth[k])!r}")
                dead = [i for i in sampler.sample(range(1, self.next_id), min(50, self.next_id - 1))
                        if i not in truth][:5]
                for k in dead:
                    n = one(cur, f"SELECT count(*) FROM {self.index} WHERE id = {k}")
                    if n:
                        bad("deleted_row_visible", f"id {k} still returns {n} row(s)")
                cur.execute(f"SELECT metric, value FROM sdb_metrics WHERE relation_id = "
                            f"(SELECT oid FROM pg_class WHERE relname = {lit(self.index)})")
                metrics = {m: v for m, v in cur.fetchall()}
                if metrics.get("num_failed_commits", 0):
                    bad("metrics_failed_commits", f"{metrics['num_failed_commits']} failed commits")
                if "num_live_docs" in metrics and int(metrics["num_live_docs"]) != c_view:
                    bad("metrics_live_docs_mismatch",
                        f"num_live_docs {metrics['num_live_docs']} vs {c_view} rows")
                self.checks += 1
                self.summary["checks"] = self.checks
                if self.checks % 3 == 0:
                    cur.execute(f"DROP INDEX {self.index}")
                    cur.execute(self.create_index_sql())
                    self.summary["rebuilds"] += 1
                    compare(cur, "after drop/create index")
        except Exception as exc:
            bad("workload_oracle_error", f"{type(exc).__name__}: {exc}")
        return findings
