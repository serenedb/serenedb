import random
import sys
import threading
import time

import psycopg

CHARGE_FAULT = "irs::ReserveMemory"
OUT_OF_MEMORY = "53200"
OUT_OF_MEMORY_CAUSES = ("failed to reserve memory", "failed to allocate data",
                        "failed to pin block", "could not allocate block")
DIM = 8
MAX_HITS = 4000
LIMIT_STEPS_MB = (1, 2, 4, 8, 16, 32, 64, 128)
WORDS = "['alpha', 'bravo', 'charlie', 'delta']"
SEARCH = "storage = 'search', refresh_interval = 0, compaction_interval = 0"


def connect(port, db):
    return psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                           password="postgres", dbname=db, autocommit=True)


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def doc_rows(first, count, tag):
    return (f"SELECT i, 'doc ' || i || ' {tag} ' || {WORDS}[i % 4 + 1], 0, "
            f"[sin(i + j)::FLOAT FOR j IN range({DIM})]::FLOAT[{DIM}] "
            f"FROM range({first}, {first + count}) t(i)")


def note_rows(first, count):
    return f"SELECT i, 'note ' || i FROM range({first}, {first + count}) t(i)"


def out_of_memory(error):
    return error.sqlstate == OUT_OF_MEMORY or any(
        cause in str(error) for cause in OUT_OF_MEMORY_CAUSES)


class Model:
    def __init__(self):
        self.lock = threading.Lock()
        self.docs = 0
        self.notes = 0
        self.next_id = 0

    def ids(self, count):
        with self.lock:
            first = self.next_id
            self.next_id += count
            return first

    def add(self, docs=0, notes=0):
        with self.lock:
            self.docs += docs
            self.notes += notes


class Step:
    def __init__(self, name, build, prepare=None, verify=None):
        self.name = name
        self.build = build
        self.prepare = prepare
        self.verify = verify


def ensure_docs(cur, model, rows):
    if scalar(cur, "SELECT count(*) FROM docs") < rows:
        cur.execute(f"INSERT INTO docs {doc_rows(model.ids(rows), rows, 'fill')}")
        model.add(docs=rows)


def ensure_notes(cur, model, rows):
    if scalar(cur, "SELECT count(*) FROM notes") < rows:
        cur.execute(f"INSERT INTO notes {note_rows(model.ids(rows), rows)}")
        model.add(notes=rows)


def indexed_steps(model, copy_file):
    def insert(cur):
        first = model.ids(5000)
        return [f"INSERT INTO docs {doc_rows(first, 5000, 'ins')}"], lambda: model.add(docs=5000)

    def update(cur):
        k = random.randrange(7)
        return [f"UPDATE docs SET body = body || ' upd', ver = ver + 1 WHERE id % 7 = {k}"], None

    def delete(cur):
        return ["DELETE FROM docs WHERE id IN (SELECT id FROM docs ORDER BY id LIMIT 100)"], \
            lambda: model.add(docs=-100)

    def copy(cur):
        return [f"COPY docs FROM '{copy_file}' (FORMAT PARQUET)"], lambda: model.add(docs=300)

    def committed(cur):
        cur.execute(f"INSERT INTO docs {doc_rows(model.ids(50), 50, 'held')}")
        model.add(docs=50)

    return [Step("insert", insert, prepare=committed), Step("update", update, prepare=committed),
            Step("delete", delete, prepare=lambda cur: ensure_docs(cur, model, 300)),
            Step("copy", copy, prepare=committed)]


def search_steps(model):
    def insert(cur):
        first = model.ids(5000)
        return [f"INSERT INTO notes {note_rows(first, 5000)}"], lambda: model.add(notes=5000)

    def committed(cur):
        cur.execute(f"INSERT INTO notes {note_rows(model.ids(50), 50)}")
        model.add(notes=50)

    def update(cur):
        k = random.randrange(5)
        return [f"UPDATE notes SET body = body || ' upd' WHERE id % 5 = {k}"], None

    def delete(cur):
        return ["DELETE FROM notes WHERE id IN (SELECT id FROM notes ORDER BY id LIMIT 100)"], \
            lambda: model.add(notes=-100)

    def truncate(cur):
        def emptied():
            with model.lock:
                model.notes = 0
        return ["TRUNCATE notes"], emptied

    return [Step("search insert", insert, prepare=committed),
            Step("search update", update, prepare=lambda cur: ensure_notes(cur, model, 100)),
            Step("search delete", delete, prepare=lambda cur: ensure_notes(cur, model, 300)),
            Step("search truncate", truncate, prepare=lambda cur: ensure_notes(cur, model, 100))]


def maintenance_steps(model):
    def drop_raw_index(cur):
        cur.execute("DROP INDEX IF EXISTS raw_idx")

    def create_index(cur):
        return ["CREATE INDEX raw_idx ON raw USING inverted (id, body mp_words)"], None

    def raw_index(cur, failed):
        exists = scalar(cur, "SELECT count(*) FROM pg_indexes WHERE indexname = 'raw_idx'")
        if failed:
            return [f"a failed CREATE INDEX left {exists} index behind"] if exists else []
        indexed = scalar(cur, "SELECT count(*) FROM raw_idx")
        rows = scalar(cur, "SELECT count(*) FROM raw")
        return [] if indexed == rows else [f"raw_idx holds {indexed} of {rows} rows"]

    def segments(cur):
        for _ in range(3):
            cur.execute(f"INSERT INTO docs {doc_rows(model.ids(20), 20, 'seg')}")
            model.add(docs=20)
            cur.execute("VACUUM (REFRESH_TABLE) docs")

    def pending(cur):
        cur.execute(f"INSERT INTO docs {doc_rows(model.ids(50), 50, 'pend')}")
        model.add(docs=50)

    return [Step("create index", create_index, prepare=drop_raw_index, verify=raw_index),
            Step("compact", lambda cur: (["VACUUM (COMPACT_TABLE) docs"], None), prepare=segments),
            Step("refresh", lambda cur: (["VACUUM (REFRESH_TABLE) docs"], None), prepare=pending)]


def setup(cur, model, copy_file):
    cur.execute("CREATE TEXT SEARCH DICTIONARY mp_words AS split_text_csv(' ') "
                "WITH (frequency, position)")
    cur.execute(f"CREATE TABLE docs (id BIGINT, body TEXT, ver INT, emb FLOAT[{DIM}]) "
                f"WITH ({SEARCH})")
    cur.execute("CREATE INDEX docs_idx ON docs USING inverted "
                "(id, body mp_words, emb ivf (metric = 'l2'))")
    cur.execute(f"CREATE TABLE notes (id BIGINT, body TEXT) WITH ({SEARCH})")
    cur.execute(f"CREATE TABLE raw (id BIGINT, body TEXT) WITH ({SEARCH})")
    cur.execute("INSERT INTO raw SELECT i, 'raw ' || i || ' ' || "
                f"{WORDS}[i % 4 + 1] FROM range(2000) t(i)")
    cur.execute(f"COPY ({doc_rows(10 ** 9, 300, 'copy')}) TO '{copy_file}' (FORMAT PARQUET)")
    ensure_docs(cur, model, 500)
    ensure_notes(cur, model, 500)
    cur.execute("VACUUM (REFRESH_TABLE) docs")
    cur.execute("VACUUM (REFRESH_TABLE) notes")
    cur.execute("CHECKPOINT")


def check(cur, model):
    problems = []
    for sql in ("VACUUM (REFRESH_TABLE) docs", "VACUUM (REFRESH_TABLE) notes"):
        try:
            cur.execute(sql)
        except psycopg.Error as e:
            problems.append(f"{sql} failed: {e.sqlstate} {e}")
    docs = scalar(cur, "SELECT count(*) FROM docs")
    if docs != model.docs:
        problems.append(f"docs has {docs} rows, {model.docs} expected")
    diff = scalar(cur, "SELECT count(*) FROM ((SELECT id, body FROM docs_idx EXCEPT ALL "
                       "SELECT id, body FROM docs) UNION ALL (SELECT id, body FROM docs "
                       "EXCEPT ALL SELECT id, body FROM docs_idx))")
    if diff:
        problems.append(f"docs_idx differs from docs in {diff} rows")
    notes = scalar(cur, "SELECT count(*) FROM notes")
    if notes != model.notes:
        problems.append(f"notes has {notes} rows, {model.notes} expected")
    try:
        cur.execute("CHECKPOINT")
    except psycopg.Error as e:
        problems.append(f"CHECKPOINT failed: {e.sqlstate} {e}")
    return problems


def run(cur, statements):
    try:
        for sql in statements:
            cur.execute(sql)
    except psycopg.Error as e:
        return e
    return None


def fired(cur, hits):
    try:
        cur.execute(f"SET sdb_faults = '-{CHARGE_FAULT}@{hits}'")
    except psycopg.errors.InvalidParameterValue:
        return True
    except psycopg.Error:
        pass
    return False


def outcome(cur, model, step, error, on_success):
    problems = []
    if error is None:
        if on_success:
            on_success()
    elif not out_of_memory(error):
        problems.append(f"failed with {error.sqlstate} {error}")
    if step.verify:
        problems += step.verify(cur, error is not None)
    return problems + check(cur, model)


def sweep_charges(cur, model, step):
    for hits in range(1, MAX_HITS + 1):
        if step.prepare:
            step.prepare(cur)
        statements, on_success = step.build(cur)
        cur.execute(f"SET sdb_faults = '{CHARGE_FAULT}@{hits}'")
        error = run(cur, statements)
        hit = fired(cur, hits)
        cur.execute("RESET sdb_faults")
        problems = outcome(cur, model, step, error, on_success)
        if problems:
            return [f"{step.name}, charge {hits} failing: {p}" for p in problems], hits - 1
        print(f"{step.name}, charge {hits} failing: {error}", file=sys.stderr)
        if not hit:
            return [], hits - 1
    return [f"{step.name}: still charging after {MAX_HITS} charges"], MAX_HITS


def used_memory(cur):
    return scalar(cur, "SELECT sum(memory_usage_bytes) FROM sdb_memory()")


def sweep_limits(cur, model, steps):
    failed = 0
    for mb in LIMIT_STEPS_MB:
        for step in steps:
            if step.prepare:
                step.prepare(cur)
            statements, on_success = step.build(cur)
            limit = used_memory(cur) + mb * 2 ** 20
            cur.execute(f"SET GLOBAL memory_limit = '{limit // 1024}KB'")
            error = run(cur, statements)
            cur.execute("RESET GLOBAL memory_limit")
            print(f"{step.name}, {mb}MB of headroom: {error}", file=sys.stderr)
            failed += error is not None
            problems = outcome(cur, model, step, error, on_success)
            if problems:
                return [f"{step.name}, {mb}MB of headroom: {p}" for p in problems]
    return [] if failed else ["no statement ran out of memory: the limits are too loose"]


def writer(port, db, model, seed, rounds, failures, problems):
    rng = random.Random(seed)
    with connect(port, db) as conn, conn.cursor() as cur:
        mine = []
        for _ in range(rounds):
            kind = rng.choice(("insert", "insert", "update", "delete", "note", "unnote"))
            if kind == "insert":
                first = model.ids(500)
                sql, done = (f"INSERT INTO docs {doc_rows(first, 500, 'conc')}",
                             lambda first=first: (mine.append(first), model.add(docs=500)))
            elif kind == "update" and mine:
                first = rng.choice(mine)
                sql, done = (f"UPDATE docs SET body = body || ' upd' WHERE id >= {first} "
                             f"AND id < {first + 500}", lambda: None)
            elif kind == "delete" and mine:
                first = rng.choice(mine)
                sql, done = (f"DELETE FROM docs WHERE id >= {first} AND id < {first + 500}",
                             lambda first=first: (mine.remove(first), model.add(docs=-500)))
            elif kind == "note":
                first = model.ids(500)
                sql, done = (f"INSERT INTO notes {note_rows(first, 500)}",
                             lambda: model.add(notes=500))
            else:
                continue
            error = run(cur, [sql])
            if error is None:
                done()
            elif out_of_memory(error):
                failures.append(kind)
            else:
                problems.append(f"{kind} failed with {error.sqlstate} {error}")


def concurrent(port, db, cur, model, headroom_mb):
    limit = used_memory(cur) + headroom_mb * 2 ** 20
    cur.execute(f"SET GLOBAL memory_limit = '{limit // 1024}KB'")
    failures, problems = [], []
    threads = [threading.Thread(target=writer, args=(port, db, model, seed, 40, failures,
                                                     problems)) for seed in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    cur.execute("RESET GLOBAL memory_limit")
    print(f"concurrent: {len(failures)} statements ran out of memory", file=sys.stderr)
    problems += check(cur, model)
    if not failures:
        problems.append("no statement ran out of memory: the limit is too loose")
    if len(failures) == len(threads) * 40:
        problems.append("every statement ran out of memory: the limit is too tight")
    return problems


def allocator_memory(cur):
    return scalar(cur, "SELECT memory_usage_bytes FROM sdb_memory() WHERE tag = 'ALLOCATOR'")


def released(cur, model):
    rows = 200000
    cur.execute(f"INSERT INTO docs {doc_rows(model.ids(rows), rows, 'big')}")
    model.add(docs=rows)
    cur.execute("VACUUM (REFRESH_TABLE) docs")
    inserted = allocator_memory(cur)
    cur.execute(f"DELETE FROM docs WHERE id IN (SELECT id FROM docs ORDER BY id DESC LIMIT {rows})")
    model.add(docs=-rows)
    cur.execute("VACUUM (REFRESH_TABLE) docs")
    grown = allocator_memory(cur) - inserted
    print(f"released: a DELETE of {rows} rows grew memory by {grown} bytes", file=sys.stderr)
    if grown >= 2 ** 20:
        return [f"a DELETE of {rows} rows still holds {grown} bytes after its refresh"]
    return check(cur, model)


def restart(port, db, model):
    with connect(port, db) as conn, conn.cursor() as cur:
        cur.execute("SET sdb_faults = 'crash_on_packet'")
        try:
            cur.execute("SELECT 1")
        except psycopg.OperationalError:
            pass
    for _ in range(120):
        try:
            conn = connect(port, db)
            break
        except psycopg.OperationalError:
            time.sleep(0.5)
    else:
        return ["serened did not come back after the restart"]
    with conn, conn.cursor() as cur:
        return check(cur, model)


def main():
    port, db, test_dir, mode = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
    random.seed(1)
    model = Model()
    copy_file = f"{test_dir}/memory_pressure_copy.parquet"
    with connect(port, db) as conn, conn.cursor() as cur:
        setup(cur, model, copy_file)
        groups = {"indexed": indexed_steps(model, copy_file), "search": search_steps(model),
                  "maintenance": maintenance_steps(model)}
        if mode == "charges":
            only = sys.argv[6:]
            failed = 0
            for step in groups[sys.argv[5]]:
                if only and step.name not in only:
                    continue
                problems, charges = sweep_charges(cur, model, step)
                failed += charges
                for problem in problems or [f"{step.name}: ok"]:
                    print(problem)
            if not failed:
                print("no memory charge failed: the fault point never fired")
        elif mode == "limits":
            steps = [s for group in groups.values() for s in group]
            for problem in sweep_limits(cur, model, steps) or ["limits: ok"]:
                print(problem)
        elif mode == "concurrent":
            for problem in concurrent(port, db, cur, model, int(sys.argv[5])) or ["concurrent: ok"]:
                print(problem)
            for problem in released(cur, model) or ["released: ok"]:
                print(problem)
    for problem in restart(port, db, model) or ["restart: ok"]:
        print(problem)


if __name__ == "__main__":
    main()
