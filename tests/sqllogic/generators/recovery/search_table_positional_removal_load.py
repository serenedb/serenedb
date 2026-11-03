from sqllogic_gen import Test

t = Test("Random inserts, deletes and updates on a search table that refreshes "
         "and compacts in the background, with crashes right after a DELETE "
         "commits: once the log is replayed, the rows left match a model.",
         seed=5)
COUNT = "SELECT count(*) AS n, coalesce(sum(id), 0) AS s FROM rl;"
RETRY = "retry $RETRY_ATTEMPTS backoff $BACKOFF_DURATION"

live = set()
conn = "start"
next_id = 0
crashes = 0


def statement(sql):
    t.connection(conn)
    t.statement(sql)
    if not sql.startswith("VACUUM"):
        t.connection(conn)
        t.statement("VACUUM (REFRESH_TABLE) rl;")


def check():
    t.connection(conn)
    t.statement("VACUUM (REFRESH_TABLE) rl;")
    t.connection(conn)
    t.expect(COUNT, ["n", "s"], [[len(live), sum(live)]])


t.control("substitution on")
t.statement("RESET sdb_faults;")
t.statement("CREATE TABLE rl (id BIGINT, grp INT) WITH (storage = 'search', "
            "refresh_interval = 30, compaction_interval = 10, "
            "compaction_max_segments = 3);", kind=f"statement ok {RETRY}")
for _ in range(120):
    x = t.rng.random()
    if x < 0.35 or len(live) < 1000:
        n = t.rng.choice([500, 2048, 3000, 9000])
        statement(f"INSERT INTO rl SELECT i, i % 17 FROM "
                  f"generate_series({next_id}, {next_id + n - 1}) g(i);")
        live |= set(range(next_id, next_id + n))
        next_id += n
    elif x < 0.7:
        m, r = t.rng.choice([(7, 3), (13, 1), (101, 50), (2, 1), (1000, 7)])
        statement(f"DELETE FROM rl WHERE id % {m} = {r};")
        live = {i for i in live if i % m != r}
    elif x < 0.85:
        lo = t.rng.randrange(0, max(next_id, 1))
        hi = lo + t.rng.randrange(1, 5000)
        statement(f"UPDATE rl SET grp = grp + 1 WHERE id BETWEEN {lo} AND {hi};")
        statement("VACUUM (REFRESH_TABLE) rl;")
    elif x < 0.93:
        check()
    else:
        crashes += 1
        t.connection(conn)
        t.statement("SET sdb_faults = 'crash_after_commit';")
        m, r = t.rng.choice([(11, 4), (17, 9), (3, 2)])
        t.connection(conn)
        t.error(f"DELETE FROM rl WHERE id % {m} = {r};", "connection closed")
        live = {i for i in live if i % m != r}
        conn = f"after_crash_{crashes}"
        t.connection(conn)
        t.expect(COUNT, ["n", "s"], [[len(live), sum(live)]],
                 kind=f"query ok {RETRY}")
check()
t.connection(conn)
t.statement("DROP TABLE rl;")
t.write()
