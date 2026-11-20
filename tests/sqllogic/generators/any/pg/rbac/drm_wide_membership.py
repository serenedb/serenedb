from sqllogic_gen import Test

GROUPS = 100
STRANGERS = 50

t = Test("A role that belongs to many roles holds exactly their privileges: "
         "the one that grants is found, the others are not.")

t.statement("DROP TABLE IF EXISTS rwm_t")
t.statement("DROP TABLE IF EXISTS rwm_u")
t.statement("DROP ROLE IF EXISTS rwm_user")
for i in range(1, GROUPS + 1):
    t.statement(f"DROP ROLE IF EXISTS rwm_g{i}")
for i in range(1, STRANGERS + 1):
    t.statement(f"DROP ROLE IF EXISTS rwm_s{i}")

t.statement("CREATE ROLE rwm_user")
for i in range(1, GROUPS + 1):
    t.statement(f"CREATE ROLE rwm_g{i}")
    t.statement(f"GRANT rwm_g{i} TO rwm_user")
for i in range(1, STRANGERS + 1):
    t.statement(f"CREATE ROLE rwm_s{i}")
t.statement("CREATE TABLE rwm_t (a INTEGER)")
t.statement("INSERT INTO rwm_t VALUES (1)")
t.statement(f"GRANT SELECT ON rwm_t TO rwm_g{GROUPS}")
t.statement("CREATE TABLE rwm_u (a INTEGER)")
t.statement(f"GRANT SELECT ON rwm_u TO rwm_s{STRANGERS}")

t.query("SELECT count(*) AS n FROM pg_roles "
        "WHERE rolname LIKE 'rwm\\_g%' AND pg_has_role('rwm_user', oid, 'USAGE')",
        ["n"], [[GROUPS]])
t.query("SELECT count(*) AS n FROM pg_roles "
        "WHERE rolname LIKE 'rwm\\_s%' AND pg_has_role('rwm_user', oid, 'USAGE')",
        ["n"], [[0]])
t.query("SELECT has_table_privilege('rwm_user', 'rwm_t', 'SELECT') AS t, "
        "has_table_privilege('rwm_user', 'rwm_u', 'SELECT') AS u",
        ["t", "u"], [["t", "f"]])

t.statement("SET ROLE rwm_user")
t.query("SELECT count(*) AS n FROM rwm_t", ["n"], [[1]])
t.error("SELECT count(*) FROM rwm_u", "permission denied for table rwm_u")
t.statement("RESET ROLE")

t.statement(f"REVOKE rwm_g{GROUPS} FROM rwm_user")
t.statement("SET ROLE rwm_user")
t.error("SELECT count(*) FROM rwm_t", "permission denied for table rwm_t")
t.statement("RESET ROLE")

t.statement("DROP TABLE rwm_t")
t.statement("DROP TABLE rwm_u")
t.statement("DROP ROLE rwm_user")
for i in range(1, GROUPS + 1):
    t.statement(f"DROP ROLE rwm_g{i}")
for i in range(1, STRANGERS + 1):
    t.statement(f"DROP ROLE rwm_s{i}")
t.write()
