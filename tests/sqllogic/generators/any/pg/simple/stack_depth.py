from sqllogic_gen import Test


def chain(terms):
    return "SELECT " + "+".join(["1"] * terms) + " AS n"


def subqueries(depth):
    return "SELECT " + "(SELECT " * depth + "1" + ")" * depth + " AS n"


def nested_chains(calls, terms):
    return "SELECT " + "abs(" * calls + "1" + ("+1" * terms + ")") * calls + " AS n"


t = Test("Nesting deeper than the stack holds fails with stack depth limit "
         "exceeded instead of crashing the server; moderate nesting works.")
t.section("A long chain of additions.")
t.expect(chain(3000), ["n"], [[3000]])
t.error(chain(100000), "(54001)")
t.section("Chains of additions nested in calls, each chain short enough on its own.")
t.expect(nested_chains(3, 1000), ["n"], [[3001]])
t.error(nested_chains(60, 5000), "(54001)")
t.section("Nested scalar subqueries.")
t.expect(subqueries(1000), ["n"], [[1]])
t.error(subqueries(3000), "(54001)", skipif="serenedb")
t.error(subqueries(30000), "(54001)", onlyif="serenedb")
t.section("The session still answers.")
t.expect("SELECT 1 AS alive", ["alive"], [[1]])
t.write()
