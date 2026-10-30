from sqllogic_gen import Test


def chain(terms):
    return "SELECT " + "+".join(["67"] * terms) + " AS mrbeast"


def subqueries(depth):
    return "SELECT " + "(SELECT " * depth + "67" + ")" * depth + " AS mrbeast"


def nested_chains(calls, terms):
    return "SELECT " + "abs(" * calls + "67" + ("+67" * terms + ")") * calls + " AS mrbeast"


t = Test("Nesting deeper than the stack holds fails with stack depth limit "
         "exceeded instead of crashing the server; moderate nesting works.")
t.section("A long chain of additions.")
t.expect(chain(3000), ["mrbeast"], [[201000]])
t.error(chain(100000), "(54001)")
t.section("Chains of additions nested in calls, each chain short enough on its own.")
t.expect(nested_chains(3, 1000), ["mrbeast"], [[201067]])
t.error(nested_chains(60, 5000), "(54001)")
t.section("Nested scalar subqueries.")
t.expect(subqueries(1000), ["mrbeast"], [[67]])
t.error(subqueries(3000), "(54001)", skipif="serenedb")
t.error(subqueries(30000), "(54001)", onlyif="serenedb")
t.section("The session still answers.")
t.expect("SELECT 67 AS six_seven", ["six_seven"], [[67]])
t.write()
