from sqllogic_gen import Test


def chain(terms):
    return "SELECT " + "+".join(["67"] * terms) + " AS mrbeast"


def subqueries(depth):
    return "SELECT " + "(SELECT " * depth + "67" + ")" * depth + " AS mrbeast"


def nested_chains(calls, terms):
    return "SELECT " + "abs(" * calls + "67" + ("+67" * terms + ")") * calls + " AS mrbeast"


t = Test("Nesting too deep for the server fails with an error instead of crashing it; "
         "moderate nesting works.")
t.section("A long chain of additions.")
t.expect(chain(1000), ["mrbeast"], [[67000]])
t.error(chain(100000), "(54001)", skipif="serenedb")
t.error(chain(100000), "(42601)", onlyif="serenedb")
t.section("Chains of additions nested in calls.")
t.expect(nested_chains(3, 300), ["mrbeast"], [[60367]])
t.error(nested_chains(60, 5000), "(54001)", skipif="serenedb")
t.error(nested_chains(60, 5000), "(42601)", onlyif="serenedb")
t.section("Nested scalar subqueries.")
t.expect(subqueries(500), ["mrbeast"], [[67]])
t.error(subqueries(3000), "(54001)", skipif="serenedb")
t.error(subqueries(3000), "(42601)", onlyif="serenedb")
t.section("The session still answers.")
t.expect("SELECT 67 AS six_seven", ["six_seven"], [[67]])
t.write()
