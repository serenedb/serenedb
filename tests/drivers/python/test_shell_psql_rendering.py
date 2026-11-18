# fmt: off

from shell_harness import ShellTest, requires_serened, shell  # noqa: F401

pytestmark = requires_serened

QUERY = "SELECT 1 AS id, 'abc' AS name, NULL::VARCHAR AS note, 'line1' || chr(10) || 'line2' AS multi, 3.5 AS num;"


def run_script(shell, *lines, arguments=()):
    test = ShellTest(shell)
    if arguments:
        test.add_argument(*arguments)
    return test.run_raw("\n".join(lines) + "\n")


def test_aligned(shell):
    result = run_script(shell, ".mode psql", QUERY)
    result.check_stdout("\n".join([
        "id | name | note | multi | num ",
        "----+------+------+-------+-----",
        "  1 | abc  |      | line1+| 3.5",
        "    |      |      | line2 | ",
        "(1 row)",
    ]))


def test_aligned_numbers_right_and_headers_centered(shell):
    result = run_script(shell, "\\pset format aligned", "SELECT 10 AS n, 'x' AS s UNION ALL SELECT 1000, 'yyyy' ORDER BY n;")
    result.check_stdout("Output format is aligned.")
    result.check_stdout("\n".join([
        "  n   |  s   ",
        "------+------",
        "   10 | x",
        " 1000 | yyyy",
        "(2 rows)",
    ]))


def test_aligned_no_rows(shell):
    result = run_script(shell, ".mode psql", "SELECT 1 AS a WHERE false;")
    result.check_stdout("a \n---\n(0 rows)")


def test_expanded(shell):
    result = run_script(shell, ".mode psql", "\\x", QUERY)
    result.check_stdout("Expanded display is on.")
    result.check_stdout("\n".join([
        "-[ RECORD 1 ]",
        "id    | 1",
        "name  | abc",
        "note  | ",
        "multi | line1+",
        "      | line2",
        "num   | 3.5",
    ]))
    result.check_not_exist("(1 row)")


def test_expanded_records(shell):
    result = run_script(shell, ".mode psql", "\\x on", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(n, s);")
    result.check_stdout("-[ RECORD 1 ]\nn | 1\ns | a\n-[ RECORD 2 ]\nn | 2\ns | b")


def test_unaligned(shell):
    result = run_script(shell, ".mode psql", "\\a", QUERY)
    result.check_stdout("Output format is unaligned.")
    result.check_stdout("\n".join([
        "id|name|note|multi|num",
        "1|abc||line1",
        "line2|3.5",
        "(1 row)",
    ]))


def test_unaligned_field_separator(shell):
    result = run_script(shell, "\\pset format unaligned", "\\pset fieldsep ';'", "SELECT 1 AS a, 2 AS b;")
    result.check_stdout('Field separator is ";".')
    result.check_stdout("a;b\n1;2\n(1 row)")


def test_unaligned_expanded(shell):
    result = run_script(shell, "\\pset format unaligned", "\\x", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(n, s);")
    result.check_stdout("n|1\ns|a\n\nn|2\ns|b")
    result.check_not_exist("(2 rows)")


def test_tuples_only(shell):
    result = run_script(shell, ".mode psql", "\\t", "SELECT 1 AS id, 'abc' AS name;")
    result.check_stdout("Tuples only is on.")
    result.check_stdout("  1 | abc")
    result.check_not_exist("id |")
    result.check_not_exist("(1 row)")


def test_tuples_only_unaligned(shell):
    result = run_script(shell, "\\a", "\\a", "\\t on", "SELECT 1 AS a, 2 AS b;")
    result.check_stdout("Tuples only is on.\n1|2")
    result.check_not_exist("a|b")


def test_csv(shell):
    result = run_script(
        shell,
        ".mode psql",
        "\\pset format csv",
        "SELECT 'a,b' AS x, '' AS y, NULL AS z, 'say \"hi\"' AS w, 'l1' || chr(10) || 'l2' AS v, 'plain' AS p;",
    )
    result.check_stdout("Output format is csv.")
    result.check_stdout('x,y,z,w,v,p\n"a,b","",,"say ""hi""","l1\nl2",plain')


def test_csv_expanded(shell):
    result = run_script(shell, ".mode psql", "\\pset format csv", "\\x", "SELECT 1 AS a, 'b,c' AS b;")
    result.check_stdout('a,1\nb,"b,c"')


def test_null_display(shell):
    result = run_script(shell, ".mode psql", "\\pset null '(null)'", "SELECT NULL AS n;")
    result.check_stdout('Null display is "(null)".')
    result.check_stdout("n    \n--------\n (null)")


def test_null_display_shared_with_nullvalue(shell):
    result = run_script(shell, ".nullvalue NIL", "\\pset format unaligned", "SELECT NULL AS n;")
    result.check_stdout("n\nNIL\n(1 row)")


def test_border(shell):
    result = run_script(shell, ".mode psql", "\\pset border 2", "SELECT 1 AS a, 'x' AS b;")
    result.check_stdout("Border style is 2.")
    result.check_stdout("+---+---+\n| a | b |\n+---+---+\n| 1 | x |\n+---+---+\n(1 row)")


def test_border_zero(shell):
    result = run_script(shell, ".mode psql", "\\pset border 0", "SELECT 1 AS a, 'x' AS b;")
    result.check_stdout("a b \n- -\n1 x\n(1 row)")


def test_footer_off(shell):
    result = run_script(shell, ".mode psql", "\\pset footer off", "SELECT 1 AS a;")
    result.check_stdout("Default footer is off.")
    result.check_not_exist("(1 row)")


def test_expanded_auto_stays_aligned_off_terminal(shell):
    result = run_script(shell, ".mode psql", "\\x auto", "SELECT 1 AS a, 2 AS b;")
    result.check_stdout("Expanded display is used automatically.")
    result.check_stdout("a | b \n---+---\n 1 | 2")


def test_pset_flag_aligned(shell):
    result = ShellTest(shell).add_argument("-P", "format=aligned", "-c", "SELECT 1 AS x").run()
    result.check_stdout("---\n 1\n(1 row)")


def test_pset_flag_expanded(shell):
    result = ShellTest(shell).add_argument("-P", "format=aligned", "-x", "-c", "SELECT 1 AS x").run()
    result.check_stdout("-[ RECORD 1 ]\nx | 1")


def test_pset_flag_unknown_format_warns(shell):
    result = ShellTest(shell).add_argument("-P", "format=nosuch", "-c", "SELECT 1 AS x").run()
    result.check_stderr("Warning: --pset: format \"nosuch\" is not supported")


def test_duckbox_stays_default(shell):
    result = run_script(shell, "SELECT 1 AS x;")
    result.check_stdout("│")
    result.check_not_exist("(1 row)")


def test_mode_psql_is_listed(shell):
    result = run_script(shell, ".mode psql", ".mode")
    result.check_stdout("current output mode: psql")


def test_list_databases_flag(shell):
    result = ShellTest(shell).add_argument("-l").run()
    result.check_stdout("database_name")
