# fmt: off

import pytest

from shell_harness import ShellTest, requires_serenedb, shell  # noqa: F401

pytestmark = requires_serenedb

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


def test_aligned_control_characters(shell):
    result = run_script(shell, ".mode psql", "SELECT 'a' || chr(9) || 'b' AS t, 'x' || chr(1) || chr(127) AS c;")
    result.check_stdout("\n".join([
        "t     |     c     ",
        "-----------+-----------",
        " a       b | x\\x01\\x7F",
        "(1 row)",
    ]))


@pytest.mark.parametrize("border,expected", [
    (0, "a+c \nb   \n- -\n1 2\n(1 row)"),
    (1, "\n a+| c \n b |   \n---+---\n 1 | 2\n(1 row)"),
])
def test_aligned_multi_line_header(shell, border, expected):
    result = run_script(shell, ".mode psql", f"\\pset border {border}", 'SELECT 1 AS "a', 'b", 2 AS c;')
    result.check_stdout(expected)


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


def test_expanded_no_rows(shell):
    result = run_script(shell, ".mode psql", "\\x", "SELECT 1 AS a WHERE false;")
    result.check_stdout("Expanded display is on.\n(0 rows)")
    result.check_not_exist("RECORD")


def test_expanded_border_zero_labels_records(shell):
    result = run_script(shell, ".mode psql", "\\pset border 0", "\\x", "SELECT 'value' AS long_column_name;")
    result.check_stdout("* Record 1           \nlong_column_name value")


def test_expanded_border_two(shell):
    result = run_script(
        shell,
        ".mode psql",
        "\\pset border 2",
        "\\x",
        "SELECT 'one' || chr(10) || 'two' AS long_name, 'alpha' AS c;",
    )
    result.check_stdout("\n".join([
        "+-[ RECORD 1 ]------+",
        "| long_name | one  +|",
        "|           | two   |",
        "| c         | alpha |",
        "+-----------+-------+",
    ]))


@pytest.mark.parametrize("border,expected", [
    (0, ["* Record 1", "a+ x", "b ", "c  yyyyyyyy+", "   z"]),
    (1, ["-[ RECORD 1 ]", "a+| x", "b |", "c | yyyyyyyy+", "  | z"]),
    (2, [
        "+-[ RECORD 1 ]-+",
        "| a+| x        |",
        "| b |          |",
        "| c | yyyyyyyy+|",
        "|   | z        |",
        "+---+----------+",
    ]),
])
def test_expanded_multi_line_header(shell, border, expected):
    result = run_script(
        shell,
        ".mode psql",
        f"\\pset border {border}",
        "\\x",
        "SELECT 'x' AS \"a",
        "b\", 'yyyyyyyy' || chr(10) || 'z' AS c;",
    )
    result.check_stdout(expected)


def test_expanded_border_two_widens_the_data_to_the_record_header(shell):
    result = run_script(shell, ".mode psql", "\\pset border 2", "SELECT 1 AS n", "\\gx")
    result.check_stdout("+-[ RECORD 1 ]-+\n| n | 1        |\n+---+----------+")


@pytest.mark.parametrize("border,expected", [
    (0, ["a 1", "b x", "  ", "a 2", "b y"]),
    (1, ["a | 1", "b | x", "--+--", "a | 2", "b | y"]),
    (2, ["+---+---+", "| a | 1 |", "| b | x |", "+---+---+", "| a | 2 |", "| b | y |", "+---+---+"]),
])
def test_expanded_tuples_only(shell, border, expected):
    result = run_script(
        shell,
        ".mode psql",
        f"\\pset border {border}",
        "\\t",
        "\\x",
        "SELECT * FROM (VALUES (1, 'x'), (2, 'y')) t(a, b);",
    )
    result.check_stdout(expected)
    result.check_not_exist("RECORD")


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


def test_unaligned_record_separator(shell):
    result = run_script(shell, "\\pset format unaligned", "\\pset recordsep ';'", "SELECT * FROM (VALUES (1), (2)) t(a);")
    result.check_stdout('Record separator is ";".')
    result.check_stdout("a;1;2;(2 rows)")


def test_unaligned_zero_byte_separators(shell):
    result = run_script(
        shell,
        "\\pset format unaligned",
        "\\pset fieldsep_zero",
        "\\pset recordsep_zero",
        "SELECT 1 AS a, 2 AS b;",
        "\\pset",
    )
    result.check_stdout("Field separator is zero byte.\nRecord separator is zero byte.\na\x00b\x001\x002\x00(1 row)\x00")
    result.check_stdout("fieldsep_zero".ljust(24) + " on")
    result.check_stdout("recordsep_zero".ljust(24) + " on")


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


def test_csv_field_separator(shell):
    result = run_script(
        shell,
        ".mode psql",
        "\\pset format csv",
        "\\pset csv_fieldsep ';'",
        "\\pset csv_fieldsep ab",
        "\\pset csv_fieldsep '\"'",
        "\\pset csv_fieldsep",
        "SELECT 'a;b' AS x, 'c,d' AS y;",
    )
    assert result.stdout.count('Field separator for CSV is ";".') == 2, result.stdout
    assert 'x;y\n"a;b";c,d' in result.stdout, result.stdout
    result.check_stderr("\\pset: csv_fieldsep must be a single one-byte character")
    result.check_stderr("\\pset: csv_fieldsep cannot be a double quote, a newline, or a carriage return")


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


def test_unknown_mode_lists_psql(shell):
    result = run_script(shell, ".mode nosuch", ".mode pz")
    message = ("Error: mode should be one of: ascii box column csv duckbox html insert json jsonlines latex line "
               "list markdown psql quote table tabs tcl trash")
    assert result.stderr.count(message) == 2, result.stderr


def test_list_databases_flag(shell):
    result = ShellTest(shell).add_argument("-l").run()
    result.check_stdout("database_name")
