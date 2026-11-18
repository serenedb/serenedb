# fmt: off

from shell_harness import ShellTest, requires_serenedb, shell  # noqa: F401

pytestmark = requires_serenedb


def run_script(shell, *lines, arguments=()):
    test = ShellTest(shell)
    if arguments:
        test.add_argument(*arguments)
    return test.run_raw("\n".join(lines) + "\n")


def test_copy_from_stdin_inline_csv(shell):
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER, s VARCHAR);",
        "COPY t FROM STDIN (FORMAT csv);",
        "1,one",
        "2,two",
        "\\.",
        "SELECT string_agg(s, '+' ORDER BY i) AS joined FROM t;",
    )
    result.check_stdout("one+two")


def test_copy_from_stdin_inline_text(shell):
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER);",
        "COPY t FROM STDIN;",
        "500",
        "60",
        "\\.",
        "SELECT sum(i) AS total FROM t;",
    )
    result.check_stdout("560")


def test_copy_from_stdin_inline_in_a_file(shell, tmp_path):
    script = tmp_path / "copy.sql"
    script.write_text(
        "CREATE TABLE t(i INTEGER);\n"
        "COPY t FROM STDIN (FORMAT csv);\n"
        "3\n"
        "4\n"
        "\\.\n"
        "SELECT sum(i) AS total FROM t;\n"
    )
    result = ShellTest(shell).add_argument("-f", script.as_posix()).run()
    result.check_stdout("7")


def test_copy_from_stdin_twice(shell):
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER);",
        "COPY t FROM STDIN (FORMAT csv); COPY t FROM STDIN (FORMAT csv);",
        "1",
        "\\.",
        "10",
        "\\.",
        "SELECT sum(i) AS total FROM t;",
    )
    result.check_stdout("11")


def test_copy_from_stdin_failure_skips_its_data(shell):
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER);",
        "COPY t FROM STDIN (FORMAT csv);",
        "1",
        "SELECT 'not data';",
        "\\.",
        "COPY missing FROM STDIN (FORMAT csv);",
        "DROP TABLE t;",
        "\\.",
        "SELECT count(*) AS n FROM t;",
    )
    assert "not data" not in result.stdout
    assert "Parser Error" not in result.stderr
    assert "0" in result.stdout


def test_copy_from_stdin_with_command_reads_process_stdin(shell):
    test = ShellTest(shell).add_argument(
        "-c", "CREATE TABLE t(i INTEGER); COPY t FROM STDIN (FORMAT csv); SELECT sum(i) AS total FROM t;"
    )
    result = test.run_raw("1\n2\n3\n")
    result.check_stdout("6")


def test_copy_to_stdout_follows_output(shell, tmp_path):
    target = tmp_path / "copy.csv"
    result = run_script(
        shell,
        f".output '{target.as_posix()}'",
        "COPY (SELECT 42 AS answer) TO STDOUT (FORMAT csv);",
        ".output",
        "COPY (SELECT 43 AS answer) TO STDOUT (FORMAT csv);",
    )
    result.check_stdout("43")
    result.check_not_exist("42")
    assert "42" in target.read_text()


def test_copy_to_stdout_follows_backslash_o(shell, tmp_path):
    target = tmp_path / "copy.csv"
    result = run_script(
        shell,
        f"\\o '{target.as_posix()}'",
        "COPY (SELECT 'to file' AS v) TO STDOUT (FORMAT csv);",
        "\\o",
        "COPY (SELECT 'to stdout' AS v) TO STDOUT (FORMAT csv);",
    )
    result.check_stdout("to stdout")
    result.check_not_exist("to file")
    assert "to file" in target.read_text()


def test_copy_to_stdout_keeps_the_order_of_the_output(shell):
    result = run_script(
        shell,
        ".mode list",
        "SELECT 'first' AS v;",
        "COPY (SELECT 'second' AS v) TO STDOUT (FORMAT csv, HEADER false);",
        "SELECT 'third' AS v;",
    )
    stdout = result.stdout
    assert stdout.index("first") < stdout.index("second") < stdout.index("third")


def test_backslash_copy_file_round_trip(shell, tmp_path):
    source = tmp_path / "in.csv"
    source.write_text("1,one\n2,two\n")
    target = tmp_path / "out.csv"
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER, s VARCHAR);",
        f"\\copy t from '{source.as_posix()}' (format csv)",
        f"\\copy (SELECT s FROM t ORDER BY i) to '{target.as_posix()}' with (format csv, header false)",
        "SELECT count(*) AS n FROM t;",
    )
    result.check_stdout("2")
    assert target.read_text().split() == ["one", "two"]


def test_backslash_copy_column_list(shell, tmp_path):
    source = tmp_path / "in.csv"
    source.write_text("5\n")
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER, s VARCHAR DEFAULT 'filled');",
        f"\\copy t (i) from '{source.as_posix()}' (format csv)",
        "SELECT i || s AS v FROM t;",
    )
    result.check_stdout("5filled")


def test_backslash_copy_from_stdin_inline(shell):
    result = run_script(
        shell,
        "CREATE TABLE t(i INTEGER, s VARCHAR);",
        "\\copy t from stdin (format csv)",
        "1,inline",
        "\\.",
        "SELECT s FROM t;",
    )
    result.check_stdout("inline")


def test_backslash_copy_to_stdout_and_pstdout(shell, tmp_path):
    target = tmp_path / "o.txt"
    result = run_script(
        shell,
        f"\\o '{target.as_posix()}'",
        "\\copy (SELECT 'shell-out' AS v) to stdout (format csv, header false)",
        "\\copy (SELECT 'process-out' AS v) to pstdout (format csv, header false)",
        "\\o",
    )
    result.check_stdout("process-out")
    result.check_not_exist("shell-out")
    assert "shell-out" in target.read_text()


def test_backslash_copy_from_pstdin(shell):
    test = ShellTest(shell).add_argument(
        "-c", "CREATE TABLE t(i INTEGER)",
        "-c", "\\copy t from pstdin (format csv)",
        "-c", "SELECT sum(i) AS total FROM t",
    )
    result = test.run_raw("4\n5\n")
    result.check_stdout("9")


def test_backslash_copy_errors(shell, tmp_path):
    missing = tmp_path / "missing.csv"
    result = run_script(
        shell,
        "\\copy",
        "\\copy t",
        f"\\copy t from '{missing.as_posix()}'",
        "\\copy t from program 'cat'",
    )
    result.check_stderr("\\copy: arguments required")
    result.check_stderr("\\copy: parse error at end of line")
    result.check_stderr("missing.csv: ")
    result.check_stderr("\\copy: PROGRAM is not supported")
