# fmt: off

from shell_harness import ShellTest, requires_serenedb, shell  # noqa: F401

pytestmark = requires_serenedb


def run_script(shell, *lines, arguments=()):
    test = ShellTest(shell)
    if arguments:
        test.add_argument(*arguments)
    return test.run_raw("\n".join(lines) + "\n")


def test_echo(shell):
    result = run_script(shell, "\\echo hello   world", "\\echo -n a", "\\echo b", "\\echo 'tab\\there' \"quoted\"")
    result.check_stdout('hello world\nab\ntab\there "quoted"')


def test_echo_from_command_line(shell):
    result = ShellTest(shell).add_argument("-c", "\\echo from -c").run()
    result.check_stdout("from -c")


def test_unknown_command(shell):
    result = run_script(shell, "\\nosuch", "SELECT 'still running' AS s;")
    result.check_stderr("invalid command \\nosuch")
    assert "still running" in result.stdout


def test_catalog_commands_are_not_supported(shell):
    result = run_script(shell, "\\dt", "\\d t")
    result.check_stderr("\\dt: catalog commands are not supported")
    result.check_stderr("\\d: catalog commands are not supported")


def test_extra_argument_is_ignored(shell):
    result = run_script(shell, "\\a extra")
    result.check_stderr('\\a: extra argument "extra" ignored')
    result.check_stdout("Output format is aligned.")


def test_several_commands_on_a_line(shell):
    result = run_script(shell, "\\a \\t on")
    result.check_stdout("Output format is aligned.\nTuples only is on.")


def test_quit(shell):
    result = run_script(shell, "SELECT 'before' AS s;", "\\q", "SELECT 'after' AS s;")
    result.check_stdout("before")
    result.check_not_exist("after")


def test_quit_from_included_file(shell, tmp_path):
    included = tmp_path / "quit.sql"
    included.write_text("SELECT 'inside' AS s;\n\\q\n")
    result = run_script(shell, f"\\i '{included.as_posix()}'", "SELECT 'after' AS s;")
    result.check_stdout("inside")
    result.check_not_exist("after")


def test_include(shell, tmp_path):
    included = tmp_path / "included.sql"
    included.write_text("SELECT 'from include' AS s;\n")
    result = run_script(shell, f"\\i '{included.as_posix()}'", f"\\include '{included.as_posix()}'")
    assert result.stdout.count("from include") == 2


def test_include_missing_argument(shell):
    result = run_script(shell, "\\i")
    result.check_stderr("\\i: missing required argument")


def test_output_redirect(shell, tmp_path):
    target = tmp_path / "out.txt"
    result = run_script(
        shell,
        f"\\o '{target.as_posix()}'",
        "SELECT 'to file' AS s;",
        "\\echo echo goes to stdout",
        "\\o",
        "SELECT 'to stdout' AS s;",
    )
    result.check_stdout("echo goes to stdout")
    result.check_stdout("to stdout")
    result.check_not_exist("to file")
    assert "to file" in target.read_text()


def test_output_shared_with_dot_output(shell, tmp_path):
    target = tmp_path / "out.txt"
    result = run_script(shell, f".output '{target.as_posix()}'", "SELECT 'to file' AS s;", "\\o", "SELECT 'back' AS s;")
    result.check_stdout("back")
    result.check_not_exist("to file")
    assert "to file" in target.read_text()


def test_g_runs_the_buffer(shell, tmp_path):
    target = tmp_path / "g.txt"
    result = run_script(
        shell,
        "SELECT 'buffered' AS s",
        "\\g",
        "SELECT 'once'",
        "  AS s",
        f"\\g '{target.as_posix()}'",
        "SELECT 'later' AS s;",
    )
    result.check_stdout("buffered")
    result.check_stdout("later")
    result.check_not_exist("once")
    assert "once" in target.read_text()


def test_g_reruns_the_last_query(shell):
    result = run_script(shell, "SELECT 'again' AS s;", "\\g")
    assert result.stdout.count("again") == 2


def test_gx(shell):
    result = run_script(shell, ".mode psql", "SELECT 1 AS a, 2 AS b", "\\gx", "SELECT 3 AS c, 4 AS d;")
    result.check_stdout("-[ RECORD 1 ]\na | 1\nb | 2")
    result.check_stdout("c | d \n---+---\n 3 | 4")


def test_toggles(shell):
    result = run_script(shell, ".mode psql", "\\x", "\\x", "\\a", "\\a", "\\t", "\\t off", "\\x auto")
    result.check_stdout("\n".join([
        "Expanded display is on.",
        "Expanded display is off.",
        "Output format is unaligned.",
        "Output format is aligned.",
        "Tuples only is on.",
        "Tuples only is off.",
        "Expanded display is used automatically.",
    ]))


def test_expanded_in_duckdb_mode_is_line_mode(shell):
    result = run_script(shell, "\\x", "SELECT 1 AS a;")
    result.check_stdout("Expanded display is on.")
    result.check_stdout("a = 1")


def test_pset_lists_settings(shell):
    result = run_script(shell, ".mode psql", "\\pset")
    result.check_stdout("border".ljust(24) + " 1")
    result.check_stdout("format".ljust(24) + " aligned")
    result.check_stdout("null".ljust(24) + " ''")
    result.check_stdout("tuples_only".ljust(24) + " off")


def test_pset_shows_a_setting(shell):
    result = run_script(shell, ".mode psql", "\\pset format", "\\pset null", "\\pset recordsep")
    result.check_stdout('Output format is aligned.\nNull display is "".\nRecord separator is <newline>.')


def test_pset_errors(shell):
    result = run_script(shell, "\\pset nosuch on", "\\pset format nosuch", "\\pset tuples_only maybe")
    result.check_stderr("\\pset: unknown option: nosuch")
    result.check_stderr('\\pset: format "nosuch" is not supported')
    result.check_stderr('\\pset: unrecognized value "maybe" for "tuples_only": Boolean expected')


def test_timing(shell):
    result = run_script(shell, "\\timing on", "SELECT 1;", "\\timing")
    result.check_stdout("Timing is on.")
    result.check_stdout("Run Time")
    result.check_stdout("Timing is off.")


def test_timing_shared_with_dot_timer(shell):
    result = run_script(shell, ".timer on", "\\timing")
    result.check_stdout("Timing is off.")


def test_help(shell):
    result = run_script(shell, "\\?")
    result.check_stdout("\\copy ...")
    result.check_stdout("\\pset [NAME [VALUE]]")
    result.check_stdout("\\x [on|off|auto]")


def test_help_commands_flag(shell):
    result = ShellTest(shell).add_argument("--help=commands").run()
    result.check_stdout("\\timing [on|off]")


def test_list_databases(shell):
    result = run_script(shell, "\\l")
    result.check_stdout("database_name")


def test_backslash_inside_a_literal_is_sql(shell):
    result = run_script(shell, ".mode psql", "\\t", "SELECT 'a", "\\x", "b' AS s;")
    result.check_stdout(" a +\n \\x+\n b")
    result.check_not_exist("Expanded display")


def test_command_keeps_the_query_buffer(shell):
    result = run_script(shell, ".mode psql", "SELECT 1 AS a", "\\x", ";")
    result.check_stdout("Expanded display is on.\n-[ RECORD 1 ]\na | 1")
