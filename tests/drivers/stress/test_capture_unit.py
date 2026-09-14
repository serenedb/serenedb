import capture


def test_log_path_is_extracted_from_every_sanitizer_env_block():
    env = {"TSAN_OPTIONS": "detect_deadlocks=1:log_path=/nope/tsan:history_size=2"}
    assert capture.sanitizer_log_paths(env) == []


def test_no_log_path_means_nothing_to_scan():
    assert capture.sanitizer_log_paths({"TSAN_OPTIONS": "detect_deadlocks=1"}) == []
    assert capture.sanitizer_log_paths({}) == []


def test_reports_are_recognised_in_a_sanitizer_log(tmp_path, monkeypatch):
    log = tmp_path / "tsan.12345"
    log.write_text(
        "==12345==WARNING: ThreadSanitizer: lock-order-inversion (potential deadlock)\n"
        "  some frame\n"
        "==12345==WARNING: ThreadSanitizer: data race (pid=1)\n"
        "==12345==WARNING: ThreadSanitizer: lock-order-inversion (potential deadlock)\n"
        "runtime error: signed integer overflow\n"
    )
    env = {"TSAN_OPTIONS": f"log_path={tmp_path}/tsan"}
    findings, totals = capture.scan_sanitizer_logs(env)
    # Grouped by kind, deliberately: the parenthetical carries a pid and would
    # split one class into a bucket per report.
    assert totals.get("lock-order-inversion") == 2
    assert totals.get("data race") == 1
    assert findings, "a sanitizer report must surface as a finding"
    assert all(f["kind"] == "sanitizer_report_in_sanitizer_log" for f in findings)


def test_a_clean_sanitizer_log_yields_nothing(tmp_path):
    log = tmp_path / "tsan.1"
    log.write_text("***** Running under ThreadSanitizer v3 (pid 1) *****\n")
    findings, totals = capture.scan_sanitizer_logs(
        {"TSAN_OPTIONS": f"log_path={tmp_path}/tsan"})
    assert findings == [] and totals == {}
