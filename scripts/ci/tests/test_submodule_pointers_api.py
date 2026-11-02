import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "check_submodule_pointers.py"
OLD = "1" * 40
NEW = "2" * 40
HEADS = {"v2026.09.12": OLD, "v2026.10.20": NEW}


@pytest.fixture()
def checker():
    spec = importlib.util.spec_from_file_location("check_submodule_pointers_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def github(monkeypatch, checker, compare, commits):
    def api_get(path):
        _, _, _, kind, rest = path.split("/", 4)
        if kind == "compare":
            status = compare.get(tuple(rest.split("...")))
            return (200, {"status": status}) if status else (404, None)
        if kind == "commits" and "/" not in rest:
            return (200, {"sha": rest}) if rest in commits else (422, None)
        raise AssertionError(path)

    monkeypatch.setattr(checker, "api_get", api_get)


def test_a_release_import_with_no_history_in_common_with_mains_gitlink_passes(monkeypatch, checker):
    github(monkeypatch, checker, {("v2026.09.12", OLD): "identical", ("v2026.10.20", NEW): "identical"},
           {OLD, NEW})
    assert checker.validate_api("serenedb/fork", NEW, OLD, HEADS, lambda: None) == ("ok", "on v2026.10.20")


def test_a_commit_the_fork_does_not_have_fails(monkeypatch, checker):
    github(monkeypatch, checker, {("v2026.09.12", OLD): "identical"}, {OLD})
    status, detail = checker.validate_api("serenedb/fork", NEW, OLD, HEADS, lambda: None)
    assert status == "fail" and "unknown" in detail
