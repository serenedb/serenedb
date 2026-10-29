import fcntl
import importlib.util
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

CI = Path(__file__).resolve().parents[1] / "ci.py"
GB = 1 << 30


def load_ci():
    spec = importlib.util.spec_from_file_location("ci_under_test", CI)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bundle(root, name, hours_ago, complete=True):
    path = root / name
    path.mkdir()
    if complete:
        (path / ".complete").write_text(name)
    used = time.time() - hours_ago * 3600
    os.utime(path, (used, used))
    return path


def names(root):
    return {path.name for path in root.iterdir()}


@pytest.fixture()
def cache(tmp_path, monkeypatch):
    ci = load_ci()
    monkeypatch.setattr(ci, "BUNDLE_CACHE", tmp_path)
    monkeypatch.setattr(ci.shutil, "disk_usage", lambda _: SimpleNamespace(
        free=(10 - sum(not path.name.startswith(".") for path in tmp_path.iterdir())) * GB))
    return ci, tmp_path


def test_a_bundle_unused_for_two_days_goes_whatever_the_free_space(cache):
    ci, root = cache
    bundle(root, "old", 49)
    bundle(root, "new", 1)
    ci.prune_bundles(min_free=0)
    assert names(root) == {"new"}


def test_the_least_recently_used_go_until_the_disk_has_the_floor_free(cache):
    ci, root = cache
    for name, hours_ago in (("a", 5), ("b", 1), ("c", 30), ("d", 10), ("e", 20)):
        bundle(root, name, hours_ago)
    ci.prune_bundles(min_free=7 * GB)
    assert names(root) == {"a", "b", "d"}


def test_a_bundle_in_use_stays(cache):
    ci, root = cache
    used = bundle(root, "used", 30)
    bundle(root, "idle", 1)
    with open(used / ".complete") as marker:
        fcntl.flock(marker, fcntl.LOCK_SH)
        ci.prune_bundles(min_free=10 * GB)
    assert names(root) == {"used"}


def test_an_unfinished_dir_goes_only_when_old(cache):
    ci, root = cache
    bundle(root, ".fetch-new", 1, complete=False)
    bundle(root, ".stage-new", 1)
    bundle(root, ".fetch-old", 49, complete=False)
    ci.prune_bundles(min_free=11 * GB)
    assert names(root) == {".fetch-new", ".stage-new"}


def test_the_ci_images_neither_the_checkout_uses_nor_newest_go():
    ci = load_ci()
    build = f"{ci.IMAGE_REPO}:mine"
    postgres = f"{ci.FIXTURE_IMAGE_REPO}postgres:18.6-aaaaaaaa"
    rows = [
        (f"{ci.IMAGE_REPO}:newest", "2026-10-29 01"), (f"{ci.IMAGE_REPO}:newest-amd64", "2026-10-29 02"),
        (f"{ci.IMAGE_REPO}:newest-arm64", "2026-10-30"), (f"{ci.IMAGE_REPO}:between", "2026-10-27"),
        (build, "2026-10-25"), (f"{build}-amd64", "2026-10-25"), (f"{ci.IMAGE_REPO}:oldest", "2026-10-20"),
        (f"{ci.IMAGE_REPO}:<none>", "2026-10-21"), (postgres, "2026-10-10"),
        (f"{ci.FIXTURE_IMAGE_REPO}postgres:18.6-bbbbbbbb", "2026-10-01"),
        ("postgres:18.6", "2026-10-02"), ("serenedb/serenedb:26.10.6", "2026-10-03"),
    ]
    assert ci.stale_images(rows, {build, postgres}, "amd64") == [
        f"{ci.IMAGE_REPO}:newest-arm64", f"{ci.IMAGE_REPO}:between", f"{ci.IMAGE_REPO}:oldest",
        f"{ci.FIXTURE_IMAGE_REPO}postgres:18.6-bbbbbbbb"]
