import os
import platform
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
ARCH = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
DOCKER_STUB = r"""#!/bin/bash
echo "$*" >> "$DOCKER_LOG"
if [[ "$1 $2 $3" == "buildx imagetools inspect" ]]; then
  exit "$STUB_INSPECT_RC"
fi
"""


@pytest.fixture()
def build(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(DOCKER_STUB)
    docker.chmod(0o755)
    image = subprocess.run(["python3", "ci.py", "image"], cwd=SCRIPTS, capture_output=True,
                           text=True, check=True).stdout.strip()

    def run(inspect_rc, mode="local"):
        log = tmp_path / "docker.log"
        log.unlink(missing_ok=True)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", DOCKER_LOG=str(log),
                   STUB_INSPECT_RC=str(inspect_rc), DOCKER_USERNAME="user", DOCKER_PASSWORD="secret")
        env.pop("SDB_DOCKER_OWNER", None)
        res = subprocess.run(["bash", "build_images.sh", mode], cwd=SCRIPTS, env=env,
                             capture_output=True, text=True)
        assert res.returncode == 0, res.stdout + res.stderr
        return log.read_text().splitlines(), res.stdout

    return image, run


def test_an_image_the_registry_has_is_not_built(build):
    image, run = build
    log, _ = run(inspect_rc=0)
    assert f"buildx imagetools inspect {image}" in log
    assert not any(line.startswith("buildx build") for line in log)


def test_a_missing_image_is_built_for_this_machine_without_push(build):
    image, run = build
    log, _ = run(inspect_rc=1)
    builds = [line for line in log if line.startswith("buildx build")]
    assert any(f"-t {image} " in line for line in builds)
    assert all("--load" in line and f"linux/{ARCH}" in line for line in builds)
    assert not any("--push" in line for line in log)


def test_missing_lists_what_the_registry_lacks_and_builds_nothing(build):
    image, run = build
    log, out = run(inspect_rc=1, mode="missing")
    assert image in out.split()
    assert not any(line.startswith("buildx build") for line in log)
    assert run(inspect_rc=0, mode="missing")[1] == ""


def test_arch_pushes_only_this_machines_architecture_without_emulation(build):
    image, run = build
    log, _ = run(inspect_rc=1, mode="arch")
    builds = [line for line in log if line.startswith("buildx build")]
    assert any(f"--load --platform linux/{ARCH} -t {image}-{ARCH} " in line for line in builds)
    assert all(f"linux/{ARCH}" in line for line in builds)
    assert f"push {image}-{ARCH}" in log
    assert not any("binfmt" in line or "--privileged" in line for line in log)


def test_arch_builds_before_it_logs_in_to_push(build):
    _, run = build
    log, _ = run(inspect_rc=1, mode="arch")
    login = next(i for i, line in enumerate(log) if line.startswith("login"))
    assert all(i < login for i, line in enumerate(log) if line.startswith("buildx"))
    assert all(i > login for i, line in enumerate(log) if line.startswith("push"))


def test_merge_joins_both_architectures_and_never_as_latest(build):
    image, run = build
    log, _ = run(inspect_rc=1, mode="merge")
    assert f"buildx imagetools create --tag {image} {image}-amd64 {image}-arm64" in log
    assert not any(line.startswith("buildx build") for line in log)
    assert not any("latest" in line for line in log)
