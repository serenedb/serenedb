import os
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
DOCKER_STUB = r"""#!/bin/bash
echo "$*" >> "$DOCKER_LOG"
if [[ "$1 $2 $3" == "buildx imagetools inspect" ]]; then
  exit "$STUB_INSPECT_RC"
fi
for arg in "$@"; do
  if [[ "$arg" == type=docker,dest=* ]]; then
    : > "${arg#type=docker,dest=}"
  fi
done
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

    def run(inspect_rc, push=False):
        log = tmp_path / "docker.log"
        log.unlink(missing_ok=True)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", DOCKER_LOG=str(log),
                   STUB_INSPECT_RC=str(inspect_rc), PUSH_IMAGES_2_REGISTRY=str(push).lower(),
                   DOCKER_USERNAME="user", DOCKER_PASSWORD="secret")
        env.pop("SDB_DOCKER_OWNER", None)
        res = subprocess.run(["bash", "build_images.sh"], cwd=SCRIPTS, env=env,
                             capture_output=True, text=True)
        assert res.returncode == 0, res.stdout + res.stderr
        return log.read_text().splitlines()

    return image, run


def test_an_image_the_registry_has_is_not_built(build):
    image, run = build
    log = run(inspect_rc=0)
    assert f"buildx imagetools inspect {image}" in log
    assert not any(line.startswith("buildx build") for line in log)


def test_a_missing_image_is_built_for_this_machine_without_push(build):
    image, run = build
    log = run(inspect_rc=1)
    builds = [line for line in log if line.startswith("buildx build")]
    assert any(f"-t {image} " in line for line in builds)
    assert all("--load" in line for line in builds)
    assert not any(line.startswith("push") for line in log)


def test_a_missing_image_is_pushed_for_both_architectures_and_never_as_latest(build):
    image, run = build
    log = run(inspect_rc=1, push=True)
    assert f"push {image}-amd64" in log
    assert f"push {image}-arm64" in log
    assert f"buildx imagetools create --tag {image} {image}-amd64 {image}-arm64" in log
    assert not any("latest" in line for line in log)
