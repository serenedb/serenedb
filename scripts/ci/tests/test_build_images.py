import json
import os
import platform
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
ARCH = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
OTHER = {"amd64": "arm64", "arm64": "amd64"}[ARCH]
DOCKER_STUB = r"""#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
with open(os.environ["DOCKER_LOG"], "a") as log:
    log.write(" ".join(args) + "\n")
with open(os.environ["STUB_STATE"]) as f:
    state = json.load(f)
listed = os.environ["STUB_LISTED"].split()
present = os.environ["STUB_PRESENT"].split()


def arch_of(image):
    suffix = image.rsplit("-", 1)[-1]
    return suffix if suffix in ("amd64", "arm64") else None


if args[:3] == ["buildx", "imagetools", "inspect"]:
    image = args[3]
    if "--format" in args:
        archs = state["created"].get(image, listed)
        print("\n".join(archs))
        sys.exit(0 if archs else 1)
    if arch_of(image):
        sys.exit(0 if arch_of(image) in present or image in state["pushed"] else 1)
    sys.exit(0 if image in state["created"] or listed else 1)
if args[:3] == ["buildx", "imagetools", "create"]:
    at = args.index("--tag")
    state["created"][args[at + 1]] = [arch_of(s) for s in args[at + 2:] if arch_of(s)]
elif args[:2] == ["buildx", "build"] and os.environ.get("STUB_PUSHED_DURING_BUILD"):
    state["pushed"] += [args[i + 1] for i, a in enumerate(args) if a == "-t" and arch_of(args[i + 1])]
elif args[0] == "push":
    state["pushed"].append(args[1])
with open(os.environ["STUB_STATE"], "w") as f:
    json.dump(state, f)
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

    def run(mode="local", *args, listed="", present="", pushed_during_build=False):
        log = tmp_path / "docker.log"
        log.unlink(missing_ok=True)
        state = tmp_path / "state.json"
        state.write_text(json.dumps({"created": {}, "pushed": []}))
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", DOCKER_LOG=str(log),
                   STUB_STATE=str(state), STUB_LISTED=listed, STUB_PRESENT=present,
                   DOCKER_USERNAME="user", DOCKER_PASSWORD="secret")
        env.pop("SDB_DOCKER_OWNER", None)
        if pushed_during_build:
            env["STUB_PUSHED_DURING_BUILD"] = "1"
        res = subprocess.run(["bash", "build_images.sh", mode, *args], cwd=SCRIPTS, env=env,
                             capture_output=True, text=True)
        assert res.returncode == 0, res.stdout + res.stderr
        return log.read_text().splitlines()

    return image, run


def builds(log):
    return [line for line in log if line.startswith("buildx build")]


@pytest.mark.parametrize("mode", ["local", "arch"])
def test_an_image_that_lists_this_architecture_is_not_built(build, mode):
    image, run = build
    log = run(mode, listed="amd64 arm64")
    assert f"buildx imagetools inspect {image} --format" in " ".join(log)
    assert not builds(log)
    assert not any(line.startswith(("login", "push", "buildx imagetools create")) for line in log)


def test_local_builds_what_this_machine_lacks_without_push(build):
    image, run = build
    for listed in ("", OTHER):
        log = run("local", listed=listed)
        assert any(f"-t {image} " in line for line in builds(log))
        assert all("--load" in line and f"linux/{ARCH}" in line for line in builds(log))
        assert not any(line.startswith(("login", "push")) for line in log)


def test_arch_pushes_only_this_machines_architecture_and_joins_it(build):
    image, run = build
    log = run("arch")
    assert any(f"--load --platform linux/{ARCH} -t {image}-{ARCH} -t {image} " in line
               for line in builds(log))
    assert all(f"linux/{ARCH}" in line for line in builds(log))
    assert f"push {image}-{ARCH}" in log
    assert f"buildx imagetools create --tag {image} {image}-{ARCH}" in log
    assert not any("binfmt" in line or "--privileged" in line for line in log)


def test_arch_joins_every_architecture_the_registry_has(build):
    image, run = build
    log = run("arch", present=OTHER)
    assert f"buildx imagetools create --tag {image} {image}-amd64 {image}-arm64" in log


def test_arch_reuses_this_architectures_image_from_the_registry(build):
    image, run = build
    log = run("arch", present=ARCH)
    assert not builds(log)
    assert not any(line.startswith("push") for line in log)
    assert f"buildx imagetools create --tag {image} {image}-{ARCH}" in log


def test_arch_does_not_push_what_another_runner_pushed_meanwhile(build):
    image, run = build
    log = run("arch", pushed_during_build=True)
    assert builds(log)
    assert not any(line.startswith("push") for line in log)
    assert f"buildx imagetools create --tag {image} {image}-{ARCH}" in log


def test_arch_builds_before_it_logs_in_to_push(build):
    _, run = build
    log = run("arch")
    login = next(i for i, line in enumerate(log) if line.startswith("login"))
    assert all(i < login for i, line in enumerate(log) if line.startswith("buildx build"))
    assert all(i > login for i, line in enumerate(log)
               if line.startswith(("push", "buildx imagetools create")))


def test_arch_never_tags_latest(build):
    _, run = build
    assert not any("latest" in line for line in run("arch", present=OTHER))


def test_tag_names_every_image_without_building(build):
    image, run = build
    log = run("tag", "latest", listed="amd64 arm64")
    tagged = [line for line in log if line.startswith("buildx imagetools create")]
    assert f"buildx imagetools create --tag {image.rsplit(':', 1)[0]}:latest {image}" in tagged
    assert len(tagged) == 3
    assert not builds(log)
    assert not any(line.startswith("push") for line in log)
