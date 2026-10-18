import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


REPO = Path(__file__).resolve().parent.parent
SOURCE = REPO / "tests/sqllogic/fixtures/pdf"
OUT = REPO / "resources/tests/pdf"
INPUTS = [
    Path(__file__),
    SOURCE / "generate.py",
    SOURCE / "generate_rendered.py",
    REPO / "scripts/ci/pdf-fixture-requirements.txt",
]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ensure_fixture():
    fingerprint = [digest(path) for path in INPUTS]
    stamp = OUT / ".generated"
    if stamp.is_file():
        state = json.loads(stamp.read_text())
        if state["inputs"] == fingerprint and all(
            (OUT / name).is_file() and digest(OUT / name) == checksum
            for name, checksum in state["files"].items()
        ):
            return

    print(f"Generating PDF test fixtures ({OUT})...", flush=True)
    with tempfile.TemporaryDirectory(prefix="pdf-fixture-", dir=OUT.parent) as work:
        directory = Path(work)
        available = all(importlib.util.find_spec(module) is not None for module in (
            "reportlab", "pypdf", "cryptography", "pymupdf"
        ))
        if not available and shutil.which("docker") is None:
            raise RuntimeError(
                "PDF fixtures need Docker or a Python environment with "
                "scripts/ci/pdf-fixture-requirements.txt installed; "
                "set PDF_FIXTURE_PYTHON to that environment's python"
            )
        command = [sys.executable] if available else [
            "docker", "run", "--rm", "-u", f"{os.getuid()}:{os.getgid()}",
            "-e", "HOME=/tmp", "-v", f"{REPO}:{REPO}",
            os.environ.get("BUILD_IMAGE", "serenedb/serenedb-build-ubuntu:latest"),
            "python3",
        ]
        for generator, target in (("generate.py", "basic"), ("generate_rendered.py", "rendered")):
            subprocess.run(command + [str(SOURCE / generator), str(directory / target)],
                           check=True, timeout=120)
        files = {str(path.relative_to(directory)): digest(path)
                 for path in sorted(directory.rglob("*.pdf"))}
        (directory / ".generated").write_text(json.dumps({"inputs": fingerprint, "files": files}))
        for path in [directory, *directory.rglob("*")]:
            path.chmod(0o755 if path.is_dir() else 0o644)
        if OUT.exists():
            shutil.rmtree(OUT)
        directory.rename(OUT)


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with (OUT.parent / "pdf-fixture.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        ensure_fixture()
