import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_URL = "http://store-host:8640/store"
RELEASE_IMAGE = "serenedb/serenedb:latest"


def url(path):
    base = os.environ.get("SDB_STORE", DEFAULT_URL).rstrip("/")
    return f"{base}/{path.lstrip('/')}"


def _failed(method, path, e):
    body = e.read().decode(errors="replace").strip()
    return RuntimeError(f"{method} {url(path)}: HTTP {e.code} {body}".rstrip())


def get(path, dest, timeout=600):
    try:
        with urllib.request.urlopen(url(path), timeout=timeout) as r, open(dest, "wb") as fh:
            shutil.copyfileobj(r, fh, 1 << 20)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise _failed("GET", path, e) from e


def exists(path, timeout=30):
    req = urllib.request.Request(url(path), method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise _failed("HEAD", path, e) from e


def put(path, src, timeout=1800):
    size = os.path.getsize(src)
    with open(src, "rb") as fh:
        req = urllib.request.Request(url(path), data=fh, method="PUT",
                                     headers={"Content-Length": str(size)})
        try:
            with urllib.request.urlopen(req, timeout=timeout):
                pass
        except urllib.error.HTTPError as e:
            raise _failed("PUT", path, e) from e


def reachable(timeout=5):
    try:
        req = urllib.request.Request(url(""), method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except OSError:
        return False


def shell(sql, workdir):
    workdir = Path(workdir).resolve()
    local = os.environ.get("SDB_SHELL")
    if local:
        argv = [local, "shell", "-jsonlines", "-c", sql]
        res = subprocess.run(argv, cwd=workdir, capture_output=True, text=True, timeout=1800)
    else:
        argv = ["docker", "run", "--rm", "--entrypoint", "serened",
                "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                "-v", f"{workdir}:{workdir}", "-w", str(workdir), RELEASE_IMAGE,
                "shell", "-jsonlines", "-c", sql]
        res = subprocess.run(argv, capture_output=True, text=True, timeout=1800)
    if res.returncode != 0:
        raise RuntimeError(f"serened shell failed ({res.returncode}): {res.stderr[-2000:]}")
    rows = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def jsonl_to_parquet(src, dest):
    src, dest = Path(src).resolve(), Path(dest).resolve()
    shell(f"COPY (SELECT * FROM read_json('{src}', format = 'newline_delimited')) "
          f"TO '{dest}' (FORMAT parquet, COMPRESSION zstd)", src.parent)


def timing_snapshot(parquet, dest):
    parquet = Path(parquet).resolve()
    rows = shell(f"SELECT unit, p95 FROM read_parquet('{parquet}')", parquet.parent)
    Path(dest).write_text(json.dumps({"units": {r["unit"]: r["p95"] for r in rows}}))
