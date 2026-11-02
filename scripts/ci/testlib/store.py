import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

RELEASE_IMAGE = ("serenedb/serenedb:26.10.2"
                 "@sha256:d8b14726c5c9ec3740d5d081413e6603d5ae4e6b3db82a446f0a74c42ce59ba3")


def url(path):
    host = os.environ.get("SDB_STORE_HOST", "").rstrip("/")
    if not host:
        raise RuntimeError("SDB_STORE_HOST is not set: it holds the address of the CI store")
    return f"{host}/store/{path.lstrip('/')}"


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


def files(path, timeout=60):
    try:
        with urllib.request.urlopen(url(path.rstrip("/") + "/"), timeout=timeout) as r:
            return [e["name"] for e in json.load(r) if e["type"] == "file"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise _failed("GET", path, e) from e


def run_files(kind, run, prefix):
    run_id, _, attempt = run.rpartition("-")
    attempts = range(int(attempt), 0, -1) if run_id and attempt.isdigit() else ()
    dirs = [f"{kind}/{run_id}-{a}" for a in attempts] or [f"{kind}/{run}"]
    latest = {}
    for d in dirs:
        for name in files(f"{d}/"):
            if name.startswith(prefix):
                latest.setdefault(name, f"{d}/{name}")
    return sorted(latest.values())


def recent(prefix, days):
    now = time.time()
    for i in range(days):
        day = time.strftime("%Y-%m-%d", time.gmtime(now - i * 86400))
        for name in files(f"{prefix}/{day}/"):
            yield f"{prefix}/{day}/{name}"


def quoted(values):
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def fetch_all(keys, work):
    local = []
    for key in keys:
        dest = Path(work) / key.replace("/", "_")
        if get(key, dest):
            local.append(dest)
    return local


def reachable(timeout=5):
    if not os.environ.get("SDB_STORE_HOST"):
        return False
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


def timing_snapshot(work, sources, dest):
    units, priors = {}, {}
    if sources:
        work = Path(work).resolve()
        roles = {str(Path(path).resolve()): (scale, measured) for path, scale, measured in sources}
        rows = shell(f"SELECT unit, p95, filename FROM read_parquet([{quoted(roles)}], filename = true)",
                     work)
        for r in rows:
            scale, measured = roles[r["filename"]]
            if measured:
                units[r["unit"]] = r["p95"]
            else:
                priors[r["unit"]] = max(r["p95"] * scale, priors.get(r["unit"], 0.0))
    priors = {unit: p95 for unit, p95 in priors.items() if unit not in units}
    Path(dest).write_text(json.dumps({"units": units, "priors": priors}))
