import json
import os
import shutil
import subprocess
import time
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


def files(path, timeout=60):
    try:
        with urllib.request.urlopen(url(path.rstrip("/") + "/"), timeout=timeout) as r:
            return [e["name"] for e in json.load(r) if e["type"] == "file"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise _failed("GET", path, e) from e


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


def attribution_parquet(src, dest):
    src, dest = Path(src).resolve(), Path(dest).resolve()
    shell(f"COPY (SELECT i.unit, i.status, f.function FROM read_csv('{src}/*.txt', "
          f"columns = {{'function': 'VARCHAR'}}, header = false, delim = '\\t', quote = '', "
          f"escape = '', filename = true) f JOIN read_json('{src}/index.jsonl', "
          f"format = 'newline_delimited') i "
          f"ON regexp_extract(f.filename, '([0-9]+)\\.txt$', 1)::INTEGER = i.n) "
          f"TO '{dest}' (FORMAT parquet, COMPRESSION zstd)", dest.parent)


def timing_snapshot(work, scales, own, dest):
    units, priors = {}, {}
    if scales:
        work = Path(work).resolve()
        files = [work / f"{name}.parquet" for name in scales]
        rows = shell(f"SELECT unit, p95, parse_filename(filename, true) AS config "
                     f"FROM read_parquet([{quoted(files)}], filename = true)", work)
        for r in rows:
            if r["config"] == own:
                units[r["unit"]] = r["p95"]
            else:
                scaled = r["p95"] * scales[r["config"]]
                priors[r["unit"]] = max(scaled, priors.get(r["unit"], 0.0))
    priors = {unit: p95 for unit, p95 in priors.items() if unit not in units}
    Path(dest).write_text(json.dumps({"units": units, "priors": priors}))
