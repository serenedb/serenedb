import base64
import hashlib
import hmac
import http.client
import json
import os
import random
import re
import shutil
import socket
import string
import subprocess
import tempfile
import time
import urllib.request
from email.utils import formatdate
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
FIXTURES = REPO / "tests" / "sqllogic" / "fixtures"

MINIO_IMAGE = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
POD_IMAGE = "alpine:3.22"
AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0"
ICEBERG_REST_IMAGE = "apache/iceberg-rest-fixture:1.10.1"
KEV_IMAGE = "serenedb/serenedb-test-kev:1.0-0.5b-1534a08c"
POSTGRES_IMAGE = "postgres:18.6"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.8"
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
               "K1SZFPTOtr/KBHBeksoGMGw==")

SUFFIX_SERVICES = {
    "_s3": ("minio",),
    "_azure": ("azurite",),
    "_iceberg": ("iceberg",),
    "_ollama": ("ollama",),
    "_kev": ("kev",),
    "_pgscan": ("postgres",),
    "_pgscan_tls": ("postgres",),
    "_chscan": ("clickhouse",),
}
DEPENDS = {"iceberg": ("minio",)}


def fixture_image_tag(name):
    dockerfile = (FIXTURES / name / "Dockerfile").read_text()
    m = re.search(r"^ARG [A-Z_]*VERSION=(\S+)", dockerfile, re.M)
    digest = hashlib.sha256((FIXTURES / name / "Dockerfile").read_bytes()).hexdigest()[:8]
    return f"{m.group(1) if m else 'latest'}-{digest}"


def services_for_test(path):
    name = os.path.basename(path)
    stem, ext = os.path.splitext(name)
    found = []
    for suffix, services in SUFFIX_SERVICES.items():
        if stem.endswith(suffix):
            if ext != ".test_slow":
                raise ValueError(f"{path}: a test that starts a service must be named *.test_slow")
            found = list(services)
    try:
        if "${RESOURCES}/tests/iceberg" in Path(path).read_text(errors="replace"):
            found.append("iceberg-data")
    except OSError:
        pass
    return found


def with_dependencies(names):
    out = []
    for name in names:
        for dep in DEPENDS.get(name, ()):
            if dep not in out:
                out.append(dep)
        if name not in out:
            out.append(name)
    return out


def _tag():
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(4))


def _owner_args():
    owner = os.environ.get("SDB_DOCKER_OWNER")
    return ["--label", f"serenedb.owner={owner}"] if owner else []


def _docker(*args, check=True, capture=False, input_bytes=None, timeout=600):
    return subprocess.run(
        ["docker", *args], check=check, input=input_bytes, timeout=timeout,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
        stderr=subprocess.PIPE if capture else subprocess.DEVNULL,
    )


def wait_for(what, predicate, timeout, interval=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise RuntimeError(f"{what} not ready within {timeout:.0f}s")


def tcp_open(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout):
            return True
    except OSError:
        return False


_wait = wait_for


class ServiceSet:
    def __init__(self, bind=None, host=None, log_dir=None, prefix="sdb"):
        self.bind = bind or os.environ.get("SDB_SERVICE_BIND", "127.0.0.1")
        self.host = host or os.environ.get("SDB_SERVICE_HOST", self.bind)
        self.netns = os.environ.get("SDB_SERVICE_NETNS", "")
        self.log_dir = Path(log_dir) if log_dir else None
        self.name = f"{_tag()}-{prefix}-{os.getpid()}"
        self.network = None
        self.containers = {}
        self.pods = {}
        self.started = set()
        self.env = {}
        self.tmp_dirs = []

    def _ensure_network(self):
        if self.network is None:
            self.network = f"{self.name}-net"
            _docker("network", "create", *_owner_args(), self.network)
        return self.network

    def pod(self, peer, fixed_ports):
        if self.netns or peer in self.pods:
            return
        name = f"{self.name}-{peer}-pod"
        publish = [a for ip, port in fixed_ports for a in ("-p", f"{ip}:{port}:{port}")]
        _docker("run", "-d", "--name", name, *_owner_args(), "--network", self._ensure_network(),
                *publish, POD_IMAGE, "sleep", "infinity")
        self.pods[peer] = name
        self.containers[f"{peer}-pod"] = name

    def join(self, peer, container):
        if not self.netns:
            self.pods[peer] = container

    def run(self, service, image, *args, ports=(), fixed_ports=(), extra=(), peer=None):
        name = f"{self.name}-{service}"
        if fixed_ports or peer:
            if self.netns:
                network = ["--network", self.netns]
            elif peer:
                self.pod(peer, fixed_ports)
                network = ["--network", f"container:{self.pods[peer]}"]
            else:
                network = ["--network", self._ensure_network()]
                network += [a for ip, port in fixed_ports for a in ("-p", f"{ip}:{port}:{port}")]
        else:
            network = ["--network", self._ensure_network()]
            for port in ports:
                network += ["-p", f"{self.bind}::{port}"]
        _docker("run", "-d", "--name", name, *_owner_args(), *network, *extra, image, *args)
        self.containers[service] = name
        return name

    def oneshot(self, service, image, args, extra=(), peer=None, timeout=300):
        name = f"{self.name}-{service}"
        if self.netns:
            network = ["--network", self.netns]
        elif peer:
            network = ["--network", f"container:{self.pods[peer]}"]
        else:
            network = ["--network", self._ensure_network()]
        res = _docker("run", "--rm", "--name", name, *_owner_args(), *network, *extra, image, *args,
                      check=False, capture=True, timeout=timeout)
        if res.returncode != 0:
            raise RuntimeError(f"{service} failed: {res.stderr.decode()[-1000:]}")

    def address(self, name, port):
        return self.host, self._port(name, port)

    def _port(self, name, port):
        out = _docker("port", name, f"{port}/tcp", capture=True).stdout.decode()
        for line in out.splitlines():
            return line.rsplit(":", 1)[1].strip()
        raise RuntimeError(f"{name} publishes no port {port}")

    _run = run

    def start(self, names):
        for name in with_dependencies(names):
            if name not in self.started:
                getattr(self, "_start_" + name.replace("-", "_"))()
                self.started.add(name)
        return dict(self.env)

    def _start_iceberg_data(self):
        subprocess.run([str(REPO / "scripts" / "ensure_iceberg_fixture.sh")], check=True)

    def _start_biglake(self):
        self.env.update(biglake_env())

    def _start_minio(self):
        access, secret, bucket = "minioadmin", "minioadmin", "testbucket"
        name = self._run("minio", MINIO_IMAGE, "server", "/data", "--console-address", ":9001",
                         ports=(9000,), extra=["--ulimit", "nofile=65536:65536",
                                               "-e", f"MINIO_ROOT_USER={access}",
                                               "-e", f"MINIO_ROOT_PASSWORD={secret}"])
        _wait("MinIO", lambda: _docker("exec", name, "mc", "alias", "set", "local",
                                      "http://127.0.0.1:9000", access, secret,
                                      check=False).returncode == 0, 60)
        _docker("exec", name, "mc", "mb", f"local/{bucket}")
        self.env.update(MINIO_HOST=self.host, MINIO_PORT=self._port(name, 9000),
                        MINIO_ACCESS_KEY=access, MINIO_SECRET_KEY=secret, MINIO_BUCKET=bucket)

    def _start_azurite(self):
        container = "testcont"
        name = self._run("azurite", AZURITE_IMAGE, "azurite-blob", "--blobHost", "0.0.0.0",
                         "--skipApiVersionCheck", ports=(10000,))
        port = self._port(name, 10000)
        _wait("Azurite", lambda: _azurite_create_container(self.host, port, container), 60)
        self.env.update(
            AZURITE_BLOB_CONTAINER=container,
            AZURITE_CONNECTION_STRING=(
                f"DefaultEndpointsProtocol=http;AccountName={AZURITE_ACCOUNT};"
                f"AccountKey={AZURITE_KEY};BlobEndpoint=http://{self.host}:{port}/{AZURITE_ACCOUNT};"),
        )

    def _start_iceberg(self):
        minio = self.containers["minio"]
        name = self._run("iceberg", ICEBERG_REST_IMAGE, ports=(8181,), extra=[
            "--ulimit", "nofile=65536:65536",
            "-e", f"AWS_ACCESS_KEY_ID={self.env['MINIO_ACCESS_KEY']}",
            "-e", f"AWS_SECRET_ACCESS_KEY={self.env['MINIO_SECRET_KEY']}",
            "-e", "AWS_REGION=us-east-1",
            "-e", f"CATALOG_WAREHOUSE=s3://{self.env['MINIO_BUCKET']}/warehouse/",
            "-e", "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
            "-e", f"CATALOG_S3_ENDPOINT=http://{minio}:9000",
            "-e", "CATALOG_S3_PATH__STYLE__ACCESS=true",
            "-e", "CATALOG_URI=jdbc:sqlite:/tmp/iceberg_catalog.db?journal_mode=WAL&busy_timeout=30000",
        ])
        url = f"http://{self.host}:{self._port(name, 8181)}"
        warehouse = "demo"

        def answering():
            try:
                with urllib.request.urlopen(f"{url}/v1/config?warehouse={warehouse}", timeout=3) as r:
                    return r.status == 200
            except Exception:
                return False

        _wait("iceberg-rest", answering, 90)
        e = self.env
        self.env.update(
            ICEBERG_REST_URL=url,
            ICEBERG_WAREHOUSE=warehouse,
            ICEBERG_FIXTURES=str(REPO / "third_party" / "duckdb_iceberg" / "data" / "persistent"),
            ICEBERG_BOOTSTRAP=(
                "CREATE OR REPLACE PERSISTENT SECRET iceberg_ci_storage (TYPE S3, "
                f"KEY_ID '{e['MINIO_ACCESS_KEY']}', SECRET '{e['MINIO_SECRET_KEY']}', "
                f"ENDPOINT '{e['MINIO_HOST']}:{e['MINIO_PORT']}', URL_STYLE 'path', USE_SSL false, "
                f"SCOPE 's3://{e['MINIO_BUCKET']}/warehouse/');"),
            ICEBERG_SERVER_OPTIONS=(
                f"warehouse '{warehouse}', uri '{url}', authorization_type 'none'"),
        )

    def _start_ollama(self):
        image = f"serenedb/serenedb-test-ollama:{fixture_image_tag('ollama')}"
        name = self._run("ollama", image, ports=(11434,))
        _wait("Ollama", lambda: _docker("exec", name, "ollama", "list",
                                       check=False).returncode == 0, 60)
        model = os.environ.get("OLLAMA_MODEL", "all-minilm")
        chat = os.environ.get("OLLAMA_CHAT_MODEL", "qwen2.5:0.5b")
        for m in (model, chat):
            if _docker("exec", name, "ollama", "show", m, check=False).returncode != 0:
                raise RuntimeError(f"Ollama model {m} is not in {image}; add it to "
                                   "tests/sqllogic/fixtures/ollama/Dockerfile")
        port = self._port(name, 11434)
        for path, body in (("/api/embed", {"model": model, "input": "warm", "keep_alive": -1}),
                           ("/api/generate", {"model": chat, "prompt": "", "keep_alive": -1})):
            request = urllib.request.Request(
                f"http://{self.host}:{port}{path}", data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=600) as r:
                r.read()
        self.env.update(OLLAMA_HOST=self.host, OLLAMA_PORT=port,
                        OLLAMA_MODEL=model, OLLAMA_CHAT_MODEL=chat)

    def _start_kev(self):
        name = self._run("kev", KEV_IMAGE, ports=(8009,))
        probe = "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8009/v1/models')"
        _wait("Kev", lambda: _docker("exec", name, "python", "-c", probe,
                                    check=False).returncode == 0, 180)
        self.env.update(KEV_HOST=self.host, KEV_PORT=self._port(name, 8009))

    def _start_postgres(self):
        sock_root = Path(os.environ.get("SDB_SERVICE_SOCKET_ROOT", tempfile.gettempdir()))
        sock_root.mkdir(parents=True, exist_ok=True)
        sock = Path(tempfile.mkdtemp(prefix="sdb-pgsocket-", dir=sock_root))
        sock.chmod(0o777)
        self.tmp_dirs.append(sock)
        name = self._run("postgres", POSTGRES_IMAGE, "-c", "wal_level=logical",
                         "-c", "max_replication_slots=64", "-c", "max_wal_senders=64",
                         ports=(5432,), extra=["-v", f"{sock}:/var/run/postgresql",
                                               "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                                               "-e", "POSTGRES_DB=postgres"])
        _wait("postgres", lambda: _docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U",
                                         "postgres", check=False).returncode == 0, 60)
        self.env.update(PGHOST=self.host, PGPORT=self._port(name, 5432), PGUSER="postgres",
                        PGDATABASE="postgres", PGSOCKETDIR=str(sock))

    def _start_clickhouse(self):
        fixtures_sql = FIXTURES / "clickhouse" / "01_init.sql"
        name = self._run("clickhouse", CLICKHOUSE_IMAGE, ports=(9000,), extra=[
            "-e", "CLICKHOUSE_SKIP_USER_SETUP=1",
            "--health-cmd", 'clickhouse-client --query "SELECT 1"',
            "--health-interval", "5s", "--health-timeout", "3s", "--health-retries", "3",
            "--health-start-period", "60s",
        ])
        _wait("clickhouse", lambda: _docker("exec", name, "clickhouse-client", "--query",
                                           "SELECT 1", check=False).returncode == 0, 60)
        _docker("exec", "-i", name, "clickhouse-client", "--multiquery",
                input_bytes=fixtures_sql.read_bytes())

        def loaded():
            res = _docker("exec", name, "clickhouse-client", "--query",
                          "SELECT count() FROM system.tables WHERE database='chtest'",
                          check=False, capture=True)
            out = res.stdout.decode().strip()
            return out.isdigit() and int(out) >= 3

        _wait("clickhouse fixtures", loaded, 60)
        port = self._port(name, 9000)
        _wait("clickhouse native protocol", lambda: _clickhouse_answers(self.host, port), 60)
        self.env.update(CHHOST=self.host, CHPORT=port, CHUSER="default")

    def logs(self):
        if not self.log_dir:
            return
        self.log_dir.mkdir(parents=True, exist_ok=True)
        for service, name in self.containers.items():
            res = _docker("logs", name, check=False, capture=True)
            (self.log_dir / f"{service}.log").write_bytes(res.stdout + res.stderr)

    def health(self):
        dead = []
        for service, name in self.containers.items():
            res = _docker("inspect", "-f", "{{.State.Status}} exit={{.State.ExitCode}} "
                          "oom={{.State.OOMKilled}}", name, check=False, capture=True)
            state = res.stdout.decode().strip() or "gone"
            if not state.startswith("running"):
                dead.append(f"{service}: {state}")
        return dead

    def stop(self):
        self.logs()
        for name in self.containers.values():
            _docker("rm", "-fv", name, check=False)
        self.containers.clear()
        if self.network:
            _docker("network", "rm", self.network, check=False)
            self.network = None
        for d in self.tmp_dirs:
            shutil.rmtree(d, ignore_errors=True)
            if d.exists():
                _docker("run", "--rm", *_owner_args(), "-v", f"{d.parent}:/parent", POD_IMAGE,
                        "rm", "-rf", f"/parent/{d.name}", check=False)
        self.tmp_dirs.clear()


def _azurite_create_container(host, port, container):
    date = formatdate(usegmt=True)
    to_sign = (f"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:2021-08-06\n"
               f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}/{container}\nrestype:container")
    sig = base64.b64encode(hmac.new(base64.b64decode(AZURITE_KEY), to_sign.encode(),
                                    hashlib.sha256).digest()).decode()
    try:
        conn = http.client.HTTPConnection(host, int(port), timeout=2)
        conn.request("PUT", f"/{AZURITE_ACCOUNT}/{container}?restype=container",
                     headers={"x-ms-date": date, "x-ms-version": "2021-08-06",
                              "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{sig}",
                              "Content-Length": "0"})
        status = conn.getresponse().status
    except OSError:
        return False
    if status in (201, 409):
        return True
    raise RuntimeError(f"Azurite container create failed: {status}")


def _clickhouse_answers(host, port):
    def uv(n):
        out = bytearray()
        while True:
            b = n & 0x7F
            n >>= 7
            out.append(b | 0x80 if n else b)
            if not n:
                return bytes(out)

    def us(s):
        b = s.encode()
        return uv(len(b)) + b

    try:
        with socket.create_connection((host, int(port)), 2) as s:
            s.sendall(uv(0) + us("readycheck") + uv(24) + uv(8) + uv(54448) + us("default")
                      + us("default") + us(""))
            return bool(s.recv(1))
    except OSError:
        return False


def biglake_env():
    project = os.environ["BIGLAKE_PROJECT"]
    catalog = os.environ["BIGLAKE_CATALOG"]

    def lit(s):
        return "'" + s.replace("'", "''") + "'"

    if os.environ.get("BIGLAKE_CLIENT_EMAIL"):
        body = (f"TYPE ICEBERG, PROVIDER google, CLIENT_EMAIL {lit(os.environ['BIGLAKE_CLIENT_EMAIL'])}, "
                f"PRIVATE_KEY {lit(os.environ['BIGLAKE_PRIVATE_KEY'])}, "
                f"PRIVATE_KEY_ID {lit(os.environ.get('BIGLAKE_PRIVATE_KEY_ID', ''))}, "
                f"EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {lit(project)}}}")
    else:
        adc_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS",
                                  os.path.expanduser("~/.config/gcloud/application_default_credentials.json"))
        adc = json.loads(Path(adc_path).read_text())
        if adc.get("type") == "service_account":
            body = (f"TYPE ICEBERG, PROVIDER google, CLIENT_EMAIL {lit(adc['client_email'])}, "
                    f"PRIVATE_KEY {lit(adc['private_key'])}, "
                    f"PRIVATE_KEY_ID {lit(adc.get('private_key_id', ''))}, "
                    f"EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {lit(project)}}}")
        else:
            body = ("TYPE ICEBERG, OAUTH2_GRANT_TYPE 'refresh_token', "
                    "OAUTH2_SERVER_URI 'https://oauth2.googleapis.com/token', "
                    f"CLIENT_ID {lit(adc['client_id'])}, CLIENT_SECRET {lit(adc['client_secret'])}, "
                    f"REFRESH_TOKEN {lit(adc['refresh_token'])}, "
                    f"EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {lit(project)}}}")
    return {
        "ICEBERG_BOOTSTRAP": f"CREATE OR REPLACE PERSISTENT SECRET iceberg_ci_catalog ({body});",
        "ICEBERG_SERVER_OPTIONS": (
            f"warehouse 'bl://projects/{project}/catalogs/{catalog}', "
            "uri 'https://biglake.googleapis.com/iceberg/v1/restcatalog', secret 'iceberg_ci_catalog'"),
    }


class IcebergRestFixture:
    def __init__(self, prefix="sdbstress"):
        self.services = ServiceSet(prefix=prefix)
        self.env = {}

    def start(self, timeout=90.0):
        keys = ("MINIO_HOST", "MINIO_PORT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
                "MINIO_BUCKET", "ICEBERG_REST_URL", "ICEBERG_WAREHOUSE")
        if os.environ.get("ICEBERG_REST_URL"):
            self.env = {k: os.environ[k] for k in keys if k in os.environ}
            return self.env
        env = self.services.start(["iceberg"])
        self.env = {k: env[k] for k in keys}
        return self.env

    def stop(self):
        self.services.stop()
