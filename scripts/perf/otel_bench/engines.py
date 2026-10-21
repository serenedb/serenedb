import base64
import json
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request

import prepare

CLICKHOUSE_IMAGE = os.environ.get("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.9")
OTELCOL_IMAGE = os.environ.get("OTELCOL_IMAGE",
                               "otel/opentelemetry-collector-contrib:0.162.0")
ELASTICSEARCH_IMAGE = os.environ.get(
    "ELASTICSEARCH_IMAGE", "docker.elastic.co/elasticsearch/elasticsearch:9.5.5")
VICTORIA_LOGS_IMAGE = os.environ.get("VICTORIA_LOGS_IMAGE",
                                     "victoriametrics/victoria-logs:v1.53.0")
VICTORIA_TRACES_IMAGE = os.environ.get("VICTORIA_TRACES_IMAGE",
                                       "victoriametrics/victoria-traces:v0.12.0")
VICTORIA_METRICS_IMAGE = os.environ.get("VICTORIA_METRICS_IMAGE",
                                        "victoriametrics/victoria-metrics:v1.153.0")
QUICKWIT_IMAGE = os.environ.get("QUICKWIT_IMAGE", "quickwit/quickwit:0.9.1")
DU_IMAGE = CLICKHOUSE_IMAGE

SERVER_SLICE = os.environ.get("OTEL_BENCH_SLICE", "")
SERVER_CPUS = os.environ.get("OTEL_BENCH_SERVER_CPUS", "")


def server_command(command):
    if SERVER_SLICE:
        return ["systemd-run", "--quiet", "--scope", f"--slice={SERVER_SLICE}", "--",
                *command]
    if SERVER_CPUS:
        return ["taskset", "-c", SERVER_CPUS, *command]
    return list(command)

METRIC_TABLES = ["otel_metrics_gauge", "otel_metrics_sum", "otel_metrics_histogram",
                 "otel_metrics_exponential_histogram", "otel_metrics_summary"]
TABLES = {"logs": ["otel_logs"], "traces": ["otel_traces"], "metrics": METRIC_TABLES}

CLOCK_TICK = os.sysconf("SC_CLK_TCK")


class EngineError(Exception):
    pass


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def port_in_use(port):
    with socket.socket() as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def http(method, url, body=None, headers=None, timeout=30):
    request = urllib.request.Request(url, data=body, method=method,
                                     headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def wait_until(check, what, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
            last = error
        time.sleep(0.5)
    raise EngineError(f"{what} not ready after {timeout}s: {last}")


def peak_rss(pids):
    total = 0
    for pid in pids:
        try:
            for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmHWM:"):
                    total += int(line.split()[1]) * 1024
        except OSError:
            pass
    return total


def du(path):
    out = subprocess.run(["du", "-sk", path], capture_output=True, text=True).stdout
    return int(out.split()[0]) * 1024 if out else 0


def docker(*args, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


class Container:
    def __init__(self, name, image, args=(), env=None, data=None, mounts=(),
                 ulimits=()):
        self.name = name
        self.volume = f"{name}-data" if data else None
        command = ["run", "-d", "--name", name, "--network", "host"]
        if SERVER_SLICE:
            command += ["--cgroup-parent", SERVER_SLICE]
        elif SERVER_CPUS:
            command += ["--cpuset-cpus", SERVER_CPUS]
        for key, value in (env or {}).items():
            command += ["-e", f"{key}={value}"]
        if data:
            docker("volume", "create", self.volume)
            command += ["-v", f"{self.volume}:{data}"]
        for source, target in mounts:
            command += ["-v", f"{source}:{target}:ro"]
        for ulimit in ulimits:
            command += ["--ulimit", ulimit]
        docker("rm", "-f", name, check=False)
        try:
            docker(*command, image, *args)
        except subprocess.CalledProcessError as error:
            raise EngineError(f"docker run {image}: {error.stderr.strip()}")
        pid = docker("inspect", "-f", "{{.State.Pid}}", name).stdout.strip()
        cgroup = pathlib.Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[1]
        self.cgroup = pathlib.Path("/sys/fs/cgroup") / cgroup.lstrip("/")

    def cpu_seconds(self):
        for line in (self.cgroup / "cpu.stat").read_text().splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1]) / 1e6
        return 0.0

    def pids(self):
        try:
            return (self.cgroup / "cgroup.procs").read_text().split()
        except OSError:
            return []

    def disk_bytes(self):
        docker("stop", "-t", "300", self.name, check=False)
        out = docker("run", "--rm", "-v", f"{self.volume}:/data", "--entrypoint", "du",
                     DU_IMAGE, "-sk", "/data", check=False).stdout
        return int(out.split()[0]) * 1024 if out else 0

    def logs(self, lines=30):
        result = docker("logs", "--tail", str(lines), self.name, check=False)
        return (result.stdout + result.stderr).strip()

    def stop(self):
        docker("rm", "-f", self.name, check=False)
        if self.volume:
            docker("volume", "rm", "-f", self.volume, check=False)


class Engine:
    name = ""
    signals = ("logs", "traces", "metrics")
    encodings = ("pb", "json")
    ports = ()
    settle = 30
    containers = ()

    def __init__(self, prefix):
        self.prefix = prefix

    def supports(self, signal, encoding):
        return signal in self.signals and encoding in self.encodings

    def check_ports(self):
        busy = [port for port in self.ports if port_in_use(port)]
        if busy:
            raise EngineError(f"{self.name}: ports {busy} are already in use")

    def headers(self):
        return {}

    def retryable(self, status, body):
        return status in (429, 502, 503, 504)

    def expected(self, signal, manifest):
        return manifest["records"]

    def flush(self, signal):
        pass

    def cpu_seconds(self):
        return sum(c.cpu_seconds() for c in self.containers)

    def peak_rss(self):
        return peak_rss([pid for c in self.containers for pid in c.pids()])

    def disk_bytes(self):
        return sum(c.disk_bytes() for c in self.containers if c.volume)

    def logs(self):
        return "\n".join(f"--- {c.name}\n{c.logs()}" for c in self.containers)

    def stop(self):
        for container in reversed(self.containers):
            container.stop()
        self.containers = ()


class SereneDB(Engine):
    name = "serenedb"

    def __init__(self, prefix, binary):
        super().__init__(prefix)
        self.binary = binary
        self.proc = None

    def check_ports(self):
        if not os.access(self.binary, os.X_OK):
            raise EngineError(f"serenedb: {self.binary} is not an executable")

    def psql(self, sql):
        return subprocess.run(
            ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
             "-p", str(self.pg_port), "-U", "postgres", "-d", "postgres", "-c", sql],
            capture_output=True, text=True, check=True).stdout.strip()

    def start(self, signal):
        self.datadir = tempfile.mkdtemp(prefix="otel-bench-serenedb-")
        self.pg_port, self.http_port = free_port(), free_port()
        listen = (f"postgres://127.0.0.1:{self.pg_port},"
                  f"http://127.0.0.1:{self.http_port}?api=otel")
        self.log = open(os.path.join(self.datadir, "serened.log"), "w")
        self.proc = subprocess.Popen(
            server_command([self.binary, self.datadir, "--listen", listen]),
            stdout=self.log, stderr=subprocess.STDOUT)

        def ready():
            if self.proc.poll() is not None:
                raise EngineError(f"serened exited with {self.proc.returncode}")
            self.psql("SELECT 1")
            return True

        wait_until(ready, "serened")

    def endpoint(self, signal):
        return "127.0.0.1", self.http_port, f"/v1/{signal}"

    def headers(self):
        return {"Authorization": "Basic " + base64.b64encode(b"postgres:").decode()}

    def count(self, signal):
        return int(self.psql("SELECT " + " + ".join(f"(SELECT count(*) FROM {t})"
                                                    for t in TABLES[signal])))

    def flush(self, signal):
        for table in TABLES[signal]:
            self.psql(f"VACUUM (REFRESH_TABLE) {table}")

    def cpu_seconds(self):
        fields = pathlib.Path(f"/proc/{self.proc.pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / CLOCK_TICK

    def peak_rss(self):
        return peak_rss([self.proc.pid])

    def disk_bytes(self):
        return du(self.datadir)

    def logs(self):
        try:
            return pathlib.Path(self.datadir, "serened.log").read_text()[-4000:]
        except OSError:
            return ""

    def stop(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()
            self.log.close()
            shutil.rmtree(self.datadir, ignore_errors=True)
            self.proc = None


OTELCOL_CONFIG = """\
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318
        max_request_body_size: 268435456
exporters:
  clickhouse:
    endpoint: tcp://127.0.0.1:9000
    database: otel
    create_schema: true
    ttl: 0
    timeout: 120s
    sending_queue:
      enabled: false
    retry_on_failure:
      enabled: false
service:
  telemetry:
    metrics:
      level: none
  pipelines:
    logs: {receivers: [otlp], exporters: [clickhouse]}
    traces: {receivers: [otlp], exporters: [clickhouse]}
    metrics: {receivers: [otlp], exporters: [clickhouse]}
"""


class ClickHouse(Engine):
    name = "clickhouse"
    ports = (4318, 8123, 9000, 9004, 9005, 9009)

    def query(self, sql):
        return http("POST", "http://127.0.0.1:8123/", sql.encode()).decode().strip()

    def start(self, signal):
        server = Container(f"{self.prefix}-clickhouse", CLICKHOUSE_IMAGE,
                           env={"CLICKHOUSE_SKIP_USER_SETUP": "1"},
                           data="/var/lib/clickhouse", ulimits=["nofile=262144:262144"])
        self.containers = (server,)
        wait_until(lambda: http("GET", "http://127.0.0.1:8123/ping") == b"Ok.\n",
                   "clickhouse")
        self.config_dir = tempfile.mkdtemp(prefix="otel-bench-otelcol-")
        os.chmod(self.config_dir, 0o755)
        config = pathlib.Path(self.config_dir, "config.yaml")
        config.write_text(OTELCOL_CONFIG)
        config.chmod(0o644)
        collector = Container(f"{self.prefix}-otelcol", OTELCOL_IMAGE,
                              args=["--config=/etc/otelcol/config.yaml"],
                              mounts=[(config, "/etc/otelcol/config.yaml")])
        self.containers = (server, collector)
        first = TABLES[signal][0]
        wait_until(lambda: self.query(f"EXISTS TABLE otel.{first}") == "1"
                   and port_in_use(4318), "otel collector")

    def endpoint(self, signal):
        return "127.0.0.1", 4318, f"/v1/{signal}"

    def count(self, signal):
        tables = " UNION ALL ".join(f"SELECT count() AS c FROM otel.{t}"
                                    for t in TABLES[signal])
        return int(self.query(f"SELECT sum(c) FROM ({tables})"))

    def flush(self, signal):
        self.query("SYSTEM FLUSH ASYNC INSERT QUEUE")

    def disk_bytes(self):
        return int(self.query("SELECT sum(bytes_on_disk) FROM system.parts "
                              "WHERE database = 'otel' AND active"))

    def stop(self):
        super().stop()
        if getattr(self, "config_dir", None):
            shutil.rmtree(self.config_dir, ignore_errors=True)
            self.config_dir = None


class Elasticsearch(Engine):
    name = "elasticsearch"
    encodings = ("pb",)
    ports = (9200, 9300)
    settle = 15
    streams = {"logs": "logs-*.otel-*", "traces": "traces-*.otel-*",
               "metrics": "metrics-*.otel-*"}

    def request(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        return json.loads(http(method, f"http://127.0.0.1:9200{path}", data,
                               {"Content-Type": "application/json"}))

    def start(self, signal):
        self.containers = (Container(
            f"{self.prefix}-elasticsearch", ELASTICSEARCH_IMAGE,
            env={"discovery.type": "single-node", "xpack.security.enabled": "false",
                 "cluster.routing.allocation.disk.threshold_enabled": "false",
                 "http.max_protobuf_content_length": "256mb",
                 "http.max_protobuf_expanded_content_length": "1gb"},
            data="/usr/share/elasticsearch/data",
            ulimits=["nofile=65535:65535", "memlock=-1:-1"]),)
        wait_until(lambda: self.request(
            "GET", "/_cluster/health?wait_for_status=yellow&timeout=1s")["status"]
            in ("green", "yellow"), "elasticsearch")
        wait_until(lambda: self.request("GET", f"/_index_template/{signal}-otel@template")
                   ["index_templates"], f"elasticsearch {signal}-otel@template")
        if signal == "metrics":
            self.request("PUT", "/_component_template/metrics-otel@custom",
                         {"template": {"settings": {"index.look_back_time": "7d"}}})

    def endpoint(self, signal):
        return "127.0.0.1", 9200, f"/_otlp/v1/{signal}"

    def expected(self, signal, manifest):
        return None if signal == "metrics" else manifest["records"]

    def count(self, signal):
        path = (f"/{self.streams[signal]}/_count"
                "?ignore_unavailable=true&allow_no_indices=true")
        return self.request("GET", path)["count"]

    def flush(self, signal):
        self.request("POST", "/_refresh")
        self.request("POST", "/_flush")


class Victoria(Engine):
    name = "victoria"
    encodings = ("pb",)
    products = {
        "logs": ("victoria-logs", VICTORIA_LOGS_IMAGE, 9428,
                 "/insert/opentelemetry/v1/logs"),
        "traces": ("victoria-traces", VICTORIA_TRACES_IMAGE, 10428,
                   "/insert/opentelemetry/v1/traces"),
        "metrics": ("victoria-metrics", VICTORIA_METRICS_IMAGE, 8428,
                    "/opentelemetry/v1/metrics"),
    }
    ports = (9428, 10428, 8428)

    def start(self, signal):
        product, image, port, _ = self.products[signal]
        self.port = port
        self.containers = (Container(
            f"{self.prefix}-{product}", image,
            args=["-storageDataPath=/storage", f"-httpListenAddr=127.0.0.1:{port}"],
            data="/storage"),)
        wait_until(lambda: http("GET", f"http://127.0.0.1:{port}/health") == b"OK",
                   product)

    def endpoint(self, signal):
        return "127.0.0.1", self.port, self.products[signal][3]

    def expected(self, signal, manifest):
        return manifest["samples"] if signal == "metrics" else manifest["records"]

    def count(self, signal):
        base = f"http://127.0.0.1:{self.port}"
        if signal == "metrics":
            for line in http("GET", f"{base}/metrics").decode().splitlines():
                if line.startswith("vm_rows_added_to_storage_total"):
                    return int(float(line.split()[-1]))
            return 0
        query = urllib.parse.urlencode({"query": "* | count() rows"})
        out = http("GET", f"{base}/select/logsql/query?{query}").decode().strip()
        return int(json.loads(out)["rows"]) if out else 0

    def flush(self, signal):
        http("POST", f"http://127.0.0.1:{self.port}/internal/force_flush")


QUICKWIT_CONFIG = """\
version: 0.8
indexer:
  enable_otlp_endpoint: true
grpc:
  max_message_size: 256MiB
ingest_api:
  content_length_limit: 256MiB
  shard_throughput_limit: 20MiB
  shard_burst_limit: 4GiB
  shard_scale_up_factor: 10
  max_queue_memory_usage: 32GiB
  max_queue_disk_usage: 64GiB
"""


class Quickwit(Engine):
    name = "quickwit"
    signals = ("logs", "traces")
    encodings = ("pb",)
    ports = (7280, 7281)
    api = "http://127.0.0.1:7280/api/v1"

    def start(self, signal):
        self.config_dir = tempfile.mkdtemp(prefix="otel-bench-quickwit-")
        os.chmod(self.config_dir, 0o755)
        config = pathlib.Path(self.config_dir, "quickwit.yaml")
        config.write_text(QUICKWIT_CONFIG)
        config.chmod(0o644)
        self.containers = (Container(
            f"{self.prefix}-quickwit", QUICKWIT_IMAGE, args=["run"],
            env={"QW_DISABLE_TELEMETRY": "1",
                 "QW_INGEST_ROUTER_BUFFER_SIZE_BYTES": str(16 * 2 ** 30)},
            data="/quickwit/qwdata",
            mounts=[(config, "/quickwit/config/quickwit.yaml")]),)
        wait_until(lambda: http("GET", "http://127.0.0.1:7280/health/readyz") is not None,
                   "quickwit")
        for index in self.indexes(signal):
            index_config = json.loads(http("GET", f"{self.api}/indexes/{index}"))["index_config"]
            index_config["ingest_settings"]["min_shards"] = os.cpu_count()
            index_config["indexing_settings"]["commit_timeout_secs"] = 1
            http("PUT", f"{self.api}/indexes/{index}", json.dumps(index_config).encode(),
                 {"Content-Type": "application/json"})
        body = warm_up_request(signal)
        host, port, path = self.endpoint(signal)
        wait_until(lambda: http("POST", f"http://{host}:{port}{path}", body,
                                {"Content-Type": "application/x-protobuf"}) is not None,
                   "quickwit ingest")
        wait_until(lambda: self.count(signal) > 0, "quickwit warm-up record")

    def endpoint(self, signal):
        return "127.0.0.1", 7280, f"/api/v1/otlp/v1/{signal}"

    def retryable(self, status, body):
        return super().retryable(status, body) or (
            status == 500 and (b"unavailable" in body or b"rate limited" in body))

    def indexes(self, signal):
        return [i["index_config"]["index_id"]
                for i in json.loads(http("GET", f"{self.api}/indexes"))
                if i["index_config"]["index_id"].startswith(f"otel-{signal}")]

    def count(self, signal):
        return sum(json.loads(http("GET", f"{self.api}/{index}/search?query=*&max_hits=0"))
                   ["num_hits"] for index in self.indexes(signal))

    def stop(self):
        super().stop()
        if getattr(self, "config_dir", None):
            shutil.rmtree(self.config_dir, ignore_errors=True)
            self.config_dir = None


def warm_up_request(signal):
    now = str(time.time_ns())
    resource = {"attributes": [{"key": "service.name",
                                "value": {"stringValue": "otel-bench-warm-up"}}]}
    if signal == "logs":
        request = {"resourceLogs": [{"resource": resource, "scopeLogs": [{"logRecords": [
            {"timeUnixNano": now, "body": {"stringValue": "warm-up"}}]}]}]}
    else:
        request = {"resourceSpans": [{"resource": resource, "scopeSpans": [{"spans": [
            {"traceId": "0" * 31 + "1", "spanId": "0" * 15 + "1", "name": "warm-up",
             "kind": 1, "startTimeUnixNano": now, "endTimeUnixNano": now}]}]}]}
    return prepare.encode(signal, request)


ENGINES = {engine.name: engine
           for engine in (SereneDB, ClickHouse, Elasticsearch, Victoria, Quickwit)}


def make(name, binary, prefix):
    if name not in ENGINES:
        raise EngineError(f"unknown engine {name!r}; known: {', '.join(ENGINES)}")
    if name == SereneDB.name:
        return SereneDB(prefix, binary)
    return ENGINES[name](prefix)
