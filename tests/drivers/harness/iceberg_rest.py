import os
import random
import string
import subprocess
import time
import urllib.request

from serened import free_port

ENV_KEYS = ("MINIO_HOST", "MINIO_PORT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
            "MINIO_BUCKET", "ICEBERG_REST_URL", "ICEBERG_WAREHOUSE")
MINIO_IMAGE = "pgsty/minio:latest"
REST_IMAGE = "apache/iceberg-rest-fixture:1.10.1"


def _docker(*args, check=True, capture=False):
    return subprocess.run(["docker", *args], check=check, text=True,
                          stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                          stderr=subprocess.PIPE if capture else subprocess.DEVNULL)


class IcebergRestFixture:
    def __init__(self, prefix="sdbstress"):
        tag = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(4))
        self.name = f"{tag}-{prefix}-{os.getpid()}"
        self.network = None
        self.minio = None
        self.rest = None
        self.env = {}

    def start(self, timeout=90.0):
        if os.environ.get("ICEBERG_REST_URL"):
            self.env = {k: os.environ[k] for k in ENV_KEYS if k in os.environ}
            return self.env
        access, secret, bucket = "minioadmin", "minioadmin", "testbucket"
        self.network = f"{self.name}-net"
        _docker("network", "create", self.network)
        minio_port = free_port()
        self.minio = f"{self.name}-minio"
        _docker("run", "-d", "--name", self.minio, "--network", self.network,
                "--ulimit", "nofile=65536:65536",
                "-p", f"{minio_port}:9000",
                "-e", f"MINIO_ROOT_USER={access}", "-e", f"MINIO_ROOT_PASSWORD={secret}",
                MINIO_IMAGE, "server", "/data")
        deadline = time.time() + timeout
        while True:
            if _docker("exec", self.minio, "mc", "alias", "set", "local",
                       "http://127.0.0.1:9000", access, secret, check=False).returncode == 0:
                break
            if time.time() > deadline:
                raise RuntimeError("MinIO did not come up")
            time.sleep(1)
        _docker("exec", self.minio, "mc", "mb", f"local/{bucket}")
        rest_port = free_port()
        self.rest = f"{self.name}-iceberg-rest"
        _docker("run", "-d", "--name", self.rest, "--network", self.network,
                "--ulimit", "nofile=65536:65536",
                "-p", f"{rest_port}:8181",
                "-e", f"AWS_ACCESS_KEY_ID={access}", "-e", f"AWS_SECRET_ACCESS_KEY={secret}",
                "-e", "AWS_REGION=us-east-1",
                "-e", f"CATALOG_WAREHOUSE=s3://{bucket}/warehouse/",
                "-e", "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
                "-e", f"CATALOG_S3_ENDPOINT=http://{self.minio}:9000",
                "-e", "CATALOG_S3_PATH__STYLE__ACCESS=true",
                REST_IMAGE)
        url = f"http://localhost:{rest_port}"
        deadline = time.time() + timeout
        while True:
            try:
                with urllib.request.urlopen(f"{url}/v1/config?warehouse=demo", timeout=3) as r:
                    if r.status == 200:
                        break
            except Exception:
                pass
            if time.time() > deadline:
                raise RuntimeError("iceberg-rest did not answer /v1/config")
            time.sleep(1)
        self.env = {
            "MINIO_HOST": "localhost", "MINIO_PORT": str(minio_port),
            "MINIO_ACCESS_KEY": access, "MINIO_SECRET_KEY": secret, "MINIO_BUCKET": bucket,
            "ICEBERG_REST_URL": url, "ICEBERG_WAREHOUSE": "demo",
        }
        return self.env

    def stop(self):
        for name in (self.rest, self.minio):
            if name:
                _docker("rm", "-fv", name, check=False)
        if self.network:
            _docker("network", "rm", self.network, check=False)
        self.rest = self.minio = self.network = None


def main(argv):
    import argparse
    import json
    ap = argparse.ArgumentParser(prog="iceberg_rest.py")
    ap.add_argument("action", choices=("start", "stop"))
    ap.add_argument("--state", required=True)
    args = ap.parse_args(argv)
    if args.action == "start":
        fixture = IcebergRestFixture(prefix="sdbci")
        env = fixture.start()
        with open(args.state, "w") as fh:
            json.dump({"minio": fixture.minio, "rest": fixture.rest,
                       "network": fixture.network, "env": env}, fh)
        for key, value in env.items():
            print(f"export {key}={value}")
        return 0
    with open(args.state) as fh:
        state = json.load(fh)
    fixture = IcebergRestFixture()
    fixture.minio, fixture.rest, fixture.network = state["minio"], state["rest"], state["network"]
    fixture.stop()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv[1:]))
