from __future__ import annotations

import base64
import http.client
import os
import time
from pathlib import Path

import pytest
from serened import Serened, free_port

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)

pytestmark = pytest.mark.skipif(
    not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)),
    reason=f"serened binary not found or not executable: {SERENED_BIN}",
)

AUTH = "Basic " + base64.b64encode(b"postgres:").decode()


@pytest.fixture()
def http_port(tmp_path):
    port = free_port()
    http_port = free_port()
    listen = f"postgres://127.0.0.1:{port},http://127.0.0.1:{http_port}?api=test"
    with Serened(SERENED_BIN, port=port, datadir_root=str(tmp_path),
                 log_path=str(tmp_path / "serened.log"),
                 extra_args=[f"--listen={listen}", "--http_body_timeout=1s",
                             "--idle_session_timeout=1s"]) as server:
        server.start()
        yield http_port


def test_a_response_read_slower_than_the_timeouts_arrives_whole(http_port):
    size = 64 << 20
    conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=120)
    try:
        conn.request("GET", f"/_test/bytes?n={size}",
                     headers={"Authorization": AUTH})
        response = conn.getresponse()
        assert response.status == 200
        time.sleep(3)
        assert len(response.read()) == size
        conn.request("GET", "/_test/ping", headers={"Authorization": AUTH})
        response = conn.getresponse()
        assert response.status == 200
        assert response.read() == b"pong"
    finally:
        conn.close()
