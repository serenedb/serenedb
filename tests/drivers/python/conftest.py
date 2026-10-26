"""Pytest config that pulls the shared spec loader onto sys.path."""

import os
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parent.parent / "harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

# pytest-asyncio mode: each async test handles its own event loop via
# `@pytest.mark.asyncio`; no autouse needed.
import pytest
from serened import Serened

SERENED_BIN = os.environ.get(
    "SDB_DRV_SERENED_BIN",
    str(Path(__file__).resolve().parents[3] / "build" / "bin" / "serened"),
)


def _own_server(tmp_dir: Path):
    if not (Path(SERENED_BIN).is_file() and os.access(SERENED_BIN, os.X_OK)):
        pytest.skip(f"serened binary not found or not executable: {SERENED_BIN}")
    server = Serened(SERENED_BIN, datadir_root=str(tmp_dir),
                     log_path=str(tmp_dir / "serened.log"))
    try:
        server.start()
        with pytest.MonkeyPatch.context() as env:
            env.setenv("SDB_DRV_HOST", "127.0.0.1")
            env.setenv("SDB_DRV_PORT", str(server.port))
            yield server
    finally:
        server.stop()


@pytest.fixture()
def own_server(tmp_path):
    yield from _own_server(tmp_path)


@pytest.fixture(scope="module")
def own_server_module(tmp_path_factory):
    yield from _own_server(tmp_path_factory.mktemp("serened"))


def pytest_collection_modifyitems(config, items):
    pass
