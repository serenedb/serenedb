"""Pytest config that pulls the shared spec loader onto sys.path."""

import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parent.parent / "harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

# pytest-asyncio mode: each async test handles its own event loop via
# `@pytest.mark.asyncio`; no autouse needed.
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "exclusive: arms a server-wide failure point, so tests/drivers/run.sh "
        "runs it alone once every driver suite has finished",
    )


def pytest_collection_modifyitems(config, items):
    pass
