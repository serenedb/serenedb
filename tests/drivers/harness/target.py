from __future__ import annotations

import functools
import os
from pathlib import Path

import psycopg
from spec_loader import conn_kwargs


@functools.cache
def is_serenedb() -> bool:
    with psycopg.connect(**conn_kwargs()) as conn:
        return "SereneDB" in conn.execute("SELECT version()").fetchone()[0]


def check_golden(path: Path, text: str) -> None:
    twin = path.with_name(f"{path.stem}.serenedb{path.suffix}")
    if os.environ.get("SDB_DRV_UPDATE_GOLDENS") == "1":
        if not is_serenedb():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        elif text == path.read_text():
            twin.unlink(missing_ok=True)
        else:
            twin.write_text(text)
        return
    expected = twin if is_serenedb() and twin.exists() else path
    assert text == expected.read_text()
