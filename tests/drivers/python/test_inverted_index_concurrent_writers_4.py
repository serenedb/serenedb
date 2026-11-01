"""Concurrent writers keep the inverted indexes of regular tables equal to the tables."""

from __future__ import annotations

from pathlib import Path

import pytest
from inverted_dml import (  # type: ignore[import-untyped]
    WRITER_CASES,
    concurrent_writers_keep_the_index_equal_to_the_table,
    pytestmark,
)


@pytest.mark.parametrize(("options", "writers", "flags"), WRITER_CASES[5:])
def test_concurrent_writers_keep_the_index_equal_to_the_table(
    tmp_path: Path, options: str, writers: int, flags: str
) -> None:
    concurrent_writers_keep_the_index_equal_to_the_table(tmp_path, options, writers, flags)
