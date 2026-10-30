"""Concurrent writers keep search tables equal to a regular table fed the same changes."""

from __future__ import annotations

from pathlib import Path

import pytest
from search_table_dml import (  # type: ignore[import-untyped]
    WRITER_CASES,
    concurrent_writers_keep_the_search_table_equal_to_its_model,
    pytestmark,
)


@pytest.mark.parametrize(("options", "writers", "flags"), WRITER_CASES[3:])
def test_concurrent_writers_keep_the_search_table_equal_to_its_model(
    tmp_path: Path, options: str, writers: int, flags: str
) -> None:
    concurrent_writers_keep_the_search_table_equal_to_its_model(tmp_path, options, writers,
                                                                flags)
