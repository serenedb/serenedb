"""A crash during concurrent writes replays search tables equal to their models."""

from __future__ import annotations

from pathlib import Path

import pytest
from search_table_dml import (  # type: ignore[import-untyped]
    a_crash_during_concurrent_writes_replays_to_an_equal_search_table,
    pytestmark,
)


@pytest.mark.parametrize("seed", [4, 5, 6])
def test_a_crash_during_concurrent_writes_replays_to_an_equal_search_table(
    tmp_path: Path, seed: int
) -> None:
    a_crash_during_concurrent_writes_replays_to_an_equal_search_table(tmp_path, seed)
