"""A crash during concurrent writes replays the inverted indexes equal to their tables."""

from __future__ import annotations

from pathlib import Path

import pytest
from inverted_dml import (  # type: ignore[import-untyped]
    a_crash_during_concurrent_writes_replays_to_an_equal_index,
    pytestmark,
)


@pytest.mark.parametrize("seed", [1, 2])
def test_a_crash_during_concurrent_writes_replays_to_an_equal_index(
    tmp_path: Path, seed: int
) -> None:
    a_crash_during_concurrent_writes_replays_to_an_equal_index(tmp_path, seed)
