import fnmatch
from dataclasses import dataclass
from pathlib import Path

TIMEOUT_MULT = 5.0
UNKNOWN_MULT = 10.0
BELOW_RESOLUTION = 0.0005


@dataclass(frozen=True)
class Skip:
    pattern: str
    kind: str
    reason: str


def load_skips(root, config_name):
    path = Path(root) / "scripts" / "ci" / "skips" / f"{config_name}.txt"
    skips = []
    if not path.is_file():
        return skips
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        pattern, kind, reason = (line.split("\t") + ["", ""])[:3]
        skips.append(Skip(pattern.strip(), kind.strip(), reason.strip()))
    return skips


def skipped(skips, unit_id):
    for s in skips:
        if fnmatch.fnmatchcase(unit_id, s.pattern):
            return s
    return None


def plan(ctx, unit_id, default_s):
    expected = ctx.timing.expect(unit_id)
    if expected is None:
        expected = default_s * ctx.config.slowdown
    mult = TIMEOUT_MULT if ctx.timing.measured(unit_id) else UNKNOWN_MULT
    return expected, max(ctx.timeout_floor, mult * expected)
