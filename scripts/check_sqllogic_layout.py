#!/usr/bin/env python3
import os
import sys

ROOT = "tests/sqllogic"


def main() -> int:
    failed = False
    for protocol in sorted(os.listdir(f"{ROOT}/any")):
        if protocol == "any" or not os.path.isdir(f"{ROOT}/any/{protocol}"):
            continue
        for link, target in ((f"{ROOT}/{protocol}/any", f"../any/{protocol}/"),
                             (f"{ROOT}/sdb/{protocol}/any", f"../../any/{protocol}/")):
            if not os.path.islink(link) or os.readlink(link) != target:
                print(f"{link} must be a symlink to {target}: the {protocol} tests every "
                      "server must pass run there", file=sys.stderr)
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
