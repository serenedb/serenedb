#!/usr/bin/env python3
"""Persisted per-test timing cache for slowest-first scheduling.

  order <cache>            : read test paths on stdin, print slowest-first
  merge <cache> <timings>  : fold a `path\\tms` timings file into <cache>

Unknown (uncached) tests are emitted first in input order -- they may be new
and heavy, so they start before the known-slow ones, like gtest-parallel.
"""
import fcntl
import os
import sys


def load(cache):
    times = {}
    try:
        with open(cache) as f:
            for line in f:
                path, _, ms = line.rstrip("\n").partition("\t")
                if path and ms:
                    try:
                        times[path] = int(ms)
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return times


def order(cache):
    times = load(cache)
    paths = [line.strip() for line in sys.stdin if line.strip()]

    def key(item):
        idx, path = item
        ms = times.get(path)
        if ms is None:
            return (0, 0, idx)
        return (1, -ms, idx)

    for _, path in sorted(enumerate(paths), key=key):
        print(path)


def merge(cache, timings):
    new = load(timings)
    if not new:
        return
    os.makedirs(os.path.dirname(cache) or ".", exist_ok=True)
    with open(cache + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        merged = load(cache)
        merged.update(new)
        tmp = cache + ".tmp"
        with open(tmp, "w") as out:
            for path in sorted(merged):
                out.write(f"{path}\t{merged[path]}\n")
        os.replace(tmp, cache)


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "order":
        order(sys.argv[2])
    elif len(sys.argv) >= 4 and sys.argv[1] == "merge":
        merge(sys.argv[2], sys.argv[3])
    else:
        sys.stderr.write(
            "usage: timing_cache.py order <cache> | merge <cache> <timings>\n"
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
