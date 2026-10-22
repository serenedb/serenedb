import concurrent.futures
import json
import os
import re
import subprocess
import sys

PAGE = 4096
PRESENT = re.compile(rb"[\x80-\xff]")


def index_mappings(pid, idx_dir):
    maps = []
    with open(f"/proc/{pid}/maps") as f:
        for line in f:
            parts = line.split(maxsplit=5)
            if len(parts) < 6:
                continue
            path = parts[5].strip()
            if not path.startswith(idx_dir):
                continue
            start, end = (int(x, 16) for x in parts[0].split("-"))
            maps.append((start, end, int(parts[2], 16), os.path.basename(path)))
    return maps


def present_pages(pid, maps):
    res = {}
    with open(f"/proc/{pid}/pagemap", "rb") as f:
        for start, end, offset, name in maps:
            n = (end - start) // PAGE
            f.seek((start // PAGE) * 8)
            high = f.read(n * 8)[7::8]
            first = offset // PAGE
            res.setdefault(name, set()).update(first + m.start() for m in PRESENT.finditer(high))
    return res


def runs(pages):
    out = []
    for p in sorted(pages):
        if out and out[-1][0] + out[-1][1] == p:
            out[-1][1] += 1
        else:
            out.append([p, 1])
    return out


def run_one(binary, idx_dir, qid, cmd, query, tags):
    proc = subprocess.Popen([binary, idx_dir], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, bufsize=1)
    after = None
    try:
        proc.stdin.write("noop\n")
        proc.stdin.flush()
        ready = proc.stdout.readline().strip()
        maps = index_mappings(proc.pid, idx_dir)
        before = present_pages(proc.pid, maps)
        proc.stdin.write(f"{cmd}\t{query}\n")
        proc.stdin.flush()
        result = proc.stdout.readline().strip()
        if result:
            maps_after = index_mappings(proc.pid, idx_dir)
            after = present_pages(proc.pid, maps_after)
    finally:
        try:
            proc.stdin.close()
        except BrokenPipeError:
            pass
        stderr = proc.stderr.read()
        proc.wait(timeout=600)
    if after is None:
        return json.dumps({"qid": qid, "cmd": cmd, "query": query, "tags": tags, "result": "CRASH",
                           "returncode": proc.returncode, "stderr": stderr[-4000:], "files": {},
                           "open_pages": {}, "maps": []})
    files = {}
    for name, pages in after.items():
        touched = pages - before.get(name, set())
        if touched:
            files[name] = runs(touched)
    record = {
        "qid": qid, "cmd": cmd, "query": query, "tags": tags, "ready": ready, "result": result,
        "files": files,
        "open_pages": {name: len(pages) for name, pages in before.items()},
        "maps": [[s, e, o, n] for s, e, o, n in maps_after],
    }
    return json.dumps(record)


def main():
    binary, idx_dir, queries_path, out_path, cmds, workers = sys.argv[1:7]
    idx_dir = os.path.realpath(idx_dir)
    limit = int(sys.argv[7]) if len(sys.argv) > 7 else None
    jobs = []
    with open(queries_path) as f:
        for qid, line in enumerate(f):
            if limit is not None and qid >= limit:
                break
            q = json.loads(line)
            for cmd in cmds.split(","):
                jobs.append((qid, cmd, q["query"], q["tags"]))
    with open(out_path, "w") as out, concurrent.futures.ThreadPoolExecutor(int(workers)) as pool:
        futures = [pool.submit(run_one, binary, idx_dir, *job) for job in jobs]
        for done, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            out.write(fut.result() + "\n")
            if done % 100 == 0:
                print(f"{done}/{len(jobs)}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
