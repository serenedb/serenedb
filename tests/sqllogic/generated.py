import glob
import hashlib
import os
import re
import subprocess
import sys

PLACEHOLDER = "<recorded>"
PART = "<part>"
KEY_WIDTH = 16
NEW_ERRORS = "SDB_RECORD_NEW_ERRORS"


def blocks(text):
    out = []
    current = []
    for line in text.split("\n"):
        if line:
            current.append(line)
        elif current:
            out.append(current)
            current = []
    if current:
        out.append(current)
    return out


def join(parts):
    return "\n\n".join("\n".join(block) for block in parts) + "\n"


def spots(block):
    return [(i, j) for i, line in enumerate(block)
            for j, cell in enumerate(line.split("\t")) if cell == PLACEHOLDER]


def recorded(text):
    seen = {}
    out = []
    for block in blocks(text):
        if not spots(block):
            continue
        sql = "\n".join(block[1:block.index("----")])
        key = hashlib.sha256(sql.encode()).hexdigest()[:KEY_WIDTH]
        seen[key] = seen.get(key, 0) + 1
        out.append((key if seen[key] == 1 else f"{key}.{seen[key]}", sql))
    return out


def write(path, text):
    temp = os.path.join(os.path.dirname(path), f".generating.{os.getpid()}")
    with open(temp, "w") as f:
        f.write(text)
    os.replace(temp, path)


def parts(text):
    return re.split(rf"^{re.escape(PART)}\n\n", text, flags=re.M)


def part_path(target, k):
    return f"{target[:-len('.gen.test')]}.{k}.gen.test"


def written(target):
    stem = target[:-len(".gen.test")]
    found = [path for path in glob.glob(f"{glob.escape(stem)}.*.gen.test")
             if path[len(stem) + 1:-len(".gen.test")].isdigit()]
    return found + ([target] if os.path.exists(target) else [])


def assembled(text, target):
    pieces = parts(text)
    if len(pieces) == 1:
        with open(target) as f:
            return f.read()
    setup = len(blocks(pieces[0]))
    out = []
    for k in range(1, len(pieces)):
        with open(part_path(target, k)) as f:
            got = blocks(f.read())
        out += (got[:setup] if k == 1 else []) + [[PART]] + got[setup:]
    return join(out)


def escape(text):
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")


def unescape(text):
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m[1], m[1]),
                  text)


def load(path):
    try:
        with open(path) as f:
            lines = f.read().split("\n")
    except FileNotFoundError:
        return {}
    records = {}
    for line in lines:
        if not line:
            continue
        key, _, rest = line.partition("\t")
        if rest.startswith("error\t"):
            records[key] = ("error", unescape(rest[len("error\t"):]))
        else:
            records[key] = ("values", rest.split("\t"))
    return records


def fill(text, records):
    keys = iter(key for key, _ in recorded(text))
    out = []
    missing = 0
    for block in blocks(text):
        places = spots(block)
        if not places:
            out.append(block)
            continue
        found = records.get(next(keys))
        if not found:
            missing += 1
            out.append(block)
            continue
        kind, values = found
        if kind == "error":
            sql = block[1:block.index("----")]
            out.append(["query error", *sql, "----", *values.split("\n"), ""])
            continue
        filled = list(block)
        for (i, j), value in zip(places, values):
            cells = filled[i].split("\t")
            cells[j] = value
            filled[i] = "\t".join(cells)
        out.append(filled)
    return join(out), missing


def error_of(block):
    if block[0] == "query error":
        return "\n".join(block[block.index("----") + 1:])
    if block[0].startswith("query error "):
        return unescape(block[0][len("query error "):])
    return None


def record(text, overridden, target):
    want, got = blocks(text), blocks(overridden)
    if len(want) != len(got):
        sys.exit(f"{target} doesn't follow its generator")
    keys = iter(key for key, _ in recorded(text))
    records = {}
    for expected, actual in zip(want, got):
        places = spots(expected)
        if not places:
            continue
        key = next(keys)
        if expected[1] != actual[1]:
            sys.exit(f"{target} doesn't follow its generator at {expected[1]}")
        error = error_of(actual)
        if error is not None:
            records[key] = ("error", error)
            continue
        values = []
        for i, j in places:
            cells = actual[i].split("\t") if i < len(actual) else []
            value = cells[j] if j < len(cells) else ""
            if value == PLACEHOLDER:
                return None
            if not value:
                sys.exit(f"{target} can't record an empty value at "
                         f"{expected[1]}")
            values.append(value)
        if len(values) > 1 and values[0] == "error":
            sys.exit(f"{target} can't record 'error' as the first of "
                     f"several values at {expected[1]}")
        records[key] = ("values", values)
    return records


def generate(generator):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [os.path.dirname(os.path.abspath(__file__)),
                      env.get("PYTHONPATH")]))
    return subprocess.run([sys.executable, generator], check=True, env=env,
                          stdout=subprocess.PIPE, text=True).stdout


def show(generator, keys):
    records = load(generator[:-len(".py")] + ".recorded")
    for key, sql in recorded(generate(generator)):
        if keys and key not in keys:
            continue
        kind, value = records.get(key, ("missing", None))
        answer = ("not recorded" if kind == "missing" else
                  f"error: {value}" if kind == "error" else "\t".join(value))
        print(f"{key}\t{answer}\n{sql}\n")


def main(command, generator, target):
    text = generate(generator)
    path = generator[:-len(".py")] + ".recorded"
    if command == "write":
        filled, missing = fill(text, load(path))
        pieces = parts(filled)
        outputs = ({target: filled} if len(pieces) == 1 else
                   {part_path(target, k): pieces[0] + body
                    for k, body in enumerate(pieces[1:], 1)})
        for stale in set(written(target)) - set(outputs):
            os.remove(stale)
        for out, content in outputs.items():
            write(out, content)
        if missing:
            print(f"{target}: {missing} queries have no value in {path}, "
                  "record them with scripts/ci/test.py --override", file=sys.stderr)
        return
    if not any(spots(block) for block in blocks(text)):
        return
    records = record(text, assembled(text, target), target)
    if records is None:
        print(f"{target} wasn't overridden, {path} is unchanged",
              file=sys.stderr)
        return
    queries = dict(recorded(text))
    previous = load(path)
    fresh = [f"{key}\t{queries[key]}\n  {value}"
             for key, (kind, value) in records.items()
             if kind == "error" and previous.get(key) != (kind, value)]
    if fresh and os.environ.get(NEW_ERRORS) != "1":
        sys.exit(f"{target} has queries that fail where {path} recorded "
                 f"something else; check them and record with "
                 f"{NEW_ERRORS}=1:\n" + "\n".join(fresh))
    lines = []
    for key, (kind, value) in records.items():
        rest = (f"error\t{escape(value)}" if kind == "error"
                else "\t".join(value))
        lines.append(f"{key}\t{rest}")
    write(path, "\n".join(lines) + "\n")


if __name__ == "__main__":
    if sys.argv[1] == "show":
        show(sys.argv[2], set(sys.argv[3:]))
    else:
        main(*sys.argv[1:])
