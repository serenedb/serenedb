import hashlib
import os
import re
import subprocess
import sys

PLACEHOLDER = "<recorded>"
KEY_WIDTH = 16
SQL_WIDTH = 100
LITERAL_WIDTH = 24
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


def escape(text):
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")


def unescape(text):
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m[1], m[1]),
                  text)


def excerpts(sqls):
    queries = [sql.replace("\n", " ").replace("\t", " ") for sql in sqls]
    prefix = os.path.commonprefix(queries) if len(queries) > 1 else ""
    while prefix and (prefix.count("'") % 2 or not prefix.endswith(" ")):
        prefix = prefix[:-1]
    out = []
    for sql in queries:
        parts = sql[len(prefix):].split("'")
        parts[1::2] = [part if len(part) <= LITERAL_WIDTH
                       else part[:LITERAL_WIDTH - 3] + "..."
                       for part in parts[1::2]]
        sql = "'".join(parts)
        out.append(sql if len(sql) <= SQL_WIDTH
                   else sql[:SQL_WIDTH - 3] + "...")
    return out


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
        key, _, rest = line.split("\t", 2)
        if rest.startswith("error\t"):
            records[key] = ("error", unescape(rest[len("error\t"):]))
        else:
            records[key] = ("values", rest.split(" "))
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
            if not value or len(value.split()) != 1:
                sys.exit(f"{target} can't record {value!r} at {expected[1]}")
            values.append(value)
        records[key] = ("values", values)
    return records


def generate(generator):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [os.path.dirname(os.path.abspath(__file__)),
                      env.get("PYTHONPATH")]))
    return subprocess.run([sys.executable, generator], check=True, env=env,
                          stdout=subprocess.PIPE, text=True).stdout


def main(command, generator, target):
    text = generate(generator)
    path = generator[:-len(".py")] + ".recorded"
    if command == "write":
        filled, missing = fill(text, load(path))
        write(target, filled)
        if missing:
            print(f"{target}: {missing} queries have no value in {path}, "
                  "record them with run.sh --override", file=sys.stderr)
        return
    if not any(spots(block) for block in blocks(text)):
        return
    with open(target) as f:
        records = record(text, f.read(), target)
    if records is None:
        print(f"{target} wasn't overridden, {path} is unchanged",
              file=sys.stderr)
        return
    queries = recorded(text)
    shown = dict(zip((key for key, _ in queries),
                     excerpts([sql for _, sql in queries])))
    previous = load(path)
    fresh = [f"{key}\t{shown[key]}\n  {value}"
             for key, (kind, value) in records.items()
             if kind == "error" and previous.get(key) != (kind, value)]
    if fresh and os.environ.get(NEW_ERRORS) != "1":
        sys.exit(f"{target} has queries that fail where {path} recorded "
                 f"something else; check them and record with "
                 f"{NEW_ERRORS}=1:\n" + "\n".join(fresh))
    lines = []
    for key, (kind, value) in records.items():
        rest = (f"error\t{escape(value)}" if kind == "error"
                else " ".join(value))
        lines.append(f"{key}\t{shown[key]}\t{rest}")
    write(path, "\n".join(lines) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
