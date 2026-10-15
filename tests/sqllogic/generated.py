import hashlib
import os
import re
import subprocess
import sys

PLACEHOLDER = "<recorded>"
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


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


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


def keys(text):
    queries = [block[1].replace("\t", " ") for block in blocks(text)
               if spots(block)]
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


def load(path, text):
    try:
        with open(path) as f:
            lines = f.read().split("\n")
    except FileNotFoundError:
        return {}
    if lines[0] != f"skeleton {digest(text)}":
        print(f"{path} was recorded for another version of its generator, "
              "record it again with run.sh --override", file=sys.stderr)
        return {}
    records = {}
    for line in lines[1:]:
        if not line:
            continue
        ordinal, _, rest = line.split("\t", 2)
        if rest.startswith("error\t"):
            records[int(ordinal)] = ("error", unescape(rest[len("error\t"):]))
        else:
            records[int(ordinal)] = ("values", rest.split(" "))
    return records


def fill(text, records):
    out = []
    ordinal = 0
    for block in blocks(text):
        places = spots(block)
        if not places:
            out.append(block)
            continue
        ordinal += 1
        kind, recorded = records.get(ordinal, ("values", []))
        if kind == "error":
            sql = block[1:block.index("----")]
            out.append(["query error", *sql, "----", *recorded.split("\n"),
                        ""])
            continue
        filled = list(block)
        for (i, j), value in zip(places, recorded):
            cells = filled[i].split("\t")
            cells[j] = value
            filled[i] = "\t".join(cells)
        out.append(filled)
    return join(out)


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
    records = {}
    ordinal = 0
    for expected, actual in zip(want, got):
        places = spots(expected)
        if not places:
            continue
        ordinal += 1
        if expected[1] != actual[1]:
            sys.exit(f"{target} doesn't follow its generator at {expected[1]}")
        error = error_of(actual)
        if error is not None:
            records[ordinal] = ("error", error)
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
        records[ordinal] = ("values", values)
    return records


def new_errors(text, previous, records):
    sql = keys(text)
    return [f"{ordinal}\t{sql[ordinal - 1]}\n  {error}"
            for ordinal, (kind, error) in sorted(records.items())
            if kind == "error" and previous.get(ordinal) != (kind, error)]


def main(command, generator, target):
    text = subprocess.run([sys.executable, generator], check=True,
                          stdout=subprocess.PIPE, text=True).stdout
    path = generator[:-len(".py")] + ".recorded"
    if command == "write":
        write(target, fill(text, load(path, text)))
        return
    if not any(spots(block) for block in blocks(text)):
        return
    with open(target) as f:
        records = record(text, f.read(), target)
    if records is None:
        print(f"{target} wasn't overridden, {path} is unchanged",
              file=sys.stderr)
        return
    fresh = new_errors(text, load(path, text), records)
    if fresh and os.environ.get(NEW_ERRORS) != "1":
        sys.exit(f"{target} has queries that fail where {path} recorded "
                 f"something else; check them and record with "
                 f"{NEW_ERRORS}=1:\n" + "\n".join(fresh))
    lines = [f"skeleton {digest(text)}"]
    for ordinal, sql in enumerate(keys(text), 1):
        kind, recorded = records[ordinal]
        rest = (f"error\t{escape(recorded)}" if kind == "error"
                else " ".join(recorded))
        lines.append(f"{ordinal}\t{sql}\t{rest}")
    write(path, "\n".join(lines) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
