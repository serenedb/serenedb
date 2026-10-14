import hashlib
import os
import subprocess
import sys

PLACEHOLDER = "<recorded>"
PER_LINE = 20


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


def load(path, text):
    try:
        with open(path) as f:
            lines = f.read().split("\n")
    except FileNotFoundError:
        return [], []
    if lines[0] != f"skeleton {digest(text)}":
        print(f"{path} was recorded for another version of its generator, "
              "record it again with run.sh --override", file=sys.stderr)
        return [], []
    errors = [line[len("error "):] for line in lines[1:]
              if line.startswith("error ")]
    values = [value for line in lines[1:] if not line.startswith("error ")
              for value in line.split()]
    return values, errors


def fill(text, values, errors):
    tokens = iter(values)
    out = []
    for block in blocks(text):
        filled = list(block)
        for i, j in spots(block):
            token = next(tokens, None)
            if token is None:
                break
            if token.startswith("!"):
                filled = ([f"query error {errors[int(token[1:])]}"] +
                          block[1:block.index("----")])
                break
            cells = filled[i].split("\t")
            cells[j] = token
            filled[i] = "\t".join(cells)
        out.append(filled)
    return join(out)


def record(text, overridden, target):
    want, got = blocks(text), blocks(overridden)
    if len(want) != len(got):
        sys.exit(f"{target} doesn't follow its generator")
    values = []
    errors = []
    for expected, actual in zip(want, got):
        places = spots(expected)
        if not places:
            continue
        if expected[1] != actual[1]:
            sys.exit(f"{target} doesn't follow its generator at {expected[1]}")
        if actual[0].startswith("query error "):
            error = actual[0][len("query error "):]
            if error not in errors:
                errors.append(error)
            values.append(f"!{errors.index(error)}")
            continue
        for i, j in places:
            cells = actual[i].split("\t") if i < len(actual) else []
            value = cells[j] if j < len(cells) else ""
            if value == PLACEHOLDER:
                return None
            if not value or value.startswith("!") or len(value.split()) != 1:
                sys.exit(f"{target} can't record {value!r} at {expected[1]}")
            values.append(value)
    return values, errors


def main(command, generator, target):
    text = subprocess.run([sys.executable, generator], check=True,
                          stdout=subprocess.PIPE, text=True).stdout
    path = generator[:-len(".py")] + ".recorded"
    if command == "write":
        write(target, fill(text, *load(path, text)))
        return
    if not any(spots(block) for block in blocks(text)):
        return
    with open(target) as f:
        recorded = record(text, f.read(), target)
    if recorded is None:
        print(f"{target} wasn't overridden, {path} is unchanged",
              file=sys.stderr)
        return
    values, errors = recorded
    lines = [f"skeleton {digest(text)}"]
    lines += [f"error {error}" for error in errors]
    lines += [" ".join(values[k:k + PER_LINE])
              for k in range(0, len(values), PER_LINE)]
    write(path, "\n".join(lines) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
