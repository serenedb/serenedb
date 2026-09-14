import pathlib
import re
from dataclasses import dataclass, field

MARKER_RE = re.compile(r"^\s*#\s+DOCS_TEST:\s*([A-Za-z0-9_.-]+)\s*$")
BODY_RE = re.compile(r"^\s*#\s+DOCS_TEST_BODY\s*$")
RAW_RE = re.compile(r"^\s*#\s+DOCS_TEST_RAW\s*$")
RAW_LINE_RE = re.compile(r"^\s*#\|( ?)(.*)$")
HEADER_RE = re.compile(r"^(query|statement)\b", re.I)
BOUNDARY_RE = re.compile(
    r"^(query|statement|connection|skipif|onlyif|control|hash-threshold)\b", re.I
)
NOOP_RE = re.compile(r"^CREATE\s+(?:TEMP\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?__docs_", re.I)
TAG_RE = re.compile(r"^[ \t>]*<SqlLogicTest\b([^>]*)/>[ \t]*$")
TAG_ID_RE = re.compile(r"\bid\s*=\s*\"([^\"]*)\"")

SITE_DOCS_BASES = ("sdb/pg/site_docs", "recovery/site_docs")


@dataclass
class Snippet:
    command: str = ""
    result: str = ""
    setup_result: str = ""
    docs_only: bool = False


@dataclass
class Report:
    inlined: int = 0
    missing: dict = field(default_factory=dict)
    empty: dict = field(default_factory=dict)


def _joined(lines):
    return "\n".join(lines).strip()


def _find(lines, start, pattern):
    return next((i for i in range(start, len(lines)) if pattern.match(lines[i])), -1)


def _find_boundary(lines, start):
    for index in range(start, len(lines)):
        if BOUNDARY_RE.match(lines[index].strip()) and _after_gap(lines, index):
            return index
    return len(lines)


def _after_gap(lines, index):
    for cursor in range(index - 1, -1, -1):
        line = lines[cursor].strip()
        if not line:
            return True
        if not line.startswith("#"):
            return False
    return True


def _header_result(header):
    if header.lower().startswith("statement"):
        return header[len("statement") :].strip() or "ok"
    tail = header[len("query") :].strip()
    return tail if tail.lower().startswith("error") else ""


def _parse_block(lines, header_index):
    header = lines[header_index].strip()
    end = _find_boundary(lines, header_index + 1)
    separator = next((i for i in range(header_index + 1, end) if lines[i].strip() == "----"), -1)
    if separator == -1:
        return _joined(lines[header_index + 1 : end]), _header_result(header), end
    command = _joined(lines[header_index + 1 : separator])
    end = _find_boundary(lines, separator + 1)
    result = _joined(lines[separator + 1 : end])
    if not result and header.lower().startswith("statement"):
        result = _header_result(header)
    return command, result, end


def _parse_blocks(lines):
    blocks = []
    cursor = 0
    while cursor < len(lines):
        header_index = _find(lines, cursor, HEADER_RE)
        if header_index == -1:
            break
        command, result, end = _parse_block(lines, header_index)
        blocks.append((command, result))
        cursor = max(end, header_index + 1)
    command = "\n\n".join(command for command, _ in blocks if command)
    result = "\n\n".join(result or "ok" for _, result in blocks)
    return command, result


def _parse_raw(lines):
    decoded = [RAW_LINE_RE.sub(r"\2", line) for line in lines]
    separator = next((i for i in range(len(decoded)) if decoded[i].strip() == "----"), -1)
    if separator == -1:
        return _joined(decoded), ""
    return _joined(decoded[:separator]), _joined(decoded[separator + 1 :])


def _drop_ok(value):
    chunks = [chunk.strip() for chunk in re.split(r"\n{2,}", value)]
    return "\n\n".join(chunk for chunk in chunks if chunk and chunk.lower() != "ok")


def parse_test_file(text):
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    markers = [(i, m.group(1)) for i, line in enumerate(lines) if (m := MARKER_RE.match(line))]
    if not markers or not any(BODY_RE.match(line) for line in lines):
        return {}
    bounds = [line for line, _ in markers[1:]] + [len(lines)]
    snippets = {}
    for (marker_line, name), bound in zip(markers, bounds):
        region = lines[marker_line + 1 : bound]
        body_index = _find(region, 0, BODY_RE)
        if body_index == -1:
            command, result = _parse_blocks(region)
            snippets[name] = Snippet(command, result)
            continue
        raw_index = _find(region[:body_index], 0, RAW_RE)
        setup_end = body_index if raw_index == -1 else raw_index
        _, setup_result = _parse_blocks(region[:setup_end])
        command, result = _parse_blocks(region[body_index + 1 :])
        raw_command, raw_result = ("", "")
        if raw_index != -1:
            raw_command, raw_result = _parse_raw(region[raw_index + 1 : body_index])
        docs_only = False
        if raw_command:
            noop = not command.strip() or bool(NOOP_RE.match(command.strip()))
            result = raw_result if noop else raw_result or result
            command = raw_command
            docs_only = noop and not raw_result
        snippets[name] = Snippet(command, result, _drop_ok(setup_result), docs_only)
    return snippets


def load(tests_root: pathlib.Path) -> dict:
    snippets = {}
    for base in SITE_DOCS_BASES:
        root = tests_root / base
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.test*")):
            file_id = re.sub(r"\.test(?:_slow)?$", "", path.relative_to(root).as_posix())
            for name, snippet in parse_test_file(path.read_text(encoding="utf-8")).items():
                snippets[f"{file_id}/{name}"] = snippet
    return snippets


def render(snippet: Snippet, hide_result: bool) -> str:
    blocks = []
    if snippet.command:
        blocks.append(f"```{'sqllogictest' if snippet.docs_only else 'sql'}\n{snippet.command}\n```")
    if not hide_result and not snippet.docs_only:
        setup = f"setup error:\n{snippet.setup_result}" if snippet.setup_result else ""
        result = _drop_ok("\n\n".join(filter(None, (setup, snippet.result))).replace("<slt:ignore>", "(varies)"))
        if result:
            blocks.append(f"```\n{result}\n```")
    return "\n\n".join(blocks)


def inline(body: str, snippets: dict, page: str, report: Report) -> str:
    out = []
    for line in body.split("\n"):
        match = TAG_RE.match(line)
        if not match:
            out.append(line)
            continue
        id_match = TAG_ID_RE.search(match.group(1))
        test_id = id_match.group(1) if id_match else ""
        snippet = snippets.get(test_id)
        if snippet is None:
            report.missing.setdefault(page, []).append(test_id)
            continue
        rendered = render(snippet, "hideResult" in match.group(1))
        if not rendered:
            report.empty.setdefault(page, []).append(test_id)
            continue
        report.inlined += 1
        out += ["", *rendered.split("\n"), ""]
    return "\n".join(out)
