#!/usr/bin/env python3
import argparse
from builtins import tuple
import hashlib
import pathlib
import re
import sys

DELIMITER = "sdbdoc"
EXTENSIONS = {".md", ".mdx"}

FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n", re.S)
IMPORT_RE = re.compile(r"^\s*(import|export)\s.*$")
COMPONENT_OPEN_RE = re.compile(r"^\s*<([A-Z][A-Za-z.]*)(\s[^>]*)?>\s*$")
COMPONENT_CLOSE_RE = re.compile(r"^\s*</([A-Z][A-Za-z.]*)>\s*$")
COMPONENT_SELF_CLOSING_RE = re.compile(r"<[A-Z][A-Za-z.]*(\s[^>]*)?/>")
HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
EMPTY_DIV_RE = re.compile(r"^\s*<div\s[^>]*>\s*</div>\s*$")
WRAPPER_TAG_RE = re.compile(r"</?(details|summary)>")
JSX_STYLE_RE = re.compile(r"\s*style=\{\{[^}]*\}\}")
# "## Setup {#setup}" -> "## Setup". #{1,6} is one to six literal hashes (the
# heading level); \{ and \} are literal braces; [^}]* is anything up to the
# closing brace. Group 1 is the heading without the anchor.
HEADING_ANCHOR_RE = re.compile(r"^(#{1,6}\s.*?)\s*\{#[^}]*\}\s*$")
BLANK_RUN_RE = re.compile(r"\n{3,}")


def split_frontmatter(text: str) -> tuple[dict[str, str], str]:
    match = FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    meta = {}
    for line in match.group(1).splitlines():
        key, sep, value = line.partition(":")
        if sep:
            meta[key.strip()] = value.strip().strip('"').strip("'")
    return meta, text[match.end():]


def clean(body: str) -> str:
    out = []
    depth = 0
    for line in HTML_COMMENT_RE.sub("", body).split("\n"):
        if IMPORT_RE.match(line) or EMPTY_DIV_RE.match(line):
            continue
        if COMPONENT_OPEN_RE.match(line):
            depth += 1
            continue
        if COMPONENT_CLOSE_RE.match(line):
            depth = max(depth - 1, 0)
            continue
        line = COMPONENT_SELF_CLOSING_RE.sub("", line)
        line = WRAPPER_TAG_RE.sub("", line)
        line = JSX_STYLE_RE.sub("", line)
        line = HEADING_ANCHOR_RE.sub(r"\1", line)
        if depth > 0:
            line = line.strip()
        if line.strip() in ("", ">"):
            line = ""
        out.append(line.rstrip())
    # Removing lines leaves holes: squeeze blank runs, trim blank edges, then
    # end with exactly one newline unless nothing is left at all.
    text = BLANK_RUN_RE.sub("\n\n", "\n".join(out)).strip("\n")
    return text + "\n" if text else ""


SPLIT_MODES = ("headings", "page")
HEADING_RE = re.compile(r"^(#{1,6}) (.*)$")
# Inline formatting that must not leak into keys and titles: `code`, **bold**,
# *em*, [text](url). Underscores stay: date_part and datepart are different.
INLINE_CODE_RE = re.compile(r"`([^`]*)`")
LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
EMPHASIS_RE = re.compile(r"\*\*|\*")


def plain_heading(text: str) -> str:
    text = INLINE_CODE_RE.sub(r"\1", text)
    text = LINK_RE.sub(r"\1", text)
    return EMPHASIS_RE.sub("", text).strip()


def key_segment(title: str) -> str:
    return title.replace("#", "\\#").replace(" ", "_")


class Unit:
    def __init__(self, path, title, breadcrumb, content):
        self.path, self.title, self.breadcrumb, self.content = path, title, breadcrumb, content

    def fields(self):
        return (self.path, self.title, self.breadcrumb, self.content)


def split_units(page: str, title: str, body: str) -> list[Unit]:
    # Heading rows only, each holding everything nested under it. The page
    # title acts as the H1: "page#Title" carries the whole page and every
    # heading key hangs under it; a body H1 repeating the title is folded in.
    lines = body.split("\n")
    heads = []
    in_fence = False
    for i, line in enumerate(lines):
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        match = None if in_fence else HEADING_RE.match(line)
        if match:
            heads.append((i, len(match.group(1)), plain_heading(match.group(2))))
    root = page + "#" + key_segment(title)
    units = [Unit(root, title, "", body)]
    stack = []
    for n, (i, level, heading) in enumerate(heads):
        if level == 1 and heading == title:
            continue
        end = next((j for j, l, _ in heads[n + 1:] if l <= level), len(lines))
        content = "\n".join(lines[i + 1:end]).strip("\n")
        stack = [s for s in stack if s[0] < level] + [(level, heading)]
        key = root + "".join("#" + key_segment(h) for _, h in stack)
        breadcrumb = " / ".join([title] + [h for _, h in stack[:-1]])
        units.append(Unit(key, heading, breadcrumb, content + "\n" if content else ""))
    return units


def collect(docs_dir: pathlib.Path) -> list[Unit]:
    units = []
    errors = []
    for path in sorted(docs_dir.rglob("*")):
        if not path.is_file() or path.suffix not in EXTENSIONS:
            continue
        rel = path.relative_to(docs_dir).as_posix()
        meta, body = split_frontmatter(path.read_text(encoding="utf-8"))
        title = meta.get("title") or path.stem
        split = meta.get("split")
        if split not in SPLIT_MODES:
            errors.append(f"{rel}: frontmatter key 'split' must be one of {', '.join(SPLIT_MODES)} (got {split!r})")
            continue
        content = clean(body)
        page_units = split_units(rel, title, content) if split == "headings" else [Unit(rel, title, "", content)]
        seen = set()
        for unit in page_units:
            if unit.path in seen:
                errors.append(f"{rel}: duplicate section key {unit.path!r}")
            seen.add(unit.path)
            for field in unit.fields():
                if f"){DELIMITER}\"" in field:
                    errors.append(f"{rel}: contains the raw string delimiter ){DELIMITER}\"")
        units += page_units
    if errors:
        sys.exit("\n".join(errors))
    return units


def digest(units: list[Unit]) -> str:
    h = hashlib.sha256()
    for unit in units:
        for field in unit.fields():
            h.update(field.encode("utf-8"))
            h.update(b"\0")
    return h.hexdigest()


def raw(text: str) -> str:
    return f'R"{DELIMITER}({text}){DELIMITER}"'


def render(units: list[Unit]) -> str:
    out = ['#include "docs/docs_data.h"', "", "namespace sdb::docs {"]
    if units:
        out += ["namespace {", "", "constexpr Doc kDocs[] = {"]
        for u in units:
            # f-string: {{ and }} emit the literal braces of the C++ initializer.
            out.append(f"  {{{raw(u.path)}, {raw(u.title)}, {raw(u.breadcrumb)}, {raw(u.content)}}},")
        out += ["};", "", "}  // namespace", ""]
        out.append("std::span<const Doc> GetDocs() { return kDocs; }")
    else:
        out += ["", "std::span<const Doc> GetDocs() { return {}; }"]
    out += [
        "",
        f'std::string_view GetDocsHash() {{ return "{digest(units)}"; }}',
        "",
        "}  // namespace sdb::docs",
        "",
    ]
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("docs_dir", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    if not args.docs_dir.is_dir():
        sys.exit(f"{args.docs_dir}: not a directory")
    units = collect(args.docs_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(units), encoding="utf-8")
    pages = len({u.path.split("#", 1)[0] for u in units})
    print(f"generated {args.output}: {pages} pages, {len(units)} rows")


if __name__ == "__main__":
    main()
