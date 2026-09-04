#!/usr/bin/env python3
import argparse
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
    text = BLANK_RUN_RE.sub("\n\n", "\n".join(out)).strip("\n")
    return text + "\n" if text else ""


def collect(docs_dir: pathlib.Path) -> list[tuple[str, str, str]]:
    docs = []
    for path in sorted(docs_dir.rglob("*")):
        if not path.is_file() or path.suffix not in EXTENSIONS:
            continue
        rel = path.relative_to(docs_dir).as_posix()
        meta, body = split_frontmatter(path.read_text(encoding="utf-8"))
        title = meta.get("title") or path.stem
        content = clean(body)
        for field in (title, content):
            if f"){DELIMITER}\"" in field:
                sys.exit(f"{rel}: contains the raw string delimiter ){DELIMITER}\"")
        docs.append((rel, title, content))
    return docs


def digest(docs: list[tuple[str, str, str]]) -> str:
    h = hashlib.sha256()
    for fields in docs:
        for field in fields:
            h.update(field.encode("utf-8"))
            h.update(b"\0")
    return h.hexdigest()


def raw(text: str) -> str:
    return f'R"{DELIMITER}({text}){DELIMITER}"'


def render(docs: list[tuple[str, str, str]]) -> str:
    out = ['#include "docs/docs_data.h"', "", "namespace sdb::docs {"]
    if docs:
        out += ["namespace {", "", "constexpr Doc kDocs[] = {"]
        for rel, title, content in docs:
            out.append(f"  {{{raw(rel)}, {raw(title)}, {raw(content)}}},")
        out += ["};", "", "}  // namespace", ""]
        out.append("std::span<const Doc> GetDocs() { return kDocs; }")
    else:
        out += ["", "std::span<const Doc> GetDocs() { return {}; }"]
    out += [
        "",
        f'std::string_view GetDocsHash() {{ return "{digest(docs)}"; }}',
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
    docs = collect(args.docs_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(docs), encoding="utf-8")
    print(f"generated {args.output} from {len(docs)} docs")


if __name__ == "__main__":
    main()
