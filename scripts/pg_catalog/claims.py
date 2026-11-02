import pathlib
import re

from .common import PG_CATALOG, REPO_ROOT

REPO = pathlib.Path(REPO_ROOT)
DOCS = REPO / "docs/compatibility/system-table-compatibility.md"
TYPE_DOCS = REPO / "docs/compatibility/data-type-compatibility.md"
CLAIM_ROW = re.compile(r"\|\s*([a-z_][a-z_0-9]*)\s*\|\s*(🟢|🟡|🔴)\s*\|")

SECTIONS = {
    "## System Tables": ("sys_tables", "pg_catalog"),
    "## System Views": ("sys_views", "pg_catalog"),
    "## Information Schema": ("sys_infoschema", "information_schema"),
}


def parse(path):
    section = None
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        for heading, value in SECTIONS.items():
            if line.startswith(heading):
                section = value
        match = CLAIM_ROW.match(line)
        if match and section:
            rows.append((section[0], section[1], match.group(1), match.group(2)))
    if not rows:
        raise SystemExit("no claim rows parsed from " + str(path))
    return rows


def type_rows():
    rows = [match.groups()
            for line in TYPE_DOCS.read_text(encoding="utf-8").splitlines()
            if (match := CLAIM_ROW.match(line))]
    if not rows:
        raise SystemExit("no type rows parsed from " + str(TYPE_DOCS))
    return rows


def check_types(gen):
    arrays = {row["typarray"] for row in gen.rows["pg_type"]}
    expected = {row["typname"] for row in gen.rows["pg_type"]
                if row["typnamespace"] == PG_CATALOG
                and row["typtype"] in "brm" and row["oid"] not in arrays}
    documented = [name for name, _ in type_rows()]
    if len(documented) != len(set(documented)):
        raise SystemExit(f"{TYPE_DOCS.name} lists a type twice")
    if missing := sorted(expected - set(documented)):
        raise SystemExit(f"{TYPE_DOCS.name} does not list {missing}")
    if unknown := sorted(set(documented) - expected):
        raise SystemExit(f"{TYPE_DOCS.name} lists types PostgreSQL does not "
                         f"have: {unknown}")
