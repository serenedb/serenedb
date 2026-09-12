"""Randomized text search dictionary chains against their function form.

Every template is also a scalar function of the same name and a chain is the
nesting of those calls, so a dictionary created from a random chain has to
produce the tokens the nested functions produce. A stored dictionary is a stage
as well, so the full chain has to equal the last stage's function applied to a
dictionary holding the prefix. Unions interleave their members by position, so
they are compared as sorted lists. Shingles wrap a chain and take the nested
function's list as their token stream.

Token lists travel back hex-encoded: shingle tokens carry a 0xFF separator and
sparse grams cut multi-byte characters, so neither is guaranteed to be valid
UTF-8. Chains with sparse grams are fed ASCII only for that reason, and the
shingle comparison skips values whose base stream is empty, where the function
form emits one empty token and the dictionary none. A list also carries no
positions, so the wrapped chains hold no n-gram stages: their grams share
positions, which the dictionary form sees and the function form cannot.
"""

from __future__ import annotations

import random

import psycopg
import pytest
from psycopg import sql
from spec_loader import conn_kwargs, schema_name

DRIVER_KEY = "python_dictionary_chains"
SEED = 0x5EEDC0FFEE
CHAINS = 64
UNIONS = 24
WRAPPED = 16

VALUES = [
    "Quick Brown foxes, running FAST",
    "a,b;c d",
    "  spaced , out  ",
    "Café résumé naïve",
    "x",
    "",
    "   ",
    "The the THE a A",
    "path/to/some/file.txt",
    "über Straße ÄÖÜ",
    "quick,brown,fox",
    "hello   world",
    "MiXeD CaSe TOKENS here",
    "tab\tseparated\tvalues",
    "punct!?.;: marks",
    "12,34 56;78",
    "Ελληνικά και English",
    "日本語 テキスト",
]
ASCII_VALUES = [v for v in VALUES if v.isascii()]

# (stage as written after AS, the same stage as a function of {x})
TOKENIZERS = [
    ("split_text(case := 'lower')", "split_text({x}, case := 'lower')"),
    ("split_text()", "split_text({x})"),
    ("split_csv(',')", "split_csv({x}, ',')"),
    ("split_by_delimiters([',', ';', ' '])", "split_by_delimiters({x}, [',', ';', ' '])"),
    ("split_by_non_alpha(case := 'upper')", "split_by_non_alpha({x}, case := 'upper')"),
    ("split_by_pattern('[ ,;]+')", "split_by_pattern({x}, '[ ,;]+')"),
    ("expand_path()", "expand_path({x})"),
    ("generate_ngrams(2, 3)", "generate_ngrams({x}, 2, 3)"),
    ("generate_sparse_ngrams()", "generate_sparse_ngrams({x})"),
    ("stem_words('en_US.UTF-8')", "stem_words({x}, 'en_US.UTF-8')"),
    (
        "normalize_tokens('en_US.UTF-8', case := 'lower', accent := false)",
        "normalize_tokens({x}, 'en_US.UTF-8', case := 'lower', accent := false)",
    ),
    ("remove_stopwords(['the', 'a'])", "remove_stopwords({x}, ['the', 'a'])"),
]

# SQL stages read the value as $1; their function form maps over the list {l}.
SQL_STAGES = [
    ("upper($1)", "list_transform({l}, x -> upper(x))"),
    ("(x -> trim(x))", "list_transform({l}, x -> trim(x))"),
    ("replace($1, 'o', '0')", "list_transform({l}, x -> replace(x, 'o', '0'))"),
    ("string_split($1, ',')", "flatten(list_transform({l}, x -> string_split(x, ',')))"),
    ("$1", "{l}"),
]

BYTE_ORIENTED = ("generate_sparse_ngrams",)


def _stage_function(stage: tuple[str, str], prev: str | None) -> str:
    dictionary, function = stage
    if "{x}" in function:
        return function.format(x=prev if prev is not None else "v")
    return function.format(l=prev if prev is not None else "[v]")


POSITIONAL = ("generate_ngrams", "generate_sparse_ngrams")


def _chain(rng: random.Random, length: int, positional: bool = True) -> list[tuple[str, str]]:
    pool = [
        stage
        for stage in TOKENIZERS * 3 + SQL_STAGES
        if positional or not stage[0].startswith(POSITIONAL)
    ]
    return [rng.choice(pool) for _ in range(length)]


def _functions(stages: list[tuple[str, str]]) -> str:
    expr: str | None = None
    for stage in stages:
        expr = _stage_function(stage, expr)
    assert expr is not None
    return expr


def _dictionary(stages: list[tuple[str, str]]) -> str:
    return " | ".join(stage[0] for stage in stages)


def _values(*chains: list[tuple[str, str]]) -> list[str]:
    for stages in chains:
        if any(stage[0].startswith(BYTE_ORIENTED) for stage in stages):
            return ASCII_VALUES
    return VALUES


RNG = random.Random(SEED)
CHAIN_CASES = [_chain(RNG, RNG.randint(1, 4)) for _ in range(CHAINS)]
UNION_CASES = [(_chain(RNG, RNG.randint(1, 2)), _chain(RNG, RNG.randint(1, 2))) for _ in range(UNIONS)]
WRAPPED_CASES = [_chain(RNG, RNG.randint(1, 3), positional=False) for _ in range(WRAPPED)]


@pytest.fixture(scope="module")
def schema() -> str:
    return schema_name(DRIVER_KEY)


@pytest.fixture(scope="module")
def conn(schema: str) -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    yield c
    with c.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    c.close()


def _create(cur, name: str, definition: str) -> None:
    cur.execute(f"CREATE TEXT SEARCH DICTIONARY {name} AS {definition}")


def _mismatches(cur, lhs: str, rhs: str, values: list[str], only: str = "true") -> list:
    rows = ", ".join("(" + sql.quote(v) + ")" for v in values)
    cur.execute(
        f"SELECT v, list_transform(l, x -> hex(x::BLOB)), list_transform(r, x -> hex(x::BLOB)) "
        f"FROM (SELECT v, {lhs} AS l, {rhs} AS r FROM (VALUES {rows}) AS t(v) WHERE {only}) "
        "WHERE l IS DISTINCT FROM r"
    )
    return cur.fetchall()


@pytest.mark.parametrize("stages", CHAIN_CASES, ids=[str(i) for i in range(CHAINS)])
def test_chain_matches_nested_functions(conn, schema, stages, request):
    name = f"{schema}.chain_{request.node.callspec.id}"
    with conn.cursor() as cur:
        _create(cur, name, _dictionary(stages))
        bad = _mismatches(cur, f"ts_lexize('{name}', v)", _functions(stages), _values(stages))
    assert bad == [], f"{_dictionary(stages)} vs {_functions(stages)}: {bad}"


@pytest.mark.parametrize(
    "stages",
    [s for s in CHAIN_CASES if len(s) >= 2],
    ids=[str(i) for i, s in enumerate(CHAIN_CASES) if len(s) >= 2],
)
def test_prefix_dictionary_is_a_stage(conn, schema, stages, request):
    ident = request.node.callspec.id
    prefix = f"{schema}.prefix_{ident}"
    full = f"{schema}.full_{ident}"
    with conn.cursor() as cur:
        _create(cur, prefix, _dictionary(stages[:-1]))
        _create(cur, full, f"{prefix} | {stages[-1][0]}")
        bad = _mismatches(
            cur,
            f"ts_lexize('{full}', v)",
            _stage_function(stages[-1], f"ts_lexize('{prefix}', v)"),
            _values(stages),
        )
    assert bad == [], f"{_dictionary(stages)} via prefix: {bad}"


@pytest.mark.parametrize("pair", UNION_CASES, ids=[str(i) for i in range(UNIONS)])
def test_union_merges_its_members(conn, schema, pair, request):
    name = f"{schema}.union_{request.node.callspec.id}"
    left, right = pair
    with conn.cursor() as cur:
        _create(cur, name, f"[{_dictionary(left)}, {_dictionary(right)}]")
        bad = _mismatches(
            cur,
            f"list_sort(ts_lexize('{name}', v))",
            f"list_sort(list_concat({_functions(left)}, {_functions(right)}))",
            _values(left, right),
        )
    assert bad == [], f"[{_dictionary(left)}, {_dictionary(right)}]: {bad}"


@pytest.mark.parametrize("stages", WRAPPED_CASES, ids=[str(i) for i in range(WRAPPED)])
def test_shingles_wrap_a_chain(conn, schema, stages, request):
    name = f"{schema}.shingle_{request.node.callspec.id}"
    base = _functions(stages)
    with conn.cursor() as cur:
        _create(cur, name, f"generate_shingles({_dictionary(stages)}, 2, 2, storetokens := false)")
        bad = _mismatches(
            cur,
            f"ts_lexize('{name}', v)",
            f"generate_shingles({base}, 2, 2, storetokens := false)",
            _values(stages),
            only=f"len({base}) > 0",
        )
    assert bad == [], f"generate_shingles({_dictionary(stages)}): {bad}"
