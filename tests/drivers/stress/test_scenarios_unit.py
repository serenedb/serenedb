import pytest

import scenarios
from ops import NameGen
from rng import derive

ENV = {
    "iceberg_fixtures": ["/fixture/plain_v1", "/fixture/part_v1"],
    "host": "127.0.0.1",
    "port": 1234,
    "attach_root": "/tmp/attach",
}
ITERATIONS = 8000


def drive(name, seed=31):
    st = scenarios.WorkerState(NameGen("ab", 0), env=ENV)
    pick = scenarios.resolve(name)
    stream = derive(seed, 0)
    ops_seen = []
    for _ in range(ITERATIONS):
        op = pick(stream, st)
        ops_seen.append(op)
        st.note_created(op)
        st.note_rows(op)
        st.note_attachment(op)
        st.note_dropped(op)
    return st, ops_seen


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_no_bucket_grows_without_bound(name):
    st, _ = drive(name)
    ceiling = max(st.table_cap, st.other_cap) + 8
    for label, bucket in (
        ("tables", st.tables), ("views", st.views), ("indexes", st.indexes),
        ("sequences", st.sequences), ("iceberg_views", st.iceberg_views),
        ("tokenizers", st.tokenizers), ("servers", st.servers),
        ("databases", st.databases), ("attachments", st.attachments),
    ):
        assert len(bucket) <= ceiling, (
            f"{name}: {label} reached {len(bucket)}, over the {ceiling} ceiling -- "
            "an op family is creating without checking its cap"
        )


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_no_duplicate_keys_in_any_bucket(name):
    st, _ = drive(name)
    for label, bucket in (
        ("tables", st.tables), ("views", st.views), ("indexes", st.indexes),
        ("iceberg_views", st.iceberg_views), ("servers", st.servers),
        ("databases", st.databases), ("tokenizers", st.tokenizers),
    ):
        assert len(bucket) == len(set(bucket)), f"{name}: duplicate keys in {label}"


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_an_inverted_index_is_only_built_over_an_iceberg_view(name):
    st = scenarios.WorkerState(NameGen("ab", 0), env=ENV)
    pick = scenarios.resolve(name)
    stream = derive(7, 0)
    for _ in range(ITERATIONS):
        op = pick(stream, st)
        if op.kind == "create_inverted_index" and op.needs:
            assert op.needs[0] in st.iceberg_views, (
                f"{name}: inverted index over {op.needs[0]}, which has no body "
                "column -- a ddl_churn view is SELECT id, v"
            )
        st.note_created(op)
        st.note_rows(op)
        st.note_attachment(op)
        st.note_dropped(op)


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_a_search_only_targets_an_index_whose_column_is_known(name):
    st = scenarios.WorkerState(NameGen("ab", 0), env=ENV)
    pick = scenarios.resolve(name)
    stream = derive(9, 0)
    for _ in range(ITERATIONS):
        op = pick(stream, st)
        if op.kind == "search_index":
            assert op.key in st.search_col, (
                f"{name}: searching {op.key} with no recorded column is a 42703 of "
                "our own making"
            )
        st.note_created(op)
        st.note_rows(op)
        st.note_attachment(op)
        st.note_dropped(op)


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_a_tokenizer_is_only_dropped_once_unreferenced(name):
    st = scenarios.WorkerState(NameGen("ab", 0), env=ENV)
    pick = scenarios.resolve(name)
    stream = derive(11, 0)
    for _ in range(ITERATIONS):
        op = pick(stream, st)
        if op.kind == "drop_tokenizer":
            assert op.key not in set(st.uses_dict.values()), (
                f"{name}: DROP TEXT SEARCH DICTIONARY while an index references it "
                "is 2BP01, and its CASCADE form is a syntax error"
            )
        st.note_created(op)
        st.note_rows(op)
        st.note_attachment(op)
        st.note_dropped(op)


@pytest.mark.parametrize("name", sorted(scenarios.SCENARIOS))
def test_parent_links_never_dangle(name):
    st, _ = drive(name)
    live = set(st.tables) | set(st.views) | set(st.iceberg_views)
    for child, parent in st.parent.items():
        assert parent in live or child not in (set(st.indexes) | set(st.views)), (
            f"{name}: {child} still points at dropped parent {parent}"
        )
