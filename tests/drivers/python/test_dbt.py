from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import tools_harness as harness
from dbt.cli.main import dbtRunner
from spec_loader import conn_kwargs
from target import check_golden, is_serenedb

DB = harness.database_name("dbt")
SERENEDB = is_serenedb()
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "dbt"


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    harness.create_empty_database(DB)
    root = tmp_path_factory.mktemp("dbt")
    project = root / "project"
    shutil.copytree(FIXTURES / "project", project)
    kw = conn_kwargs()
    (root / "profiles.yml").write_text(json.dumps({"drv": {"target": "dev", "outputs": {
        "dev": {"type": "postgres", "host": kw["host"], "port": kw["port"],
                "user": kw["user"], "password": "", "dbname": DB,
                "schema": "analytics", "threads": 1}}}}))
    yield project
    harness.drop_database(DB)


def dbt(project: Path, *args: str) -> list:
    paths = [] if args[0] == "debug" else ["--target-path", str(project / "target")]
    result = dbtRunner().invoke([
        *args, *paths, "--project-dir", str(project), "--profiles-dir",
        str(project.parent), "--log-path", str(project / "logs"), "--quiet"])
    assert result.exception is None, result.exception
    if not hasattr(result.result, "results"):
        assert result.success
        return []
    outcomes = sorted([node.node.unique_id, str(node.status),
                       node.adapter_response.get("_message"), node.message]
                      for node in result.result.results)
    assert result.success, outcomes
    return outcomes


def sql(statement: str) -> None:
    with harness.connect(DB) as conn:
        conn.execute(statement)


def test_debug(project):
    dbt(project, "debug")


def test_seed_run_snapshot_and_test(project):
    results = {
        "seed": dbt(project, "seed"),
        "run": dbt(project, "run"),
        "snapshot": dbt(project, "snapshot"),
        "test": dbt(project, "test"),
    }
    check_golden(FIXTURES / "build.json", json.dumps(results, indent=1) + "\n")


def test_incremental_models(project):
    sql("INSERT INTO analytics.customers_seed VALUES "
        "(5, 'Zeus', '2024-05-06', 1.50, 'bronze')")
    sql("UPDATE analytics.customers_seed SET score = 99.00 WHERE id = 1")
    check_golden(FIXTURES / "incremental.json", json.dumps(
        dbt(project, "run", "--select", "recent_customers", "appended_customers"),
        indent=1) + "\n")
    with harness.connect(DB) as conn:
        assert conn.execute("SELECT count(*) FROM analytics.recent_customers"
                            ).fetchone()[0] == 5
        assert conn.execute("SELECT count(*) FROM analytics.appended_customers"
                            ).fetchone()[0] == 5


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="catalog bug: temporary tables are missing from "
                          "information_schema.columns, so dbt reads no columns from "
                          "its snapshot staging table and renders insert into t ()")
def test_snapshot_records_changes(project):
    dbt(project, "snapshot")
    with harness.connect(DB) as conn:
        assert conn.execute("SELECT count(*) FROM snapshots.customers_snapshot "
                            "WHERE id = 1").fetchone()[0] == 2


def test_full_refresh(project):
    dbt(project, "run", "--full-refresh")


def test_docs_catalog_matches_postgresql(project):
    dbt(project, "docs", "generate")
    catalog = json.loads((project / "target" / "catalog.json").read_text())
    nodes = {}
    for unique_id, node in sorted(catalog["nodes"].items()):
        metadata = node["metadata"]
        nodes[unique_id] = {
            "metadata": [metadata["type"], metadata["schema"], metadata["name"],
                         metadata["comment"], metadata["owner"]],
            "columns": [[column["index"], column["name"], column["type"],
                         column["comment"]] for column in node["columns"].values()],
        }
    check_golden(FIXTURES / "catalog.json", json.dumps(nodes, indent=1) + "\n")
