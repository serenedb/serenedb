import os
import socket
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "tests" / "drivers" / "sqlsmith" / "run.sh"

SQLSMITH = """#!/bin/bash
dump=0
for arg in "$@"; do
    case "$arg" in
    --version)
        echo "SQLsmith stub"
        exit 0
        ;;
    --seed=*) seed="${arg#--seed=}" ;;
    --dump-all-queries) dump=1 ;;
    esac
done
echo "..." >&2
if [[ "$dump" -eq 1 ]]; then
    head -c 2097152 /dev/zero | tr '\\0' 'x'
    echo
    echo "select $seed as last_query;"
fi
"""


def test_a_failed_run_keeps_the_last_queries_of_each_instance_and_says_how_to_replay_it(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "sqlsmith"
    stub.write_text(SQLSMITH)
    stub.chmod(0o755)
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
    junit = tmp_path / "junit"
    env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "SDB_DRV_HOST": "127.0.0.1",
           "SDB_DRV_PORT": str(port), "SDB_DRV_JUNIT": str(junit), "SDB_FUZZ_SEED": "100",
           "SDB_FUZZ_PARALLEL": "2", "SDB_FUZZ_QUERIES": "7"}
    res = subprocess.run(["bash", str(RUNNER)], env=env, capture_output=True, text=True,
                         timeout=120)
    assert res.returncode == 1, res.stderr
    assert "FAIL: server is no longer reachable" in res.stderr
    assert "SQLsmith stub" in res.stdout.splitlines()[0]
    for instance in range(2):
        queries = (junit / f"tests-drivers-sqlsmith-{instance}.sql").read_text()
        assert len(queries) == 1 << 20
        assert queries.endswith(f"select {100 + instance} as last_query;\n")
    assert "with SQLsmith stub" in res.stderr
    assert ("SDB_FUZZ_SEED=100 SDB_FUZZ_PARALLEL=2 SDB_FUZZ_QUERIES=7 "
            "bash tests/drivers/sqlsmith/run.sh") in res.stderr
