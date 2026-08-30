#!/bin/bash
# Concurrent multi-entry catalog commits must not deadlock.
#
# The cluster catalog log is guarded by one process-wide mutex. A commit takes
# it and holds it for the rest of the transaction, while the per-catalog and
# per-set locks are taken and released per undo entry. Any site that takes the
# WAL mutex while already holding a catalog lock closes a cycle, and two
# concurrent transactions that each commit several catalog entries wedge the
# server for good.
#
# The load below is deliberately plain: CREATE TABLE with a primary key and a
# check constraint puts several entries in one commit, and every worker touches
# the same CatalogSet. Under a ThreadSanitizer build the cycles are also
# reported as lock-order-inversion.
#
# Usage:
#   tests/scripts/catalog_commit_deadlock.sh --port 7975 [--workers 8] [--seconds 40]
#
# Exits non-zero if the server stops answering a trivial query while, or after,
# the load runs.

set -u

PORT=""
WORKERS=8
SECONDS_TO_RUN=40
HOST=127.0.0.1
USER_NAME=postgres
DB=postgres

while [[ $# -gt 0 ]]; do
	case "$1" in
	--port) PORT="$2"; shift 2 ;;
	--workers) WORKERS="$2"; shift 2 ;;
	--seconds) SECONDS_TO_RUN="$2"; shift 2 ;;
	--host) HOST="$2"; shift 2 ;;
	--user) USER_NAME="$2"; shift 2 ;;
	--database) DB="$2"; shift 2 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

if [[ -z "$PORT" ]]; then
	echo "usage: $0 --port <port> [--workers N] [--seconds N]" >&2
	exit 2
fi

# Bounded: against a wedged server an unbounded client never returns, and the
# run has to end with a verdict rather than hang with it.
psql_q() { timeout 20 psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB" -tA "$@"; }

# A wedged server is the failure this looks for, so every probe is bounded.
probe() {
	timeout 15 psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB" -tAc "SELECT 1;" >/dev/null 2>&1
}

if ! probe; then
	echo "FAIL: server at $HOST:$PORT is not answering before the load starts" >&2
	exit 1
fi

echo "hammering $WORKERS workers for ${SECONDS_TO_RUN}s against $HOST:$PORT"

pids=()
for w in $(seq 1 "$WORKERS"); do
	(
		end=$(($(date +%s) + SECONDS_TO_RUN))
		while [[ $(date +%s) -lt $end ]]; do
			psql_q -q -v ON_ERROR_STOP=0 <<-SQL >/dev/null 2>&1
				BEGIN;
				CREATE TABLE IF NOT EXISTS ccd_a_$w (id INT PRIMARY KEY, v INT CHECK (v >= 0));
				CREATE TABLE IF NOT EXISTS ccd_b_$w (id INT PRIMARY KEY, v INT CHECK (v >= 0));
				COMMIT;
				BEGIN;
				DROP TABLE IF EXISTS ccd_a_$w;
				DROP TABLE IF EXISTS ccd_b_$w;
				COMMIT;
			SQL
		done
	) &
	pids+=($!)
done

# Probe while the load runs: a deadlock takes the whole server down, not just
# the workers, so a plain SELECT stops answering.
wedged=0
deadline=$(($(date +%s) + SECONDS_TO_RUN))
while [[ $(date +%s) -lt $deadline ]]; do
	if ! probe; then
		echo "FAIL: server stopped answering while the load was running" >&2
		wedged=1
		break
	fi
	sleep 3
done

for p in "${pids[@]}"; do
	wait "$p" 2>/dev/null
done

if [[ $wedged -eq 0 ]] && ! probe; then
	echo "FAIL: server stopped answering after the load finished" >&2
	wedged=1
fi

for w in $(seq 1 "$WORKERS"); do
	timeout 15 psql_q -q -c "DROP TABLE IF EXISTS ccd_a_$w; DROP TABLE IF EXISTS ccd_b_$w;" \
		>/dev/null 2>&1
done

if [[ $wedged -ne 0 ]]; then
	exit 1
fi

echo "PASS: server stayed responsive"
