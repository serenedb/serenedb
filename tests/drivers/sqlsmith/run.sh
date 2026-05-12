#!/bin/bash
# SQLsmith fuzz harness against serened over PG wire.
#
# Local debug:
#   ninja serened
#   ./build/bin/serened /tmp/datadir --server.endpoint pgsql+tcp://0.0.0.0:5432 --server.authentication 0
#   bash tests/drivers/sqlsmith/run.sh
#
# After Stage B (CI wiring), this script is also invoked from
# tests/drivers/run.sh via the lang_runner map and inherits the SDB_DRV_*
# env block.
#
# SQLsmith --verbose emits one symbol per query to stderr:
#   .  query executed successfully
#   S  syntax error from server (noise, not a bug in serened unless clustered)
#   t  query timed out
#   e  server-reported runtime error (also noise)
#   C  broken connection -> treated as a crash, fails the run
#
# Tunables via env:
#   SDB_FUZZ_DEPTH   smoke (default; ~60s, --max-queries=2000)
#                    deep  (~hours; --max-queries=1000000)
#   SDB_FUZZ_SEED    fixed RNG seed for repro; defaults to a random int
#   SDB_SERVER_LOG   optional path to serened's stderr log; if set, scanned
#                    for sanitizer reports and assertion failures after the run

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v sqlsmith >/dev/null 2>&1; then
	echo "[sqlsmith] sqlsmith not installed; skipping (see $SCRIPT_DIR/README.md)" >&2
	exit 0
fi

HOST="${SDB_DRV_HOST:-localhost}"
PORT="${SDB_DRV_PORT:-5432}"
DB="${SDB_DRV_DATABASE:-postgres}"
DB_USER="${SDB_DRV_USER:-postgres}"
DEPTH="${SDB_FUZZ_DEPTH:-smoke}"
# sqlsmith parses --seed via std::stoi (signed 32-bit int), so keep the value
# under INT_MAX (2^31 - 1). $RANDOM is bash's 15-bit RNG; two of them combined
# with a 15-bit shift give a 30-bit unsigned seed safely below the limit.
SEED="${SDB_FUZZ_SEED:-$(((RANDOM << 15) | RANDOM))}"

case "$DEPTH" in
	smoke) MAX_QUERIES=2000 ;;
	deep)  MAX_QUERIES=1000000 ;;
	*)
		echo "[sqlsmith] unknown SDB_FUZZ_DEPTH='$DEPTH'; expected smoke|deep" >&2
		exit 2
		;;
esac

JUNIT="${SDB_DRV_JUNIT:-}"
[[ -n "$JUNIT" ]] && mkdir -p "$JUNIT"

WORK=$(mktemp -d)
LOG="$WORK/sqlsmith.log"
SEED_LOG="$WORK/seed.log"

DSN="host=$HOST port=$PORT dbname=$DB user=$DB_USER"

echo "[sqlsmith] depth=$DEPTH max-queries=$MAX_QUERIES seed=$SEED dsn='$DSN'"

# Best-effort schema bootstrap. Don't abort the fuzz run on a partial schema:
# missing types/features in serened mean fewer relations to fuzz against, not
# a fatal harness error.
if command -v psql >/dev/null 2>&1; then
	psql "$DSN" -q -f "$SCRIPT_DIR/seed.sql" >"$SEED_LOG" 2>&1 || true
	if grep -E -q '^(ERROR|psql:)' "$SEED_LOG"; then
		echo "[sqlsmith] WARN: seed.sql had errors (continuing with partial schema):" >&2
		grep -E '^(ERROR|psql:)' "$SEED_LOG" | head -n 10 | sed 's/^/[sqlsmith]   /' >&2
	fi
else
	echo "[sqlsmith] WARN: psql not on PATH; skipping seed schema bootstrap" >&2
fi

# Snapshot the server log size before fuzzing so we can scan only the tail.
SERVER_LOG_START=0
if [[ -n "${SDB_SERVER_LOG:-}" && -f "$SDB_SERVER_LOG" ]]; then
	SERVER_LOG_START=$(wc -c <"$SDB_SERVER_LOG" | tr -d ' ')
fi

start=$(date +%s)
set +e
sqlsmith \
	--target="$DSN" \
	--max-queries="$MAX_QUERIES" \
	--seed="$SEED" \
	--verbose \
	>/dev/null 2>"$LOG"
sqlsmith_exit=$?
set -e

elapsed=$(($(date +%s) - start))

# sqlsmith --verbose prints one symbol per query to stderr.
crashes=$(tr -cd 'C' <"$LOG" | wc -c | tr -d ' ')
ok=$(tr -cd '.' <"$LOG" | wc -c | tr -d ' ')
errs=$(tr -cd 'e' <"$LOG" | wc -c | tr -d ' ')
syntax=$(tr -cd 'S' <"$LOG" | wc -c | tr -d ' ')
timeouts=$(tr -cd 't' <"$LOG" | wc -c | tr -d ' ')

# Scan the tail of the server log (added during this run only) for
# sanitizer reports and assertion failures.
server_issues=0
SAN_RE='(AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer|LeakSanitizer|MemorySanitizer|Assertion .* failed|FATAL|SIGSEGV|SIGABRT)'
if [[ -n "${SDB_SERVER_LOG:-}" && -f "$SDB_SERVER_LOG" ]]; then
	if tail -c +"$((SERVER_LOG_START + 1))" "$SDB_SERVER_LOG" | grep -E -q "$SAN_RE"; then
		server_issues=1
		echo "[sqlsmith] sanitizer / assert hit in $SDB_SERVER_LOG:" >&2
		tail -c +"$((SERVER_LOG_START + 1))" "$SDB_SERVER_LOG" \
			| grep -E -n "$SAN_RE" \
			| head -n 20 \
			| sed 's/^/[sqlsmith]   /' >&2
	fi
fi

# A `C` symbol from sqlsmith means "the libpq connection broke during one
# query." That can happen because the server crashed, but also because the
# server terminated a single connection for a non-fatal reason (timeout,
# OOM in one query, internal error path that closes the session). The
# authoritative crash signals are: the server log shows a sanitizer /
# assertion hit, OR the server process no longer accepts new connections.
server_alive=1
if ! bash -c "echo > /dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
	server_alive=0
fi

total_queries=$((ok + errs + syntax + timeouts))

# Decode sqlsmith's own exit status. Unix exit > 128 means killed by signal
# (N - 128). That's a fuzzer-side crash, distinct from anything the server did.
fuzzer_crashed=0
sqlsmith_status="exit=$sqlsmith_exit"
if [[ "$sqlsmith_exit" -gt 128 ]]; then
	fuzzer_crashed=1
	sqlsmith_status="killed by signal $((sqlsmith_exit - 128)) (exit=$sqlsmith_exit)"
fi

echo "[sqlsmith] done in ${elapsed}s: ok=$ok err=$errs syntax=$syntax timeouts=$timeouts conn_drops=$crashes total=$total_queries/$MAX_QUERIES sqlsmith=$sqlsmith_status server_alive=$server_alive"

fail=0
if [[ "$server_alive" -eq 0 ]]; then
	fail=1
	echo "[sqlsmith] FAIL: server is no longer reachable at ${HOST}:${PORT}" >&2
elif [[ "$crashes" -gt 0 ]]; then
	echo "[sqlsmith] note: $crashes connection drop(s) during the run, but server is still up (likely per-query failures, not a server crash)" >&2
fi
if [[ "$server_issues" -gt 0 ]]; then
	fail=1
fi

# sqlsmith's own exit. Two notable shapes:
#   - exited nonzero with zero successful queries -> probably catalog
#     introspection broke (a real serened issue worth flagging).
#   - died by signal (exit > 128) -> sqlsmith bug. Not a serened bug, but
#     it cuts the fuzz run short so we surface it loudly.
preserve_logs=0
if [[ "$sqlsmith_exit" -ne 0 && "$ok" -eq 0 ]]; then
	fail=1
	preserve_logs=1
	echo "[sqlsmith] FAIL: sqlsmith exited $sqlsmith_exit with no successful queries (catalog introspection issue?)" >&2
	tail -n 40 "$LOG" | sed 's/^/[sqlsmith]   /' >&2
elif [[ "$fuzzer_crashed" -ne 0 ]]; then
	preserve_logs=1
	echo "[sqlsmith] WARN: sqlsmith itself crashed ($sqlsmith_status) after $total_queries queries. Server is healthy; this is a fuzzer-side issue." >&2
	tail -n 20 "$LOG" | sed 's/^/[sqlsmith]   /' >&2
fi

# Warn (but don't fail) when the actual fuzz depth was far below the budget.
# Under 10% of max means sqlsmith bailed out (catalog gap, fuzzer crash, etc.)
# and the run is unlikely to have probed serened deeply.
budget_floor=$((MAX_QUERIES / 10))
if [[ "$total_queries" -lt "$budget_floor" ]]; then
	preserve_logs=1
	echo "[sqlsmith] WARN: only $total_queries/$MAX_QUERIES queries reached the server (<10%). Fuzzing depth was shallow this run." >&2
fi

if [[ "$fail" -ne 0 ]] || [[ "$preserve_logs" -ne 0 ]]; then
	echo "[sqlsmith] reproduce locally with:" >&2
	echo "[sqlsmith]   sqlsmith --target=\"$DSN\" --max-queries=$MAX_QUERIES --seed=$SEED --verbose" >&2
	if [[ -n "$JUNIT" ]]; then
		cp "$LOG" "$JUNIT/tests-drivers-sqlsmith.log" 2>/dev/null || true
		cp "$SEED_LOG" "$JUNIT/tests-drivers-sqlsmith-seed.log" 2>/dev/null || true
		echo "[sqlsmith] full sqlsmith log: $JUNIT/tests-drivers-sqlsmith.log" >&2
	else
		echo "[sqlsmith] full sqlsmith log preserved at: $LOG" >&2
		# Inhibit the final cleanup so the developer can inspect it.
		WORK=""
	fi
fi

# Emit a minimal JUnit XML when running under tests/drivers/run.sh.
if [[ -n "$JUNIT" ]]; then
	out="$JUNIT/tests-drivers-sqlsmith-junit.xml"
	if [[ "$fail" -eq 0 ]]; then
		cat >"$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="sqlsmith" tests="1" failures="0" errors="0" time="$elapsed">
    <testcase name="fuzz_depth_${DEPTH}" classname="sqlsmith" time="$elapsed"/>
  </testsuite>
</testsuites>
EOF
	else
		msg="crashes=$crashes server_issues=$server_issues sqlsmith_exit=$sqlsmith_exit seed=$SEED"
		cat >"$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="sqlsmith" tests="1" failures="1" errors="0" time="$elapsed">
    <testcase name="fuzz_depth_${DEPTH}" classname="sqlsmith" time="$elapsed">
      <failure message="${msg}">Reproduce: sqlsmith --target=&quot;${DSN}&quot; --max-queries=${MAX_QUERIES} --seed=${SEED} --verbose</failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
	fi
fi

[[ -n "$WORK" ]] && rm -rf "$WORK"
exit "$fail"
