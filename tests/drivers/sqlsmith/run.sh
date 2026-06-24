#!/bin/bash
# SQLsmith fuzz harness against serened over PG wire.
#
# Local debug:
#   ninja serened
#   ./build/bin/serened /tmp/datadir --listen postgres://0.0.0.0:5432
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
#   SDB_FUZZ_QUERIES  total queries to generate per sqlsmith instance
#                     (--max-queries). Default 5000.
#   SDB_FUZZ_PARALLEL number of concurrent sqlsmith instances (default 2).
#                     Each gets a distinct seed derived from SDB_FUZZ_SEED.
#                     Total query budget = QUERIES * PARALLEL.
#   SDB_FUZZ_SEED     fixed RNG base seed for repro; defaults to a random int.
#                     Instance i uses SEED+i so reproducing requires both
#                     the base seed and the instance index.
#   SDB_FUZZ_LOG_TO   optional libpq DSN of a separate PG instance where sqlsmith
#                     will write structured query+error logs (--log-to). Must
#                     already have sqlsmith's log.sql schema applied. When set,
#                     the harness queries the log DB after the run and prints
#                     a top-20 error histogram.
#   SDB_SERVER_LOG    optional path to serened's stderr log; if set, scanned
#                     for sanitizer reports and assertion failures after the run

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if ! command -v sqlsmith >/dev/null 2>&1; then
	echo "[sqlsmith] sqlsmith not installed; skipping (see $SCRIPT_DIR/README.md)" >&2
	exit 0
fi

HOST="${SDB_DRV_HOST:-localhost}"
PORT="${SDB_DRV_PORT:-7890}"
DB="${SDB_DRV_DATABASE:-postgres}"
DB_USER="${SDB_DRV_USER:-postgres}"
MAX_QUERIES="${SDB_FUZZ_QUERIES:-5000}"
PARALLEL="${SDB_FUZZ_PARALLEL:-2}"
# sqlsmith parses --seed via std::stoi (signed 32-bit int), so keep the value
# under INT_MAX (2^31 - 1). $RANDOM is bash's 15-bit RNG; two of them combined
# with a 15-bit shift give a 30-bit unsigned seed safely below the limit.
SEED="${SDB_FUZZ_SEED:-$(((RANDOM << 15) | RANDOM))}"

if ! [[ "$MAX_QUERIES" =~ ^[0-9]+$ ]] || [[ "$MAX_QUERIES" -le 0 ]]; then
	echo "[sqlsmith] SDB_FUZZ_QUERIES must be a positive integer, got '$MAX_QUERIES'" >&2
	exit 2
fi
if ! [[ "$PARALLEL" =~ ^[0-9]+$ ]] || [[ "$PARALLEL" -le 0 ]]; then
	echo "[sqlsmith] SDB_FUZZ_PARALLEL must be a positive integer, got '$PARALLEL'" >&2
	exit 2
fi

JUNIT="${SDB_DRV_JUNIT:-}"
[[ -n "$JUNIT" ]] && mkdir -p "$JUNIT"

WORK=$(mktemp -d)
SEED_LOG="$WORK/seed.log"

# Per-instance state. With PARALLEL=1 there's a single log at sqlsmith-0.log;
# with N>1 we get sqlsmith-0.log ... sqlsmith-(N-1).log. Instance i uses
# seed (SEED + i) so reproducing a specific instance only needs that seed.
declare -a INSTANCE_SEEDS=()
declare -a INSTANCE_LOGS=()
for ((i = 0; i < PARALLEL; i++)); do
	INSTANCE_SEEDS+=("$((SEED + i))")
	INSTANCE_LOGS+=("$WORK/sqlsmith-${i}.log")
done
TOTAL_BUDGET=$((PARALLEL * MAX_QUERIES))

DSN="host=$HOST port=$PORT dbname=$DB user=$DB_USER"

echo "[sqlsmith] max-queries=$MAX_QUERIES parallel=$PARALLEL base-seed=$SEED dsn='$DSN'"

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

LOG_TO="${SDB_FUZZ_LOG_TO:-}"
log_to_args=()
if [[ -n "$LOG_TO" ]]; then
	log_to_args=(--log-to="$LOG_TO")
fi

start=$(date +%s)
declare -a PIDS=()
for ((i = 0; i < PARALLEL; i++)); do
	sqlsmith \
		--target="$DSN" \
		--max-queries="$MAX_QUERIES" \
		--seed="${INSTANCE_SEEDS[$i]}" \
		--verbose \
		"${log_to_args[@]}" \
		>/dev/null 2>"${INSTANCE_LOGS[$i]}" &
	PIDS+=($!)
done

declare -a INSTANCE_EXITS=()
sqlsmith_exit=0
n_signaled=0
n_nonzero=0
for pid in "${PIDS[@]}"; do
	wait "$pid"
	rc=$?
	INSTANCE_EXITS+=("$rc")
	if [[ "$rc" -ne 0 ]]; then
		((n_nonzero++))
		if [[ "$rc" -gt "$sqlsmith_exit" ]]; then
			sqlsmith_exit=$rc
		fi
		if [[ "$rc" -gt 128 ]]; then
			((n_signaled++))
		fi
	fi
done

elapsed=$(($(date +%s) - start))

# Aggregate symbol counts across all instance logs.
# sqlsmith --verbose prints one symbol per query to stderr.
crashes=0
ok=0
errs=0
syntax=0
timeouts=0
for log in "${INSTANCE_LOGS[@]}"; do
	[[ -f "$log" ]] || continue
	crashes=$((crashes + $(tr -cd 'C' <"$log" | wc -c | tr -d ' ')))
	ok=$((ok + $(tr -cd '.' <"$log" | wc -c | tr -d ' ')))
	errs=$((errs + $(tr -cd 'e' <"$log" | wc -c | tr -d ' ')))
	syntax=$((syntax + $(tr -cd 'S' <"$log" | wc -c | tr -d ' ')))
	timeouts=$((timeouts + $(tr -cd 't' <"$log" | wc -c | tr -d ' ')))
done

# Scan the tail of the server log (added during this run only) for
# sanitizer reports and assertion failures.
server_issues=0
SAN_RE='(AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer|LeakSanitizer|MemorySanitizer|Assertion .* failed|FATAL|SIGSEGV|SIGABRT)'
if [[ -n "${SDB_SERVER_LOG:-}" && -f "$SDB_SERVER_LOG" ]]; then
	if tail -c +"$((SERVER_LOG_START + 1))" "$SDB_SERVER_LOG" | grep -E -q "$SAN_RE"; then
		server_issues=1
		echo "[sqlsmith] sanitizer / assert hit in $SDB_SERVER_LOG:" >&2
		tail -c +"$((SERVER_LOG_START + 1))" "$SDB_SERVER_LOG" |
			grep -E -n "$SAN_RE" |
			head -n 20 |
			sed 's/^/[sqlsmith]   /' >&2
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

# Decode sqlsmith exit statuses across instances. A signal-killed instance
# (exit > 128) is a fuzzer-side crash, distinct from anything the server did.
fuzzer_crashed=0
if [[ "$n_signaled" -gt 0 ]]; then
	fuzzer_crashed=1
fi
sqlsmith_status="$((PARALLEL - n_nonzero))/$PARALLEL ok"
if [[ "$n_signaled" -gt 0 ]]; then
	sqlsmith_status="$sqlsmith_status, $n_signaled signal-killed"
fi
clean_exits=$((n_nonzero - n_signaled))
if [[ "$clean_exits" -gt 0 ]]; then
	sqlsmith_status="$sqlsmith_status, $clean_exits nonzero-exit"
fi

echo "[sqlsmith] done in ${elapsed}s: ok=$ok err=$errs syntax=$syntax timeouts=$timeouts conn_drops=$crashes total=$total_queries/$TOTAL_BUDGET sqlsmith=$sqlsmith_status server_alive=$server_alive"

# When --log-to is in use, post-process the log DB to print the top error
# classes. Requires psql; gracefully skips if unavailable.
if [[ -n "$LOG_TO" ]] && command -v psql >/dev/null 2>&1; then
	echo "[sqlsmith] top 20 error classes from log DB:"
	psql "$LOG_TO" -P pager=off -A -F $'\t' -q -X -c \
		"SELECT count(*) AS n, sqlstate, left(msg, 100) AS msg
		 FROM error
		 GROUP BY sqlstate, left(msg, 100)
		 ORDER BY n DESC
		 LIMIT 20" 2>/dev/null | sed 's/^/[sqlsmith]   /' ||
		echo "[sqlsmith]   (log DB query failed; is the schema applied?)" >&2
fi

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
#   - every instance exited non-zero with zero successful queries across the
#     whole run -> probably catalog introspection broke (a real serened issue).
#   - any instance died by signal -> fuzzer bug. Not a serened bug, but it
#     cuts the run short so we surface it loudly.
preserve_logs=0
if [[ "$n_nonzero" -eq "$PARALLEL" && "$ok" -eq 0 ]]; then
	fail=1
	preserve_logs=1
	echo "[sqlsmith] FAIL: all $PARALLEL sqlsmith instance(s) exited non-zero with no successful queries (catalog introspection issue?)" >&2
	for i in "${!INSTANCE_LOGS[@]}"; do
		echo "[sqlsmith] --- tail of instance $i (exit=${INSTANCE_EXITS[$i]}, seed=${INSTANCE_SEEDS[$i]}) ---" >&2
		tail -n 20 "${INSTANCE_LOGS[$i]}" 2>/dev/null | sed 's/^/[sqlsmith]   /' >&2
	done
elif [[ "$fuzzer_crashed" -ne 0 ]]; then
	preserve_logs=1
	echo "[sqlsmith] WARN: $n_signaled/$PARALLEL sqlsmith instance(s) crashed (signal-killed). Server is healthy; this is a fuzzer-side issue." >&2
	for i in "${!INSTANCE_EXITS[@]}"; do
		rc="${INSTANCE_EXITS[$i]}"
		[[ "$rc" -gt 128 ]] || continue
		echo "[sqlsmith] --- tail of instance $i (signal=$((rc - 128)), seed=${INSTANCE_SEEDS[$i]}) ---" >&2
		tail -n 20 "${INSTANCE_LOGS[$i]}" 2>/dev/null | sed 's/^/[sqlsmith]   /' >&2
	done
fi

# Warn (but don't fail) when actual fuzz depth was far below the total budget.
# Under 10% means instances bailed out early (catalog gap, fuzzer crash, etc.)
# and the run is unlikely to have probed serened deeply.
budget_floor=$((TOTAL_BUDGET / 10))
if [[ "$total_queries" -lt "$budget_floor" ]]; then
	preserve_logs=1
	echo "[sqlsmith] WARN: only $total_queries/$TOTAL_BUDGET queries reached the server (<10%). Fuzzing depth was shallow this run." >&2
fi

if [[ "$fail" -ne 0 ]] || [[ "$preserve_logs" -ne 0 ]]; then
	echo "[sqlsmith] reproduce locally with:" >&2
	if [[ "$PARALLEL" -eq 1 ]]; then
		echo "[sqlsmith]   sqlsmith --target=\"$DSN\" --max-queries=$MAX_QUERIES --seed=${INSTANCE_SEEDS[0]} --verbose" >&2
	else
		echo "[sqlsmith]   any one of (run them all to repro the full session):" >&2
		for s in "${INSTANCE_SEEDS[@]}"; do
			echo "[sqlsmith]     sqlsmith --target=\"$DSN\" --max-queries=$MAX_QUERIES --seed=$s --verbose" >&2
		done
	fi
	if [[ -n "$JUNIT" ]]; then
		for i in "${!INSTANCE_LOGS[@]}"; do
			cp "${INSTANCE_LOGS[$i]}" "$JUNIT/tests-drivers-sqlsmith-${i}.log" 2>/dev/null || true
		done
		cp "$SEED_LOG" "$JUNIT/tests-drivers-sqlsmith-seed.log" 2>/dev/null || true
		echo "[sqlsmith] full sqlsmith logs: $JUNIT/tests-drivers-sqlsmith-*.log" >&2
	else
		echo "[sqlsmith] full sqlsmith logs preserved under: $WORK" >&2
		# Inhibit the final cleanup so the developer can inspect.
		WORK=""
	fi
fi

# Emit a minimal JUnit XML when running under tests/drivers/run.sh.
if [[ -n "$JUNIT" ]]; then
	out="$JUNIT/tests-drivers-sqlsmith-junit.xml"
	case_name="fuzz_p${PARALLEL}_q${MAX_QUERIES}"
	if [[ "$fail" -eq 0 ]]; then
		cat >"$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="sqlsmith" tests="1" failures="0" errors="0" time="$elapsed">
    <testcase name="$case_name" classname="sqlsmith" time="$elapsed"/>
  </testsuite>
</testsuites>
EOF
	else
		msg="crashes=$crashes server_issues=$server_issues parallel=$PARALLEL nonzero=$n_nonzero signaled=$n_signaled base_seed=$SEED"
		cat >"$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="sqlsmith" tests="1" failures="1" errors="0" time="$elapsed">
    <testcase name="$case_name" classname="sqlsmith" time="$elapsed">
      <failure message="${msg}">Reproduce any instance: sqlsmith --target=&quot;${DSN}&quot; --max-queries=${MAX_QUERIES} --seed=&lt;SEED+i for i in [0,${PARALLEL})&gt; --verbose; base SEED=${SEED}</failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
	fi
fi

[[ -n "$WORK" ]] && rm -rf "$WORK"
exit "$fail"
