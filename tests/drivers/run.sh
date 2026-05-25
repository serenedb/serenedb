#!/bin/bash

# Driver-test runner.
#
# Runs against any reachable PG endpoint. Two modes:
#
#   * Local debug: developer starts serened by hand
#       ninja serened
#       ./build/bin/serened /tmp/datadir --server.endpoint pgsql+tcp://0.0.0.0:5432 --server.authentication 0
#       tests/drivers/run.sh --lang python
#
#   * Docker (CI):
#       tests/drivers/run_in_docker.sh
#     which brings up serened in compose and execs this script with
#     --host serenedb-single.
#
# Mirrors the shape of tests/sqllogic/run.sh on purpose so contributors
# transfer their knowledge.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." &>/dev/null && pwd)

declare -A defaults=(
	[host]='localhost'
	[port]='5432'
	[database]='postgres'
	[user]='postgres'
	[lang]='python,java,js,go,rust,php,csharp,c,ruby,r,sqlsmith'
	[driver]=''
	[protocols]='simple,extended-noparam,extended-text,extended-binary'
	[types]='.*'
	[junit]="${SCRIPT_DIR}/../../out/drivers-tests"
	[jobs]=$(nproc)
	[debug]=false
	[run_id]=''
	[repro]=''
)

usage() {
	cat <<-EOF
		Usage: $0 [OPTIONS]

		Connection:
		  --host HOST          PG host (default ${defaults[host]})
		  --port PORT          PG port (default ${defaults[port]})
		  --database NAME      database (default ${defaults[database]})
		  --user NAME          PG user (default ${defaults[user]})

		Selection:
		  --lang LIST          comma list: python,java,js,go,rust,php,csharp,c,ruby,r
		                       (default: all)
		  --driver LIST        comma list of <lang>_<driver> filters,
		                       e.g. python_psycopg3,go_pgx
		  --protocols LIST     simple,extended-noparam,extended-text,extended-binary
		  --types REGEX        only types whose 'name' matches (default .*)

		Output:
		  --junit DIR          JUnit XML output dir (default ${defaults[junit]})
		  --jobs N             parallel jobs across languages (default nproc)

		Other:
		  --debug              verbose, slow, no JUnit aggregation
		  --run-id ID          schema suffix; defaults to a random hex
		  --repro DIR          run a reproducer in tests/drivers/repro/<DIR>
		                       instead of the type matrix
	EOF
}

# Parse --key value / --key=value.
declare -A args=()
while [ $# -gt 0 ]; do
	opt="$1"
	case "$opt" in
	-h | --help)
		usage
		exit 0
		;;
	--debug)
		args[debug]=true
		shift
		;;
	--debug=*)
		args[debug]="${opt#*=}"
		shift
		;;
	--*=*)
		key="${opt%%=*}"
		key="${key#--}"
		key="${key//-/_}"
		args[$key]="${opt#*=}"
		shift
		;;
	--*)
		key="${opt#--}"
		key="${key//-/_}"
		if [ $# -lt 2 ]; then
			echo "Missing value for --${opt#--}" >&2
			exit 2
		fi
		args[$key]="$2"
		shift 2
		;;
	*)
		echo "Unexpected positional argument: $opt" >&2
		usage >&2
		exit 2
		;;
	esac
done

# If the caller has not passed --foo and there is already a SDB_DRV_FOO set
# in env, prefer the env value over the script default. This makes
# `SDB_DRV_TYPES=... tests/drivers/run.sh ...` work the way operators expect.
declare -A env_aliases=(
	[host]=SDB_DRV_HOST [port]=SDB_DRV_PORT [database]=SDB_DRV_DATABASE
	[user]=SDB_DRV_USER [protocols]=SDB_DRV_PROTOCOLS [types]=SDB_DRV_TYPES
	[junit]=SDB_DRV_JUNIT [run_id]=SDB_DRV_RUN_ID [debug]=SDB_DRV_DEBUG
	[driver]=SDB_DRV_DRIVER [lang]=SDB_DRV_LANG
)
for k in "${!defaults[@]}"; do
	if [[ -z "${args[$k]:-}" ]]; then
		alias="${env_aliases[$k]:-}"
		if [[ -n "$alias" && -n "${!alias:-}" ]]; then
			args[$k]="${!alias}"
		else
			args[$k]="${defaults[$k]}"
		fi
	fi
done

if [[ -z "${args[run_id]}" ]]; then
	args[run_id]="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 8)"
fi

mkdir -p "${args[junit]}"

export SDB_DRV_HOST="${args[host]}"
export SDB_DRV_PORT="${args[port]}"
export SDB_DRV_DATABASE="${args[database]}"
export SDB_DRV_USER="${args[user]}"
export SDB_DRV_PROTOCOLS="${args[protocols]}"
export SDB_DRV_TYPES="${args[types]}"
export SDB_DRV_JUNIT="${args[junit]}"
export SDB_DRV_RUN_ID="${args[run_id]}"
export SDB_DRV_DEBUG="${args[debug]}"
export SDB_DRV_DRIVER="${args[driver]}"
export SDB_DRV_SPEC="${SCRIPT_DIR}/spec"

# Silence per-tool first-run / update-check banners so the suite output
# is just test results. Each is a no-op when the driver tool isn't used.
export DOTNET_NOLOGO=1               # "Welcome to .NET 8.0!" banner
export DOTNET_CLI_TELEMETRY_OPTOUT=1 # telemetry first-run prompt
export NO_UPDATE_NOTIFIER=1          # "npm notice New major version..."
export NPM_CONFIG_UPDATE_NOTIFIER=false
export NPM_CONFIG_FUND=false
export NPM_CONFIG_AUDIT=false
export CARGO_TERM_QUIET=true # cargo's "Downloading" + "Compiling"
export PIP_DISABLE_PIP_VERSION_CHECK=1

echo "[drivers] host=${args[host]} port=${args[port]} db=${args[database]} run_id=${args[run_id]}"
echo "[drivers] languages=${args[lang]} protocols=${args[protocols]} types=${args[types]}"

# --repro is a stand-alone flow: just exec the directory's runme.sh.
if [[ -n "${args[repro]}" ]]; then
	repro_dir="${SCRIPT_DIR}/repro/${args[repro]}"
	if [[ ! -d "$repro_dir" ]]; then
		echo "Reproducer not found: $repro_dir" >&2
		exit 2
	fi
	exec "$repro_dir/runme.sh"
fi

# Map language -> driver script we launch. Each script consumes the SDB_DRV_*
# environment block and emits one JUnit XML per (driver, protocol) pair.
declare -A lang_runner=(
	[python]="${SCRIPT_DIR}/python/run.sh"
	[java]="${SCRIPT_DIR}/java/run.sh"
	[js]="${SCRIPT_DIR}/js/run.sh"
	[go]="${SCRIPT_DIR}/go/run.sh"
	[rust]="${SCRIPT_DIR}/rust/run.sh"
	[php]="${SCRIPT_DIR}/php/run.sh"
	[csharp]="${SCRIPT_DIR}/csharp/run.sh"
	[c]="${SCRIPT_DIR}/c/run.sh"
	[ruby]="${SCRIPT_DIR}/ruby/run.sh"
	[r]="${SCRIPT_DIR}/r/run.sh"
	[sqlsmith]="${SCRIPT_DIR}/sqlsmith/run.sh"
)

IFS=',' read -ra langs <<<"${args[lang]}"

# Wait for serened to accept connections. Skipped in --debug so a developer
# investigating a startup hang can attach a debugger without the loop racing.
if [[ "${args[debug]}" != "true" ]]; then
	echo -n "[drivers] waiting for ${args[host]}:${args[port]} "
	for i in $(seq 1 60); do
		if bash -c "echo > /dev/tcp/${args[host]}/${args[port]}" 2>/dev/null; then
			echo "ok"
			break
		fi
		echo -n "."
		sleep 1
		if [[ $i -eq 60 ]]; then
			echo " timed out"
			exit 1
		fi
	done
fi

final_exit=0
pids=()
declare -A pid_lang=()
declare -A pid_log=()
declare -A pid_started=()
# Where to buffer per-lang output. Each runner's stdout/stderr is captured
# to its own tempfile so we can render the final log as contiguous blocks
# (BEGIN ... END), eliminating the parallel-interleaving that makes a
# multi-driver run hard to read live.
buffer_dir="$(mktemp -d -t drv-XXXXXX)"

for lang in "${langs[@]}"; do
	runner="${lang_runner[$lang]:-}"
	if [[ -z "$runner" ]] || [[ ! -x "$runner" ]]; then
		echo "[drivers] WARN: no runner for $lang ($runner)" >&2
		continue
	fi
	log="$buffer_dir/$lang.log"
	(
		cd "$(dirname "$runner")"
		"$runner" 2>&1
	) >"$log" 2>&1 &
	pids+=("$!")
	pid_lang[$!]="$lang"
	pid_log[$!]="$log"
	pid_started[$!]="$(date +%s)"
done

# Render each lang as a contiguous block as soon as its background process
# exits. Wall time is still bounded by the slowest lang (everything runs in
# parallel); only the *printing* is serialized so output stays readable.
declare -A lang_rc=()
declare -A lang_secs=()
remaining=("${pids[@]}")
while [[ ${#remaining[@]} -gt 0 ]]; do
	# Wait for any one background process; bash's wait -n returns its exit
	# code so we don't need to check pid status afterwards.
	finished=()
	for pid in "${remaining[@]}"; do
		if ! kill -0 "$pid" 2>/dev/null; then
			finished+=("$pid")
		fi
	done
	if [[ ${#finished[@]} -eq 0 ]]; then
		# No-one done yet -- block on any.
		wait -n "${remaining[@]}" 2>/dev/null || true
		continue
	fi
	for pid in "${finished[@]}"; do
		lang="${pid_lang[$pid]}"
		log="${pid_log[$pid]}"
		rc=0
		wait "$pid" 2>/dev/null || rc=$?
		secs=$(($(date +%s) - pid_started[$pid]))
		lang_rc[$lang]=$rc
		lang_secs[$lang]=$secs
		status=$([[ $rc -eq 0 ]] && echo PASS || echo "FAIL rc=$rc")
		printf '\n===== [%s] BEGIN =====\n' "$lang"
		cat "$log"
		printf '===== [%s] END (%s, %ss) =====\n' "$lang" "$status" "$secs"
		rm -f "$log"
		if [[ $rc -ne 0 ]]; then
			final_exit=1
		fi
		# Drop pid from the remaining list.
		new_remaining=()
		for p in "${remaining[@]}"; do
			[[ "$p" != "$pid" ]] && new_remaining+=("$p")
		done
		remaining=("${new_remaining[@]}")
	done
done

# Compact summary table at the end so the operator doesn't have to scroll
# back through each block. Test counts come from the junit XMLs each runner
# produced; some emitters (mvn -q, go-junit-report, vitest --reporter=junit)
# print nothing to stdout, so the table is the only place to see the tally.
echo
echo "===== [drivers] SUMMARY ====="
printf '  %-10s %5s %5s %5s %5s  %s\n' lang tests fails errs secs status

# Map lang -> junit file glob. Some langs split across multiple files
# (java -> pgjdbc + r2dbc; js -> pg + postgres-js; python -> 3 drivers).
declare -A lang_junit_glob=(
	[c]="$SDB_DRV_JUNIT/tests-drivers-c-junit.xml"
	[csharp]="$SDB_DRV_JUNIT/tests-drivers-csharp-junit.xml"
	[go]="$SDB_DRV_JUNIT/tests-drivers-go-junit.xml"
	[java]="$SDB_DRV_JUNIT/TEST-*.xml"
	[js]="$SDB_DRV_JUNIT/tests-drivers-js-*junit.xml"
	[php]="$SDB_DRV_JUNIT/tests-drivers-php-junit.xml"
	[python]="$SDB_DRV_JUNIT/tests-drivers-python-*-junit.xml"
	[r]="$SDB_DRV_JUNIT/tests-drivers-r-junit.xml"
	[ruby]="$SDB_DRV_JUNIT/tests-drivers-ruby-junit.xml"
	[rust]="$SDB_DRV_JUNIT/tests-drivers-rust-junit.xml"
)

# Count tests/failures/errors across one or more junit files. Detection
# order (per file):
#   1. <testsuites tests="N">  -- plural wrapper with summary attr
#      (go-junit-report, vitest emit this with the true total).
#   2. first <testsuite tests="N">  -- singular root
#      (java/maven-surefire emit only this; PHPUnit/csharp/pytest emit a
#      <testsuites> wrapper *without* attrs, then nested <testsuite> roots
#      with the correct count -- summing every <testsuite> would inflate
#      the total because the inner per-class entries restate the same N).
#   3. count <testcase> elements as a last resort (c/r/ruby junits omit
#      every summary attribute).
count_junit_glob() {
	local glob="$1"
	local total_t=0 total_f=0 total_e=0
	shopt -s nullglob
	# shellcheck disable=SC2206 -- intentional word-split on glob.
	local files=($glob)
	shopt -u nullglob
	local f t fl er line
	for f in "${files[@]}"; do
		t=""
		fl=""
		er=""
		# Pass 1: <testsuites> (plural) wrapper.
		while IFS= read -r line; do
			if [[ "$line" =~ tests=\"([0-9]+)\" ]]; then
				t=${BASH_REMATCH[1]}
				fl=0
				er=0
				[[ "$line" =~ failures=\"([0-9]+)\" ]] && fl=${BASH_REMATCH[1]}
				[[ "$line" =~ errors=\"([0-9]+)\" ]] && er=${BASH_REMATCH[1]}
				break
			fi
		done < <(grep -hE '<testsuites\b' "$f" 2>/dev/null)
		# Pass 2: first <testsuite> (singular) root.
		if [[ -z "$t" ]]; then
			while IFS= read -r line; do
				if [[ "$line" =~ tests=\"([0-9]+)\" ]]; then
					t=${BASH_REMATCH[1]}
					fl=0
					er=0
					[[ "$line" =~ failures=\"([0-9]+)\" ]] && fl=${BASH_REMATCH[1]}
					[[ "$line" =~ errors=\"([0-9]+)\" ]] && er=${BASH_REMATCH[1]}
					break
				fi
			done < <(grep -hE '<testsuite[ />]' "$f" 2>/dev/null)
		fi
		# Pass 3: count <testcase> elements.
		if [[ -z "$t" ]]; then
			t=$(grep -c '<testcase\b' "$f" 2>/dev/null)
			t=${t:-0}
			fl=$(grep -c '<failure\b' "$f" 2>/dev/null)
			fl=${fl:-0}
			er=$(grep -c '<error\b' "$f" 2>/dev/null)
			er=${er:-0}
		fi
		total_t=$((total_t + t))
		total_f=$((total_f + fl))
		total_e=$((total_e + er))
	done
	echo "$total_t $total_f $total_e"
}

# Rust falls back to a plain .log when cargo2junit isn't installed; parse the
# "test result: ok. N passed; M failed" lines and sum.
count_rust_log() {
	local log="$1"
	[[ -f "$log" ]] || {
		echo "0 0 0"
		return
	}
	local t=0 fl=0
	local line
	while IFS= read -r line; do
		if [[ "$line" =~ test\ result:\ ok\.\ ([0-9]+)\ passed\;\ ([0-9]+)\ failed ]]; then
			t=$((t + ${BASH_REMATCH[1]}))
			fl=$((fl + ${BASH_REMATCH[2]}))
		elif [[ "$line" =~ test\ result:\ FAILED\.\ ([0-9]+)\ passed\;\ ([0-9]+)\ failed ]]; then
			t=$((t + ${BASH_REMATCH[1]}))
			fl=$((fl + ${BASH_REMATCH[2]}))
		fi
	done <"$log"
	echo "$t $fl 0"
}

for lang in "${langs[@]}"; do
	# Skip langs that had no runner (warn was already printed above).
	[[ -z "${lang_rc[$lang]:-}" ]] && continue
	rc="${lang_rc[$lang]}"
	secs="${lang_secs[$lang]}"
	status=$([[ $rc -eq 0 ]] && echo PASS || echo FAIL)
	tests=- fails=- errs=-
	if [[ -n "${lang_junit_glob[$lang]:-}" ]]; then
		read -r tests fails errs <<<"$(count_junit_glob "${lang_junit_glob[$lang]}")"
	fi
	# Rust may have emitted a .log instead of junit when cargo2junit isn't
	# present in the build image; pick up the count from there.
	if [[ "$lang" == "rust" && "${tests:-0}" -eq 0 ]]; then
		read -r tests fails errs <<<"$(count_rust_log "$SDB_DRV_JUNIT/tests-drivers-rust.log")"
	fi
	printf '  %-10s %5s %5s %5s %4ss  %s\n' "$lang" "$tests" "$fails" "$errs" "$secs" "$status"
done

rmdir "$buffer_dir" 2>/dev/null || true
exit $final_exit
