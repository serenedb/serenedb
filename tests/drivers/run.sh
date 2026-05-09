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
	[lang]='python,java,js,go,rust,php,csharp,c,ruby,r'
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
	[driver]=SDB_DRV_DRIVER
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

for lang in "${langs[@]}"; do
	runner="${lang_runner[$lang]:-}"
	if [[ -z "$runner" ]] || [[ ! -x "$runner" ]]; then
		echo "[drivers] WARN: no runner for $lang ($runner)" >&2
		continue
	fi
	(
		cd "$(dirname "$runner")"
		exec "$runner"
	) &
	pids+=("$!")
	pid_lang[$!]="$lang"
done

for pid in "${pids[@]}"; do
	if ! wait "$pid"; then
		echo "[drivers] ${pid_lang[$pid]} FAILED" >&2
		final_exit=1
	fi
done

exit $final_exit
