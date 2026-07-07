#!/usr/bin/env bash
# Shared launcher for the iresearch-columnstore perf benchmarks: run a serened
# either as OUR host-built binary or as the released serenedb/serenedb:latest
# image, under MATCHED docker + network conditions so the two are comparable.
#
# Why the conditions must match (not just "ours bare vs old in docker"):
#   * docker adds ~5-10% on CPU-intensive work, so to isolate the columnstore
#     rewrite from container overhead our binary must be runnable in docker too
#     (SENG_DOCKER=1) -- bind-mounted at its HOST path into a bare base image,
#     exactly like bench_wire_old_vs_new.sh's start_serened.
#   * host vs published-port networking changes per-statement connection cost;
#     both engines therefore share one SENG_HOSTNET toggle.
#
# The datadir and any input files (parquet, attached .duckdb) are bind-mounted
# at their HOST paths so (a) ATTACH/read_parquet resolve inside the container and
# (b) on-disk size is measured on the host afterwards.
#
# Config (env; sensible defaults):
#   SENG_OURS_BIN    our serened binary                 (build_perf/bin/serened)
#   SENG_OLD_IMAGE   released image                     (serenedb/serenedb:latest)
#   SENG_BASE_IMAGE  base image hosting our binary       (ubuntu:24.04)
#   SENG_DOCKER      1 = run OUR binary in docker too    (1)
#   SENG_HOSTNET     1 = --network host, 0 = -p port     (1)
#   SENG_CPUSET      --cpuset-cpus for docker engines    (unset)
#   SENG_OLD_ARGV    argv the old image runs             ("serened <datadir> --listen postgres://0.0.0.0:<port>")
#                    override if the released entrypoint differs; @DATA@/@PORT@
#                    are substituted.
#   SENG_EXTRA_ARGS  extra serened CLI args for OUR binary (unset)
#
# Functions (source this file):
#   seng_up <engine> <port> <host_datadir> [mount_path...]   engine = ours|old
#   seng_down
#   seng_wait <port>
#   seng_conn <port>            -> psql conninfo string
#   seng_datadir_bytes <dir>    -> total bytes on host (works for both engines)

: "${SENG_OURS_BIN:=${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)}/build_perf/bin/serened}"
: "${SENG_OLD_IMAGE:=serenedb/serenedb:latest}"
: "${SENG_BASE_IMAGE:=ubuntu:24.04}"
: "${SENG_DOCKER:=1}"
: "${SENG_HOSTNET:=1}"
: "${SENG_CPUSET:=}"
: "${SENG_OLD_ARGV:=@DATA@ --listen postgres://0.0.0.0:@PORT@}"
: "${SENG_EXTRA_ARGS:=}"

SENG_CONTAINER=""
SENG_PID=""
SENG_NAME="${SENG_NAME:-cs_bench_serened}"

seng__netargs() { # $1=port -> echoes docker network args
	if [[ "${SENG_HOSTNET}" != 0 ]]; then
		printf -- '--network host'
	else
		printf -- '-p %s:%s' "$1" "$1"
	fi
}

# Start `engine` on `port` over `host_datadir`. Remaining args are extra host
# paths to bind-mount read-only into the container (parquet dir, native .duckdb
# dir) so in-container ATTACH/read_parquet resolve at their host paths.
seng_up() {
	local engine="$1" port="$2" datadir="$3"
	shift 3
	local mounts=()
	local p
	for p in "$@"; do mounts+=(-v "${p}:${p}"); done
	# shellcheck disable=SC2046
	local netargs
	read -r -a netargs <<<"$(seng__netargs "${port}")"
	local cpuset=()
	[[ -n "${SENG_CPUSET}" ]] && cpuset=(--cpuset-cpus "${SENG_CPUSET}")

	docker rm -f "${SENG_NAME}" >/dev/null 2>&1 || true
	SENG_CONTAINER=""
	SENG_PID=""

	case "${engine}" in
	ours)
		local cmd=("${SENG_OURS_BIN}" "${datadir}"
			--listen "postgres://0.0.0.0:${port}")
		[[ -n "${SENG_EXTRA_ARGS}" ]] && cmd+=(${SENG_EXTRA_ARGS})
		if [[ "${SENG_DOCKER}" != 0 ]]; then
			docker run -d --name "${SENG_NAME}" "${cpuset[@]}" \
				--user "$(id -u):$(id -g)" -e HOME=/tmp \
				"${netargs[@]}" \
				-v "${SENG_OURS_BIN}:${SENG_OURS_BIN}:ro" \
				-v "${datadir}:${datadir}" \
				"${mounts[@]}" \
				"${SENG_BASE_IMAGE}" "${cmd[@]}" >/dev/null
			SENG_CONTAINER="${SENG_NAME}"
		else
			"${cmd[@]}" >"/tmp/${USER}-cs-bench-ours.log" 2>&1 &
			SENG_PID=$!
		fi
		;;
	old)
		local argv="${SENG_OLD_ARGV//@DATA@/${datadir}}"
		argv="${argv//@PORT@/${port}}"
		docker run -d --name "${SENG_NAME}" "${cpuset[@]}" \
			--user "$(id -u):$(id -g)" -e HOME=/tmp \
			"${netargs[@]}" \
			-v "${datadir}:${datadir}" \
			"${mounts[@]}" \
			--entrypoint serened \
			"${SENG_OLD_IMAGE}" ${argv} >/dev/null
		SENG_CONTAINER="${SENG_NAME}"
		;;
	*)
		echo "seng_up: unknown engine '${engine}' (want ours|old)" >&2
		return 2
		;;
	esac
	seng_wait "${port}"
}

seng_down() {
	if [[ -n "${SENG_CONTAINER}" ]]; then
		docker logs "${SENG_CONTAINER}" >"/tmp/${USER}-cs-bench-${SENG_CONTAINER}.log" 2>&1 || true
		docker rm -f "${SENG_CONTAINER}" >/dev/null 2>&1 || true
		SENG_CONTAINER=""
	elif [[ -n "${SENG_PID}" ]]; then
		kill -9 "${SENG_PID}" 2>/dev/null || true
		wait "${SENG_PID}" 2>/dev/null || true
		SENG_PID=""
	fi
}

seng_conn() { printf 'postgres://postgres@localhost:%s/postgres' "$1"; }

seng_wait() { # $1=port
	local conn
	conn="$(seng_conn "$1")"
	local _
	for _ in $(seq 1 120); do
		psql "${conn}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "serened on :$1 never became ready" >&2
	[[ -n "${SENG_CONTAINER}" ]] && docker logs "${SENG_CONTAINER}" 2>&1 | tail -40 >&2
	return 1
}

# Total bytes of a host path. The datadir is bind-mounted for docker engines, so
# the same du works whether the writer ran in-container or bare.
seng_datadir_bytes() { du -sb "$1" 2>/dev/null | awk '{print $1+0}'; }
