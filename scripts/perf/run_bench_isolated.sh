#!/usr/bin/env bash
# Run bench_wire_old_vs_new.sh on an EXCLUSIVE set of CPUs, so co-tenant load on
# this shared box can't pollute the numbers. Reserves a cgroup-v2 cpuset "root
# partition" (the kernel migrates every other task off these cores), runs the
# bench as the invoking user inside it, then relinquishes the cores on exit.
#
# Usage:  sudo bash scripts/perf/run_bench_isolated.sh
#         sudo BENCH_CORES=8-31 bash scripts/perf/run_bench_isolated.sh
#         sudo PERF_DB=all bash scripts/perf/run_bench_isolated.sh
#         sudo PERF_DB=pg,crdb PERF_REMEASURE_DB=crdb bash scripts/perf/run_bench_isolated.sh
# PERF_DB selects which external pg-wire engines to include as columns (comma list
# of pg/crdb/cedar/risingwave/clickhouse, or "all"); PERF_REMEASURE_DB refreshes
# their cached baselines. Default (no PERF_DB) is the plain old-vs-new run.
set -euo pipefail

# Reserve WHOLE physical cores (both SMT siblings) -- otherwise a co-tenant on the
# sibling thread shares our core's execution units and pollutes the measurement.
# BENCH_PHYS_CORES = how many physical cores to take (highest-numbered ones, so
# cpu0's housekeeping stays with the rest of the system). BENCH_CORES overrides
# with an explicit cpuset list.
PHYS="${BENCH_PHYS_CORES:-8}"
if [[ -n "${BENCH_CORES:-}" ]]; then
	CORES="${BENCH_CORES}"
else
	mapfile -t _coresets < <(cat /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list | sort -t, -k1,1n -u)
	if ((PHYS > ${#_coresets[@]})); then
		echo "asked for $PHYS physical cores but only ${#_coresets[@]} exist" >&2
		exit 1
	fi
	_sel=("${_coresets[@]: -$PHYS}")
	CORES="$(
		IFS=,
		echo "${_sel[*]}"
	)"
	echo "reserving $PHYS whole physical cores -> siblings: ${_sel[*]}"
fi
CG=/sys/fs/cgroup/wirebench
ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
RUN_USER="${SUDO_USER:-$(id -un)}"

if [[ $EUID -ne 0 ]]; then
	echo "must run as root: sudo bash $0" >&2
	exit 1
fi

# Forward PERF_* tuning vars through the inner sudo (it otherwise drops everything
# but PATH/HOME). Built once, reused for the engine seeding and the main bench.
fwd=()
for v in $(compgen -e | grep -E '^PERF_'); do fwd+=("$v=${!v}"); done

# All external pg-wire engines run in docker and speak the Postgres wire protocol,
# so they share ONE selection: PERF_DB picks which to include (comma list or "all").
# They CANNOT schedule inside the exclusive partition -- so each selected engine is
# measured + cached OUTSIDE, pinned to the reserved cores, and the in-partition run
# below reuses the cache. Keep this list in sync with EXT_ORDER in the bench script.
ALL_ENGINES=(pg crdb cedar risingwave clickhouse)
BASELINES="${ROOT}/scripts/perf/results/baselines"

# Resolve PERF_DB (+ the legacy PERF_PG=1) into a list of engine names.
DB_RAW="${PERF_DB:-}"
[[ "${PERF_PG:-0}" == 1 ]] && DB_RAW="${DB_RAW:+${DB_RAW},}pg"
SELECTED=()
IFS=',' read -r -a _req <<<"${DB_RAW}"
for _e in "${_req[@]}"; do
	[[ -z "${_e}" ]] && continue
	if [[ "${_e}" == all ]]; then
		SELECTED=("${ALL_ENGINES[@]}")
		break
	fi
	SELECTED+=("${_e}")
done

# remeasure check for one engine: PERF_REMEASURE_DB list (or "all"), plus the legacy
# PERF_REMEASURE_PG=1. Returns 0 when the engine should be refreshed.
remeasure_wanted() {
	local eng="$1" item
	[[ "${eng}" == pg && "${PERF_REMEASURE_PG:-0}" == 1 ]] && return 0
	IFS=',' read -r -a _rm <<<"${PERF_REMEASURE_DB:-}"
	for item in "${_rm[@]}"; do
		[[ "${item}" == all || "${item}" == "${eng}" ]] && return 0
	done
	return 1
}

# Seed each selected engine's baseline BEFORE reserving the partition. Only runs
# when its cache is missing or a refresh is asked for.
for eng in "${SELECTED[@]}"; do
	cache="${BASELINES}/${eng}.tsv"
	if [[ -s "${cache}" ]] && ! remeasure_wanted "${eng}"; then
		continue
	fi
	echo "seeding ${eng} baseline (docker, outside the partition, pinned to ${CORES})"
	# -H sets HOME to the run user's home so the docker client reads the user's
	# ~/.docker/config.json, not root's (which it can't, hence the prior warning).
	sudo -H -u "$RUN_USER" --preserve-env=PATH "${fwd[@]}" \
		PERF_SEED_ONLY="${eng}" PERF_BENCH_CORES="${CORES}" \
		bash "$ROOT/scripts/perf/bench_wire_old_vs_new.sh" ||
		echo "${eng} seeding failed; continuing without a ${eng} column" >&2
done

cleanup() {
	echo $$ >/sys/fs/cgroup/cgroup.procs 2>/dev/null || true
	if [[ -d "$CG" ]]; then
		echo member >"$CG/cpuset.cpus.partition" 2>/dev/null || true
		rmdir "$CG" 2>/dev/null || true
	fi
	echo "released reserved cores"
}
trap cleanup EXIT

mkdir -p "$CG"
echo "$CORES" >"$CG/cpuset.cpus"
cat /sys/devices/system/node/online >"$CG/cpuset.mems"
# Claim the cores exclusively then promote to a root partition; the kernel then
# removes these cpus from every other cgroup's effective set.
echo "$CORES" >"$CG/cpuset.cpus.exclusive" 2>/dev/null || true
echo root >"$CG/cpuset.cpus.partition"

state="$(cat "$CG/cpuset.cpus.partition")"
echo "partition: '${state}'   effective cpus: $(cat "$CG/cpuset.cpus.effective")"
if [[ "$state" != "root" ]]; then
	echo "ERROR: partition is '${state}', not exclusive -- aborting (cores not reserved)" >&2
	exit 1
fi

# Put this shell into the cgroup; the bench and its children (serened, pgbench,
# perf) inherit it, so they all run only on the reserved cores.
echo $$ >"$CG/cgroup.procs"
echo "reserved cores ${CORES}; running bench as ${RUN_USER}"
# PERF_REUSE_ONLY_DB=all: every external engine was already measured OUTSIDE the
# partition (seeded above); the main bench must NOT re-measure any here -- a
# container inside the exclusive partition is denied the reserved cores and would
# produce a bogus number. So always reuse the caches, even if a refresh was
# requested (the seeding already honored it). reuse-only is a no-op for engines
# PERF_DB didn't select, so passing "all" is safe.
sudo -H -u "$RUN_USER" --preserve-env=PATH "${fwd[@]}" \
	PERF_BENCH_CORES="${CORES}" PERF_REUSE_ONLY_DB=all \
	bash "$ROOT/scripts/perf/bench_wire_old_vs_new.sh"
# trap cleanup releases the partition
