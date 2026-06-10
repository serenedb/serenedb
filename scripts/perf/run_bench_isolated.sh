#!/usr/bin/env bash
# Run bench_wire_old_vs_new.sh on an EXCLUSIVE set of CPUs, so co-tenant load on
# this shared box can't pollute the numbers. Reserves a cgroup-v2 cpuset "root
# partition" (the kernel migrates every other task off these cores), runs the
# bench as the invoking user inside it, then relinquishes the cores on exit.
#
# Usage:  sudo bash scripts/perf/run_bench_isolated.sh
#         sudo BENCH_CORES=8-31 bash scripts/perf/run_bench_isolated.sh
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
# Forward PERF_* tuning vars (e.g. PERF_WIRE_CPU_THREADS) through to the
# unprivileged bench -- the inner sudo otherwise drops everything but PATH/HOME.
fwd=()
for v in $(compgen -e | grep -E '^PERF_'); do fwd+=("$v=${!v}"); done
sudo -u "$RUN_USER" --preserve-env=PATH,HOME "${fwd[@]}" bash "$ROOT/scripts/perf/bench_wire_old_vs_new.sh"
# trap cleanup releases the partition
