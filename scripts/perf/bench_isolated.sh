#!/usr/bin/env bash
set -euo pipefail

whole_cores() {
	local cpu siblings seen=() cores=()
	for ((cpu = 0; cpu < $(nproc --all); ++cpu)); do
		siblings="$(cat "/sys/devices/system/cpu/cpu${cpu}/topology/thread_siblings_list")"
		if [[ " ${seen[*]-} " != *" ${siblings} "* ]]; then
			seen+=("${siblings}")
			cores+=("${siblings}")
		fi
	done
	local half=$((${#cores[@]} / 2)) taken=()
	for siblings in "${cores[@]:half}"; do
		IFS=, read -ra list <<<"${siblings}"
		taken+=("${list[@]}")
	done
	local IFS=,
	echo "${taken[*]}"
}

usage() {
	cat <<EOF
Runs a command on CPUs no other process may use, so its timings are not
disturbed by the rest of the machine.

Usage: sudo $0 [--cpus <list>] -- <command> [args...]

The CPUs become an exclusive cgroup v2 cpuset partition: every other process
on the machine is moved off them while the command runs, and back when it
ends. The command runs as the user who called sudo, with that user's
environment. It also gets:
  SC_SERVER_ARGS="--cpu_threads <cpu count>"   unless already set
  FORCE=1                                      the global load no longer matters
and the CPUs' frequency governor is set to performance while it runs, when
the kernel exposes it.

  --cpus <list>   CPUs to take, as the kernel writes them (default: the upper
                  half of the physical cores with all their hyperthreads,
                  $(whole_cores) here)

Example:
  sudo -E $0 -- scripts/perf/bench_schema_change.sh
EOF
}

CPUS="$(whole_cores)"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--cpus)
		CPUS="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	--)
		shift
		break
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done
[[ $# -gt 0 ]] || {
	usage >&2
	exit 1
}
[[ "${EUID}" -eq 0 ]] || {
	echo "run it with sudo: it changes the machine's cpusets" >&2
	exit 1
}
[[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != root ]] || {
	echo "run it through sudo from your own user, so the command runs as you" >&2
	exit 1
}
CGROOT=/sys/fs/cgroup
grep -qw cpuset "${CGROOT}/cgroup.controllers" || {
	echo "cgroup v2 with the cpuset controller is required" >&2
	exit 1
}

COUNT=0
IFS=, read -ra RANGES <<<"${CPUS}"
CPU_LIST=()
for range in "${RANGES[@]}"; do
	first="${range%-*}"
	last="${range#*-}"
	for ((cpu = first; cpu <= last; ++cpu)); do
		CPU_LIST+=("${cpu}")
		COUNT=$((COUNT + 1))
	done
done

GROUP="${CGROOT}/sdb-bench-$$"
GOVERNORS=()
NARROWED=()

expand() {
	local range first last cpu
	IFS=, read -ra ranges <<<"$1"
	for range in "${ranges[@]}"; do
		[[ -n "${range}" ]] || continue
		first="${range%-*}"
		last="${range#*-}"
		for ((cpu = first; cpu <= last; ++cpu)); do
			echo "${cpu}"
		done
	done
}

cleanup() {
	if [[ -d "${GROUP}" ]]; then
		echo member >"${GROUP}/cpuset.cpus.partition" 2>/dev/null || true
		rmdir "${GROUP}" 2>/dev/null || true
	fi
	for entry in "${NARROWED[@]}" "${GOVERNORS[@]}"; do
		echo "${entry#*=}" >"${entry%%=*}" 2>/dev/null || true
	done
}
trap cleanup EXIT

grep -qw cpuset "${CGROOT}/cgroup.subtree_control" ||
	echo +cpuset >"${CGROOT}/cgroup.subtree_control"
for sibling in "${CGROOT}"/*/; do
	file="${sibling}cpuset.cpus"
	[[ -w "${file}" ]] || continue
	old="$(cat "${file}")"
	[[ -n "${old}" ]] || continue
	rest="$(comm -23 <(expand "${old}" | sort) <(printf '%s\n' "${CPU_LIST[@]}" | sort) | sort -n | paste -sd,)"
	[[ "${rest}" != "$(expand "${old}" | sort -n | paste -sd,)" ]] || continue
	[[ -n "${rest}" ]] || {
		echo "$(basename "${sibling}") would be left without CPUs" >&2
		exit 1
	}
	NARROWED+=("${file}=${old}")
	echo "${rest}" >"${file}"
	echo "moved $(basename "${sibling}") from CPUs ${old} to ${rest}"
done
mkdir "${GROUP}"
echo "${CPUS}" >"${GROUP}/cpuset.cpus"
cat "${CGROOT}/cpuset.mems.effective" >"${GROUP}/cpuset.mems"
echo root >"${GROUP}/cpuset.cpus.partition"
partition="$(cat "${GROUP}/cpuset.cpus.partition")"
[[ "${partition}" == root ]] || {
	echo "the kernel refused an exclusive partition on ${CPUS}: ${partition}" >&2
	exit 1
}

for cpu in "${CPU_LIST[@]}"; do
	governor="/sys/devices/system/cpu/cpu${cpu}/cpufreq/scaling_governor"
	if [[ -w "${governor}" ]]; then
		GOVERNORS+=("${governor}=$(cat "${governor}")")
		echo performance >"${governor}" 2>/dev/null || true
	fi
done

echo "taken: CPUs ${CPUS} (${COUNT}), partition ${partition}"
echo "load on the rest of the machine: $(cut -d' ' -f1-3 /proc/loadavg)"
(
	echo "${BASHPID}" >"${GROUP}/cgroup.procs"
	exec runuser -u "${SUDO_USER}" -- env \
		SC_SERVER_ARGS="${SC_SERVER_ARGS:---cpu_threads ${COUNT}}" \
		FORCE="${FORCE:-1}" \
		"$@"
)
