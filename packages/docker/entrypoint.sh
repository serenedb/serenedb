#!/bin/sh
set -e

# If command starts with '-', prepend 'serened'
case "$1" in
-*) set -- serened "$@" ;;
esac

if [ "$1" = "serened" ]; then
	echo "=== Starting SereneDB ==="

	# Copy config to writable location (serened may modify it at runtime)
	CONFIG_FILE="/etc/serenedb/serened.conf"
	RUNTIME_CONFIG="/tmp/serened.conf"
	if [ -f "$CONFIG_FILE" ]; then
		cp "$CONFIG_FILE" "$RUNTIME_CONFIG"
		set -- "$@" "--flagfile=$RUNTIME_CONFIG"
		echo "Config: $RUNTIME_CONFIG"
	fi

	# TODO: benchmark NUMA policy on multi-socket systems.
	# numactl --interleave=all spreads memory across all NUMA nodes evenly,
	# but jemalloc (our allocator) has its own NUMA-aware per-CPU arenas
	# that prefer local node allocation. Interleaving may defeat this and
	# hurt cache-friendly workloads. Need to compare:
	#   1. no numactl (jemalloc decides, default local allocation)
	#   2. numactl --interleave=all (even spread, current behavior)
	#   3. jemalloc's --enable-percpu-arena (explicit NUMA-aware arenas)
	NUMACTL=""
	if [ -d /sys/devices/system/node/node1 ] && [ -f /proc/self/numa_maps ]; then
		NUMACTL="numactl --interleave=${NUMA:-all}"
		if [ "${NUMA:-}" = "disable" ]; then
			NUMACTL=""
		elif ! $NUMACTL echo >/dev/null 2>&1; then
			echo "NUMA: cannot use $NUMACTL (try: docker run --cap-add SYS_NICE)"
			NUMACTL=""
		else
			echo "NUMA: $NUMACTL"
		fi
	fi

	# TODO: add PostgreSQL-style init script support
	# (start serened in background, run .sh/.sql from /docker-entrypoint-initdb.d/, restart)

	echo ""
	exec $NUMACTL "$@"
fi

# Not serened -- run whatever was requested
exec "$@"
