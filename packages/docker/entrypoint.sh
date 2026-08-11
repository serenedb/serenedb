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

	: "${SERENEDB_HOST_AUTH_METHOD:=}"
	if [ -n "$SERENEDB_HOST_AUTH_METHOD" ]; then
		# Only the methods serened evaluates itself. The parser accepts more
		# (ident, peer, gss, ldap, cert, ...) but they are either unenforced or
		# refuse the connection, so a typo would silently cost you access: the
		# server logs a parse error and falls back to requiring a password.
		case "$SERENEDB_HOST_AUTH_METHOD" in
		trust | reject | password | md5 | scram-sha-256) ;;
		*)
			echo >&2 "SERENEDB_HOST_AUTH_METHOD=\"$SERENEDB_HOST_AUTH_METHOD\" is not supported."
			echo >&2 "Use one of: trust, scram-sha-256, md5, password, reject."
			exit 1
			;;
		esac

		RUNTIME_HBA="/tmp/pg_hba.conf"
		{
			echo "# Generated from SERENEDB_HOST_AUTH_METHOD by the container entrypoint."
			if [ "$SERENEDB_HOST_AUTH_METHOD" = "trust" ]; then
				echo "# warning: trust is enabled for all connections"
				echo "# https://www.postgresql.org/docs/current/auth-trust.html"
			fi
			echo "local all all $SERENEDB_HOST_AUTH_METHOD"
			echo "host  all all 0.0.0.0/0 $SERENEDB_HOST_AUTH_METHOD"
			echo "host  all all ::/0      $SERENEDB_HOST_AUTH_METHOD"
		} >"$RUNTIME_HBA"
		set -- "$@" "--hba_config=$RUNTIME_HBA"
		echo "Auth: $SERENEDB_HOST_AUTH_METHOD ($RUNTIME_HBA)"

		if [ "$SERENEDB_HOST_AUTH_METHOD" = "trust" ]; then
			cat >&2 <<-'EOWARN'
				********************************************************************************
				WARNING: SERENEDB_HOST_AUTH_METHOD has been set to "trust". This will allow
				         anyone with access to the SereneDB port to connect as any role
				         without a password. In Docker's default configuration that is
				         effectively any other container on the same system.

				         Fine for a laptop and a demo. For anything else, set
				         SERENEDB_HOST_AUTH_METHOD=scram-sha-256 and give the role a password.
				********************************************************************************
			EOWARN
		fi
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
