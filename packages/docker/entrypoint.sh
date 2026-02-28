#!/bin/sh
# =============================================================================
# SereneDB Docker Entrypoint
# Handles initialization and startup of the database server
# =============================================================================
set -e

# Configuration
NUMACTL=""
CONFIG_FILE="/etc/serenedb/serened.conf"
RUNTIME_CONFIG="/tmp/serened.conf"

# Handle command line arguments
# If command starts with '-', prepend 'serened'
case "$1" in
-*)
  set -- serened "$@"
  ;;
esac

# NUMA Configuration
# Improves performance on multi-socket systems
configure_numa() {
  if [ -d /sys/devices/system/node/node1 ] && [ -f /proc/self/numa_maps ]; then
    if [ -z "$NUMA" ]; then
      NUMACTL="numactl --interleave=all"
    elif [ "$NUMA" != "disable" ]; then
      NUMACTL="numactl --interleave=$NUMA"
    fi

    if [ -n "$NUMACTL" ]; then
      if $NUMACTL echo >/dev/null 2>&1; then
        echo "NUMA: Using $NUMACTL"
      else
        echo "NUMA: Cannot use $NUMACTL (try: docker run --cap-add SYS_NICE)"
        NUMACTL=""
      fi
    fi
  fi
}

# Run init scripts from /docker-entrypoint-initdb.d/
run_init_scripts() {
  if [ -d /docker-entrypoint-initdb.d ] && [ -n "$(ls -A /docker-entrypoint-initdb.d 2>/dev/null)" ]; then
    echo "Running initialization scripts..."
    for f in /docker-entrypoint-initdb.d/*; do
      case "$f" in
      *.sh)
        echo "  Running: $f"
        . "$f"
        ;;
      *)
        echo "  Skipping: $f (not .sh)"
        ;;
      esac
    done
  fi
}

# Main
if [ "$1" = "serened" ]; then
  echo "=== Starting SereneDB ==="

  # Copy config to writable location
  cp "$CONFIG_FILE" "$RUNTIME_CONFIG"

  # Configure
  configure_numa
  run_init_scripts

  # Build final command
  set -- "$@" \
    --server.authentication="$AUTHENTICATION" \
    --config "$RUNTIME_CONFIG"

  echo "Config: $RUNTIME_CONFIG"
  echo ""
else
  # Not serened - run whatever was requested
  NUMACTL=""
fi

# Execute
exec $NUMACTL "$@"
