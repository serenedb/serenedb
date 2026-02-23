#!/bin/sh
# =============================================================================
# SereneDB Docker Entrypoint
# Handles initialization and startup of the database server
# =============================================================================
set -e

# Configuration
SERENE_INIT_PORT="${SERENE_INIT_PORT:-8999}"
AUTHENTICATION="true"
NUMACTL=""
CONFIG_FILE="/etc/serenedb/serened.conf"
RUNTIME_CONFIG="/tmp/serened.conf"
DATA_DIR="/var/lib/serenedb"
INIT_MARKER="${DATA_DIR}/SERVER"

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

# Password handling
get_root_password() {
  # From file
  if [ -n "$SERENE_ROOT_PASSWORD_FILE" ]; then
    if [ -f "$SERENE_ROOT_PASSWORD_FILE" ]; then
      SERENE_ROOT_PASSWORD="$(cat "$SERENE_ROOT_PASSWORD_FILE")"
    else
      echo "WARNING: Password file not found: $SERENE_ROOT_PASSWORD_FILE"
    fi
  fi

  # Generate random password
  if [ -n "$SERENE_RANDOM_ROOT_PASSWORD" ]; then
    SERENE_ROOT_PASSWORD=$(pwgen -s -1 16)
    echo "==========================================="
    echo "GENERATED ROOT PASSWORD: $SERENE_ROOT_PASSWORD"
    echo "==========================================="
  fi
}

# Database initialization
initialize_database() {
  # Skip if already initialized
  [ -f "$INIT_MARKER" ] && return 0

  # Disable auth if requested
  if [ -n "$SERENE_NO_AUTH" ]; then
    AUTHENTICATION="false"
    return 0
  fi

  get_root_password

  # Check that password is configured
  # Note: ${VAR+x} checks if VAR is set (even if empty)
  if [ -z "${SERENE_ROOT_PASSWORD+x}" ]; then
    echo >&2 "ERROR: Database is uninitialized and no password configured"
    echo >&2 ""
    echo >&2 "Please set one of:"
    echo >&2 "  - SERENE_ROOT_PASSWORD"
    echo >&2 "  - SERENE_ROOT_PASSWORD_FILE"
    echo >&2 "  - SERENE_RANDOM_ROOT_PASSWORD=1"
    echo >&2 "  - SERENE_NO_AUTH=1 (disable authentication)"
    exit 1
  fi

  # Initialize with password (if not empty)
  if [ -n "$SERENE_ROOT_PASSWORD" ]; then
    echo "Initializing database with root password..."
    SERENEDB_DEFAULT_ROOT_PASSWORD="$SERENE_ROOT_PASSWORD" \
      /usr/sbin/serene-init-database \
      -c "$RUNTIME_CONFIG" \
      --server.rest-server false \
      --log.level error \
      --database.init-database true ||
      true
  fi
}

# Apply encryption settings
configure_encryption() {
  if [ -n "$SERENE_ENCRYPTION_KEYFILE" ]; then
    echo "Encryption: Enabled (keyfile: $SERENE_ENCRYPTION_KEYFILE)"
    sed -i "s;^.*encryption-keyfile.*;encryption-keyfile=$SERENE_ENCRYPTION_KEYFILE;" "$RUNTIME_CONFIG"
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
  configure_encryption
  initialize_database
  run_init_scripts

  # Build final command
  set -- "$@" \
    --server.authentication="$AUTHENTICATION" \
    --config "$RUNTIME_CONFIG"

  echo "Authentication: $AUTHENTICATION"
  echo "Config: $RUNTIME_CONFIG"
  echo ""
else
  # Not serened - run whatever was requested
  NUMACTL=""
fi

# Execute
exec $NUMACTL "$@"
