#!/bin/bash
# Reproducer: an exception escaping IcebergMultiFileListSharedState's destructor
# aborts serened instead of failing the statement.
#
#   third_party/duckdb_iceberg/src/planning/iceberg_multi_file_list.cpp:247
#     IcebergMultiFileListSharedState::~IcebergMultiFileListSharedState() {
#         if (data_manifest_read_state) {
#             //! FIXME: this could throw, if the tasks encountered an error
#             data_manifest_read_state->executor.WorkOnTasks();
#         }
#     }
#
# A manifest read that fails leaves the error in the task executor; the destructor
# re-raises it, and a throwing destructor is std::terminate.
#
# Usage: repro_iceberg_destructor_abort.sh [--binary PATH] [--snapshots N] [--no-restart] [--keep]
set -u
REPO=/home/ivanovp/serenedb
BIN=$REPO/build/bin/serened
SNAPSHOTS=40
RESTART=1
KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --binary) BIN=$2; shift 2;;
    --snapshots) SNAPSHOTS=$2; shift 2;;
    --no-restart) RESTART=0; shift;;
    --keep) KEEP=1; shift;;
    *) echo "unknown option $1"; exit 2;;
  esac
done
WORK=$(mktemp -d); DD=$WORK/datadir; LOG=$WORK/serened.log; STATE=$WORK/fixture.state
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
NS="rp$$"; SRV=""; OWN_FIXTURE=0
cleanup() {
  [ -n "$SRV" ] && kill -9 $SRV 2>/dev/null
  [ "$OWN_FIXTURE" = 1 ] && python3 "$REPO/tests/drivers/harness/iceberg_rest.py" stop --state "$STATE" >/dev/null 2>&1
  if [ "$KEEP" = 1 ]; then echo "artifacts kept in $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT
Q() { psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start() {
  nohup "$BIN" "$DD" --listen="postgres://0.0.0.0:$PORT" >> "$LOG" 2>&1 & SRV=$!
  for i in $(seq 1 180); do
    Q 'SELECT 1' >/dev/null 2>&1 && return 0
    kill -0 $SRV 2>/dev/null || { echo "FAIL: server exited during boot"; return 1; }
    sleep 1
  done
  echo "FAIL: server did not accept connections"; return 1
}
stop() { [ -n "$SRV" ] || return 0; kill -TERM $SRV 2>/dev/null
  for i in $(seq 1 180); do kill -0 $SRV 2>/dev/null || return 0; sleep 1; done; kill -9 $SRV 2>/dev/null; }
attach() {
  Q "CREATE OR REPLACE PERSISTENT SECRET s (TYPE S3, KEY_ID '$MINIO_ACCESS_KEY', SECRET '$MINIO_SECRET_KEY', ENDPOINT '$MINIO_HOST:$MINIO_PORT', URL_STYLE 'path', USE_SSL false, SCOPE 's3://$MINIO_BUCKET/warehouse/')" | grep -i '^ERROR'
  Q "CREATE SERVER IF NOT EXISTS lake FOREIGN DATA WRAPPER iceberg_fdw OPTIONS (warehouse '$ICEBERG_WAREHOUSE', endpoint '$ICEBERG_REST_URL', authorization_type 'none')" | grep -i '^ERROR'
}
mcx() { docker exec "$MINIO_CONTAINER" mc "$@" 2>/dev/null; }

if [ -n "${ICEBERG_REST_URL:-}" ]; then
  echo "using the iceberg fixture from the environment"
else
  echo "starting an iceberg-rest + MinIO fixture"
  eval "$(python3 "$REPO/tests/drivers/harness/iceberg_rest.py" start --state "$STATE")" || exit 1
  OWN_FIXTURE=1
fi
if [ "$OWN_FIXTURE" = 1 ]; then
  MINIO_CONTAINER=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["minio"])' "$STATE")
else
  MINIO_CONTAINER=$(docker ps --format '{{.Names}}' | grep -- "-minio$" | head -1)
fi
[ -z "$MINIO_CONTAINER" ] && { echo "FAIL: no MinIO container found"; exit 1; }
# Refuse to touch the box-wide MinIO: it bind-mounts the repo's iceberg fixture
# data, so removing objects there would destroy checked-in test resources.
case "$MINIO_CONTAINER" in
  minio|*serenedb*) echo "FAIL: refusing to use shared container '$MINIO_CONTAINER'"; exit 1;;
esac
if docker inspect "$MINIO_CONTAINER" --format '{{range .Mounts}}{{.Source}} {{end}}' 2>/dev/null | grep -q "$REPO"; then
  echo "FAIL: '$MINIO_CONTAINER' bind-mounts a path inside $REPO; refusing to delete objects in it"; exit 1
fi
mcx alias set local http://127.0.0.1:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null

echo "binary: $BIN"
start || exit 1
echo "version: $(Q 'SELECT version()')"
attach
Q "CREATE SCHEMA lake.$NS" | grep -i '^ERROR'
Q "CREATE TABLE lake.$NS.t (id INTEGER, body TEXT)" | grep -i '^ERROR'
echo "writing $SNAPSHOTS snapshots so the manifest read runs as a parallel task set"
for i in $(seq 1 $SNAPSHOTS); do
  Q "INSERT INTO lake.$NS.t SELECT s, 'doc ' || s FROM generate_series($((i*100-99)), $((i*100))) t(s)" | grep -i '^ERROR'
done
PREFIX=local/$MINIO_BUCKET/warehouse/$NS/t/metadata
MANIFESTS=$(mcx ls $PREFIX/ | awk '{print $NF}' | grep -E -- '-m[0-9]+\.avro$')
echo "rows=$(Q "SELECT count(*) FROM lake.$NS.t")  manifests=$(echo "$MANIFESTS" | wc -l)"

if [ "$RESTART" = 1 ]; then stop; start || exit 1; attach; fi
CONTROL=$(Q "SELECT count(*) FROM lake.$NS.t")
echo "control scan before the fault: $CONTROL"
case "$CONTROL" in *ERROR*) echo "FAIL: control scan already broken"; exit 1;; esac

case "$PREFIX" in
  *"/warehouse/$NS/t/metadata") : ;;
  *) echo "FAIL: refusing to delete from an unexpected prefix '$PREFIX'"; exit 1;;
esac
[ -z "$MANIFESTS" ] && { echo "FAIL: no manifest files found under $PREFIX"; exit 1; }
# Remove every data manifest of THIS table. The manifest list (snap-*.avro), the
# metadata json and the parquet data are deliberately left intact, so the failure
# lands on a manifest read task and nowhere else.
for m in $MANIFESTS; do mcx rm $PREFIX/$m >/dev/null; done
echo "fault injected: removed $(echo "$MANIFESTS" | wc -l) data manifests of this table, manifest list kept"
if [ "$RESTART" = 1 ]; then stop; start || exit 1; attach; fi

OUT=$(Q "SELECT count(*) FROM lake.$NS.t")
sleep 4
echo "client saw: $(echo "$OUT" | head -2 | tr '\n' ' ' | cut -c1-140)"
if kill -0 $SRV 2>/dev/null; then
  echo "NOT REPRODUCED: the statement failed and the server stayed up"
  exit 1
fi
UNCAUGHT=$(grep -E "terminating due to uncaught exception" "$LOG" | tail -1)
if [ -z "$UNCAUGHT" ]; then
  echo "INCONCLUSIVE: the server died without an uncaught-exception message"
  tail -5 "$LOG" | cut -c1-160
  exit 1
fi
echo "REPRODUCED: serened aborted on an exception that escaped a destructor"
echo "  $(echo "$UNCAUGHT" | cut -c1-190)"
if grep -q "IcebergMultiFileListSharedState::~IcebergMultiFileListSharedState" "$LOG"; then
  echo "  symbolised frames:"
  grep -oE "duckdb::Iceberg[A-Za-z]*(::~[A-Za-z]+)?|duckdb::MultiFileBindData::~MultiFileBindData" "$LOG" | sort -u | sed 's/^/    /'
else
  echo "  (binary is stripped: no symbols, but the uncaught exception is the manifest read and the"
  echo "   only caller that can terminate on it is the shared-state destructor)"
fi
exit 0
