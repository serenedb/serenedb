#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
ROOT=/home/mironov/projects/serenedb/serenedb
NEW=$S/bins/serened-new
MAIN=$S/wt_base/build_perf/bin/serened
PRE=$S/wt_presplit/build_perf/bin/serened
OUT=$S/perf
mkdir -p "$OUT"
cd "$ROOT" || exit 1
log() { echo "[$(date -u +%H:%M:%S)] load=$(cut -d' ' -f1 /proc/loadavg) $*" | tee -a "$OUT/plan.log"; }

log "catalog vs main"
PERF_CAT_OLD_BIN=$MAIN PERF_CAT_NEW_BIN=$NEW PERF_CAT_REPS=5 bash scripts/perf/bench_catalog_ddl.sh >"$OUT/cat_main.log" 2>&1
log "rc=$?"
log "catalog vs pre-split"
PERF_CAT_OLD_BIN=$PRE PERF_CAT_NEW_BIN=$NEW PERF_CAT_REPS=5 bash scripts/perf/bench_catalog_ddl.sh >"$OUT/cat_pre.log" 2>&1
log "rc=$?"
log "recovery (index replay) vs main"
PERF_STORAGE_OLD_BIN=$MAIN PERF_STORAGE_NEW_BIN=$NEW PERF_REC_REFRESH=0 PERF_QUIET_LOAD=1000 bash scripts/perf/bench_recovery_large_wal.sh >"$OUT/rec0_main.log" 2>&1
log "rc=$?"
log "recovery (index replay) vs pre-split"
PERF_STORAGE_OLD_BIN=$PRE PERF_STORAGE_NEW_BIN=$NEW PERF_REC_REFRESH=0 PERF_QUIET_LOAD=1000 bash scripts/perf/bench_recovery_large_wal.sh >"$OUT/rec0_pre.log" 2>&1
log "rc=$?"
log "recovery (table replay) vs main"
PERF_STORAGE_OLD_BIN=$MAIN PERF_STORAGE_NEW_BIN=$NEW PERF_REC_REFRESH=1 PERF_QUIET_LOAD=1000 bash scripts/perf/bench_recovery_large_wal.sh >"$OUT/rec1_main.log" 2>&1
log "rc=$?"
log "storage vs main"
PERF_STORAGE_OLD_BIN=$MAIN PERF_STORAGE_NEW_BIN=$NEW PERF_PROFILE=core PERF_BASELINE_DIR=$OUT/baselines-main PERF_REMEASURE_OLD=1 PERF_QUIET_WAIT_SECS=0 bash scripts/perf/bench_storage_old_vs_new.sh >"$OUT/storage_main.log" 2>&1
log "rc=$?"
log "storage vs pre-split"
PERF_STORAGE_OLD_BIN=$PRE PERF_STORAGE_NEW_BIN=$NEW PERF_PROFILE=core PERF_BASELINE_DIR=$OUT/baselines-pre PERF_REMEASURE_OLD=1 PERF_QUIET_WAIT_SECS=0 bash scripts/perf/bench_storage_old_vs_new.sh >"$OUT/storage_pre.log" 2>&1
log "rc=$?"
log "done"
