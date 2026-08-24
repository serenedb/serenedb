#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)

usage() {
	cat <<'EOF'
bench_remote_fs.sh -- remote file read/write benchmark: serened shell vs upstream duckdb CLI.

Engines (BRFS_ENGINES):
  sdb               serened shell (patched httpfs, curl-only; azure routed through duckdb's HTTPUtil)
  duckdb-curl       duckdb CLI, SET httpfs_client_implementation='curl'    (s3/http backends)
  duckdb-httplib    duckdb CLI, SET httpfs_client_implementation='httplib' (s3/http backends)
  duckdb-azure-sdk  duckdb CLI, Azure SDK transport (libcurl on Linux)     (az backend)

Backends (BRFS_BACKENDS): s3 (MinIO), az (Azurite), http (anonymous MinIO GET).
All containers run with --network host (no bridge/NAT on the data path); all engine traffic
goes through toxiproxy on loopback in both latency regimes so the data path is identical.
Latencies (BRFS_LATENCIES): wan (default; netem paretonormal on lo, tc-filtered to the two
proxy ports only, 2 x BRFS_WAN_DELAY_MS one-way RTT ~5ms) | none (raw loopback).
Scenarios (BRFS_SCENARIOS): read_large read_glob read_pruned write_large write_many iceberg_scan.
Modes (BRFS_MODES): warm (one session, .timer, first rep discarded) | cold (one fresh process, /usr/bin/time -v).

Requires: docker, python3, curl, GNU /usr/bin/time; network on first run (duckdb CLI zip,
duckdb extensions, container images, pip wheels for the upload/iceberg prep container).

Output: aligned table in BRFS_RESULTS (default scripts/perf/results/bench_remote_fs_<ts>.txt)
and on stdout; per-cell engine output under BRFS_DATA/logs. Columns: engine client backend
scenario mode reps median_s p90_s rss_mb mbps. Cold rss_mb is the query-process peak; warm
rss_mb is the peak over the timed reps only (VmHWM is reset via /proc/PID/clear_refs after
setup + warmup). duckdb rows carry (+/-N%) deltas vs the sdb row of the same group. Read mbps is exact (uploaded bytes / median), write
mbps approximated from the source share.

Config (env): BRFS_SDB_BIN BRFS_DUCKDB_BIN BRFS_DUCKDB_VERSION BRFS_DATA BRFS_RESULTS
  BRFS_THREADS BRFS_MEMORY_LIMIT BRFS_REPS_READ BRFS_REPS_WRITE BRFS_EXT_FILE_CACHE
  BRFS_ENGINES BRFS_BACKENDS BRFS_SCENARIOS BRFS_LATENCIES BRFS_MODES
  BRFS_WAN_DELAY_MS BRFS_WAN_JITTER_MS BRFS_WAN_LOSS_PCT
  BRFS_ROWS_LARGE BRFS_ROWS_SMALL_TOTAL BRFS_SMALL_FILES
  BRFS_ROWS_WRITE_LARGE BRFS_ROWS_WRITE_MANY BRFS_ROWS_ICEBERG BRFS_WRITE_PARTS
  BRFS_PREFIX BRFS_S3_PORT BRFS_AZ_PORT BRFS_TOXI_PORT BRFS_MINIO_PORT BRFS_AZURITE_PORT BRFS_KEEP

Defaults are sized so the full matrix finishes in ~5 minutes; for a serious run scale up,
e.g. BRFS_ROWS_LARGE=8000000 BRFS_SMALL_FILES=200 BRFS_REPS_READ=10 BRFS_REPS_WRITE=5.
EOF
}
[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && {
	usage
	exit 0
}

: "${BRFS_SDB_BIN:=$ROOT/build_bench/bin/serened}"
: "${BRFS_DUCKDB_BIN:=}"
: "${BRFS_DUCKDB_VERSION:=v1.5.4}"
: "${BRFS_DATA:=${HOME}/.cache/serenedb-bench-remote-fs}"
: "${BRFS_RESULTS:=$ROOT/scripts/perf/results/bench_remote_fs_$(date +%Y%m%d_%H%M%S).txt}"
: "${BRFS_THREADS:=8}"
: "${BRFS_MEMORY_LIMIT:=8GB}"
: "${BRFS_REPS_READ:=5}"
: "${BRFS_REPS_WRITE:=3}"
: "${BRFS_EXT_FILE_CACHE:=0}"
: "${BRFS_ENGINES:=sdb duckdb-curl duckdb-httplib duckdb-azure-sdk}"
: "${BRFS_BACKENDS:=s3 az http}"
: "${BRFS_SCENARIOS:=read_large read_glob read_pruned write_large write_many iceberg_scan}"
: "${BRFS_LATENCIES:=wan}"
: "${BRFS_MODES:=warm cold}"
: "${BRFS_WAN_DELAY_MS:=2.5}"
: "${BRFS_WAN_JITTER_MS:=0.5}"
: "${BRFS_WAN_LOSS_PCT:=0}"
: "${BRFS_ROWS_LARGE:=6000000}"
: "${BRFS_ROWS_SMALL_TOTAL:=1800000}"
: "${BRFS_SMALL_FILES:=180}"
: "${BRFS_ROWS_WRITE_LARGE:=1200000}"
: "${BRFS_ROWS_WRITE_MANY:=300000}"
: "${BRFS_ROWS_ICEBERG:=1200000}"
: "${BRFS_WRITE_PARTS:=10}"
: "${BRFS_PREFIX:=brfs}"
: "${BRFS_KEEP:=0}"
: "${BRFS_BUCKET:=testbucket}"
: "${BRFS_CONTAINER:=testcont}"
: "${BRFS_MINIO_IMAGE:=minio/minio:latest}"
: "${BRFS_AZURITE_IMAGE:=mcr.microsoft.com/azure-storage/azurite}"
: "${BRFS_TOXIPROXY_IMAGE:=ghcr.io/shopify/toxiproxy}"
: "${BRFS_NETSHOOT_IMAGE:=nicolaka/netshoot}"
: "${BRFS_PYTHON_IMAGE:=python:3.12-slim}"

MINIO_NAME="${BRFS_PREFIX}-minio"
AZURITE_NAME="${BRFS_PREFIX}-azurite"
TOXI_NAME="${BRFS_PREFIX}-toxiproxy"
AZ_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
DDB_BIN=""
WAN_TOUCHED=0
SDB_MED=NA
SDB_P90=NA
SDB_RSS=NA
SDB_MBPS=NA
SZ_LARGE=0
SZ_SMALL=0
PRUNE_LO=$((BRFS_ROWS_LARGE / 2))
PRUNE_HI=$((PRUNE_LO + 1200000))

die() {
	echo "ERROR: $*" >&2
	exit 1
}

need() {
	command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"
}

free_port() {
	python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

engine_cmd() {
	case "$1" in
	sdb)
		printf '%s\n' "$BRFS_SDB_BIN" shell
		;;
	duckdb-*)
		printf '%s\n' "$DDB_BIN"
		;;
	*)
		die "unknown engine $1"
		;;
	esac
}

engine_label() {
	if [[ "$1" == sdb ]]; then printf 'sdb'; else printf 'duckdb'; fi
}

client_label() {
	case "$1" in
	sdb | duckdb-curl) printf 'curl' ;;
	duckdb-httplib) printf 'httplib' ;;
	duckdb-azure-sdk) printf 'azure-sdk' ;;
	esac
}

cell_ok() {
	local engine="$1" backend="$2" scen="$3"
	case "$backend" in
	s3 | http)
		case "$engine" in sdb | duckdb-curl | duckdb-httplib) ;; *) return 1 ;; esac
		;;
	az)
		case "$engine" in sdb | duckdb-azure-sdk) ;; *) return 1 ;; esac
		;;
	esac
	case "$backend:$scen" in
	az:iceberg_scan) return 1 ;;
	http:read_glob | http:write_large | http:write_many | http:iceberg_scan) return 1 ;;
	esac
	return 0
}

setup_sql() {
	local engine="$1" backend="$2" sql=""
	if [[ "$engine" == duckdb-* ]]; then
		sql+="SET extension_directory='${BRFS_DATA}/ddb_ext';"
		sql+="LOAD httpfs;LOAD azure;LOAD avro;LOAD iceberg;"
	fi
	case "$engine" in
	duckdb-curl) sql+="SET httpfs_client_implementation='curl';" ;;
	duckdb-httplib) sql+="SET httpfs_client_implementation='httplib';" ;;
	esac
	# Connection reuse for every engine except duckdb-azure-sdk: its azure traffic is
	# pooled inside the Azure SDK's own curl transport, unconditionally, so the httpfs
	# setting does not apply to it.
	[[ "$engine" != duckdb-azure-sdk ]] && sql+="SET httpfs_connection_caching=true;"
	sql+="SET threads=${BRFS_THREADS};SET memory_limit='${BRFS_MEMORY_LIMIT}';"
	[[ "$BRFS_EXT_FILE_CACHE" == 1 ]] || sql+="SET enable_external_file_cache=false;"
	case "$backend" in
	s3)
		sql+="CREATE OR REPLACE SECRET brfs_s3 (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin', ENDPOINT '127.0.0.1:${S3_PORT}', USE_SSL false, URL_STYLE 'path', REGION 'us-east-1');"
		;;
	az)
		sql+="CREATE OR REPLACE SECRET brfs_az (TYPE azure, CONNECTION_STRING 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=${AZ_KEY};BlobEndpoint=http://127.0.0.1:${AZ_PORT}/devstoreaccount1;');"
		;;
	esac
	printf '%s' "$sql"
}

scenario_sql() {
	local scen="$1" backend="$2" engine="$3" lat="$4" mode="$5"
	local u tag="${engine}_${lat}_${mode}"
	case "$backend" in
	s3) u="s3://${BRFS_BUCKET}" ;;
	az) u="az://${BRFS_CONTAINER}" ;;
	http) u="http://127.0.0.1:${S3_PORT}/${BRFS_BUCKET}" ;;
	esac
	case "$scen" in
	read_large)
		printf "SELECT count(*), sum(a), sum(p), max(h), max(h2) FROM read_parquet('%s/large.parquet');" "$u"
		;;
	read_glob)
		printf "SELECT count(*), sum(a) FROM read_parquet('%s/small/*/*.parquet');" "$u"
		;;
	read_pruned)
		printf "SELECT count(*), sum(a) FROM read_parquet('%s/large.parquet') WHERE id BETWEEN %s AND %s;" "$u" "$PRUNE_LO" "$PRUNE_HI"
		;;
	write_large)
		printf "COPY (SELECT * FROM read_parquet('%s/large.parquet') WHERE id < %s) TO '%s/out/wl_%s.parquet' (FORMAT parquet);" "$BRFS_DATA" "$BRFS_ROWS_WRITE_LARGE" "$u" "$tag"
		;;
	write_many)
		printf "COPY (SELECT * FROM read_parquet('%s/large.parquet') WHERE id < %s) TO '%s/out/wm_%s' (FORMAT parquet, PARTITION_BY (p), OVERWRITE_OR_IGNORE);" "$BRFS_DATA" "$BRFS_ROWS_WRITE_MANY" "$u" "$tag"
		;;
	iceberg_scan)
		printf "SELECT count(*), sum(a) FROM iceberg_scan('%s');" "$(cat "$BRFS_DATA/iceberg_meta.txt")"
		;;
	esac
}

scen_bytes() {
	case "$1" in
	read_large) printf '%s' "$SZ_LARGE" ;;
	read_glob) printf '%s' "$SZ_SMALL" ;;
	write_large) awk -v s="$SZ_LARGE" -v a="$BRFS_ROWS_WRITE_LARGE" -v b="$BRFS_ROWS_LARGE" 'BEGIN{printf "%d", s * a / b}' ;;
	write_many) awk -v s="$SZ_LARGE" -v a="$BRFS_ROWS_WRITE_MANY" -v b="$BRFS_ROWS_LARGE" 'BEGIN{printf "%d", s * a / b}' ;;
	*) printf '' ;;
	esac
}

row_out() {
	printf '%-8s %-10s %-8s %-13s %-5s %5s %16s %16s %14s %15s\n' "$@"
}

pct() {
	local cur="${1#\~}" ref="${2#\~}"
	if [[ -z "$cur" || "$cur" == NA || -z "$ref" || "$ref" == NA ]]; then
		return 0
	fi
	awk -v c="$cur" -v r="$ref" 'BEGIN { if (r > 0) printf "(%+.0f%%)", (c - r) / r * 100 }'
}

brfs_stats() {
	awk '
	function srt(a, n,   i, j, t) {
		for (i = 2; i <= n; i++) { t = a[i]; j = i - 1; while (j > 0 && a[j] > t) { a[j + 1] = a[j]; j-- } a[j + 1] = t }
	}
	function med(a, n) { return n % 2 ? a[int((n + 1) / 2)] : (a[n / 2] + a[n / 2 + 1]) / 2 }
	function p90(a, n,   i) { i = int(0.9 * n); if (i < 0.9 * n) i++; if (i < 1) i = 1; return a[i] }
	{ r[NR] = $1 + 0; if (NF > 3 && $4 + 0 > rss) rss = $4 + 0 }
	END {
		if (NR == 0) { printf "NA\tNA\tNA\n"; exit }
		srt(r, NR)
		printf "%.3f\t%.3f\t%s\n", med(r, NR), p90(r, NR), (rss ? sprintf("%.1f", rss / 1024) : "NA")
	}'
}

scen_reps() {
	case "$1" in
	write_*) printf '%s' "$BRFS_REPS_WRITE" ;;
	*) printf '%s' "$BRFS_REPS_READ" ;;
	esac
}

run_warm() {
	local engine="$1" setup="$2" q="$3" log="$4" reps="$5"
	local i stdin
	stdin=$(printf '.bail on\n.timer on\n%s\n%s' "$setup" "$q")
	stdin+=$(printf '\n.system echo 5 > /proc/\$PPID/clear_refs')
	for i in $(seq 1 "$reps"); do
		stdin+=$(printf '\n%s' "$q")
	done
	stdin+=$(printf '\n.system grep VmHWM /proc/\$PPID/status')
	local -a cmd
	mapfile -t cmd < <(engine_cmd "$engine")
	printf '%s\n' "$stdin" | "${cmd[@]}" -init /dev/null -batch >"$log" 2>&1 || return 1
	grep -c '^Run Time' "$log" >/dev/null || return 1
	local stats rss
	stats=$(grep '^Run Time' "$log" | tail -n "$reps" | awk '{print $5, $7, $9}' | brfs_stats)
	rss=$(awk '/^VmHWM/ { printf "%.1f", $2 / 1024 }' "$log")
	printf '%s\t%s\n' "${stats%$'\t'*}" "${rss:-NA}"
}

run_cold() {
	local engine="$1" setup="$2" q="$3" log="$4" reps="$5"
	local i
	local tf="$log.time" acc="$log.acc"
	local -a cmd
	mapfile -t cmd < <(engine_cmd "$engine")
	: >"$acc"
	: >"$log"
	for i in $(seq 1 "$reps"); do
		printf '.bail on\n%s\n%s\n' "$setup" "$q" |
			/usr/bin/time -v -o "$tf" "${cmd[@]}" -init /dev/null -batch >>"$log" 2>&1 || return 1
		awk '
		/Elapsed \(wall clock\)/ { n = split($NF, x, ":"); s = 0; for (j = 1; j <= n; j++) s = s * 60 + x[j]; real = s }
		/User time \(seconds\)/ { u = $NF }
		/System time \(seconds\)/ { sy = $NF }
		/Maximum resident set size/ { r = $NF }
		END { print real, u, sy, r }
		' "$tf" >>"$acc"
	done
	brfs_stats <"$acc"
}

run_cell() {
	local engine="$1" backend="$2" scen="$3" lat="$4" mode="$5"
	local setup q log stats reps rc=0 bytes mbps="" median
	setup=$(setup_sql "$engine" "$backend")
	q=$(scenario_sql "$scen" "$backend" "$engine" "$lat" "$mode")
	log="$BRFS_DATA/logs/${engine}_${backend}_${scen}_${lat}_${mode}.log"
	echo ">> $engine $backend $scen $mode" >&2
	reps=$(scen_reps "$scen")
	[[ "$mode" == cold ]] && reps=1
	if [[ "$mode" == warm ]]; then
		stats=$(run_warm "$engine" "$setup" "$q" "$log" "$reps") || rc=$?
	else
		stats=$(run_cold "$engine" "$setup" "$q" "$log" "$reps") || rc=$?
	fi
	if [[ $rc -ne 0 || -z "$stats" ]]; then
		stats=$(printf 'NA\tNA\tNA')
		echo "!! failed, see $log" >&2
	fi
	bytes=$(scen_bytes "$scen")
	median=${stats%%$'\t'*}
	if [[ -n "$bytes" && "$median" != NA ]]; then
		mbps=$(awk -v b="$bytes" -v t="$median" 'BEGIN{ if (t > 0) printf "%.1f", b / 1e6 / t }')
	fi
	local median_v p90_v rss_v mbps_v
	IFS=$'\t' read -r median_v p90_v rss_v <<<"$stats"
	mbps_v="${mbps:-NA}"
	if [[ "$engine" == sdb ]]; then
		SDB_MED="$median_v" SDB_P90="$p90_v" SDB_RSS="$rss_v" SDB_MBPS="$mbps_v"
	else
		median_v="$median_v$(pct "$median_v" "$SDB_MED")"
		p90_v="$p90_v$(pct "$p90_v" "$SDB_P90")"
		rss_v="$rss_v$(pct "$rss_v" "$SDB_RSS")"
		mbps_v="$mbps_v$(pct "$mbps_v" "$SDB_MBPS")"
	fi
	row_out "$(engine_label "$engine")" "$(client_label "$engine")" "$backend" "$scen" "$mode" \
		"$reps" "$median_v" "$p90_v" "$rss_v" "$mbps_v" | tee -a "$BRFS_RESULTS"
}

ensure_duckdb() {
	if [[ -n "$BRFS_DUCKDB_BIN" ]]; then
		DDB_BIN="$BRFS_DUCKDB_BIN"
		[[ -x "$DDB_BIN" ]] || die "BRFS_DUCKDB_BIN=$DDB_BIN is not executable"
	else
		local arch dir zip
		case "$(uname -m)" in
		x86_64) arch=amd64 ;;
		aarch64) arch=arm64 ;;
		*) die "unsupported arch $(uname -m), set BRFS_DUCKDB_BIN" ;;
		esac
		dir="$BRFS_DATA/duckdb-${BRFS_DUCKDB_VERSION}"
		DDB_BIN="$dir/duckdb"
		if [[ ! -x "$DDB_BIN" ]]; then
			echo "downloading duckdb CLI ${BRFS_DUCKDB_VERSION} ..." >&2
			mkdir -p "$dir"
			zip="$dir/cli.zip"
			curl -fsSL -o "$zip" \
				"https://github.com/duckdb/duckdb/releases/download/${BRFS_DUCKDB_VERSION}/duckdb_cli-linux-${arch}.zip"
			python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' "$zip" "$dir"
			chmod +x "$DDB_BIN"
			rm -f "$zip"
		fi
	fi
	"$DDB_BIN" -init /dev/null -batch \
		-c "SET extension_directory='${BRFS_DATA}/ddb_ext'; INSTALL httpfs; INSTALL azure; INSTALL avro; INSTALL iceberg;" \
		>/dev/null
}

gen_datasets() {
	local stamp="$BRFS_DATA/.generated"
	local cfg="${BRFS_ROWS_LARGE}/${BRFS_ROWS_SMALL_TOTAL}/${BRFS_SMALL_FILES}/${BRFS_WRITE_PARTS}"
	if [[ -f "$stamp" && "$(cat "$stamp")" == "$cfg" ]]; then
		return
	fi
	echo "generating datasets in $BRFS_DATA ..." >&2
	rm -rf "$BRFS_DATA/small" "$BRFS_DATA/large.parquet"
	"$DDB_BIN" -init /dev/null -batch -c "
CREATE VIEW big AS
SELECT range AS id,
       ((range * 2654435761) % 1000000)::BIGINT AS a,
       (range % ${BRFS_WRITE_PARTS})::INTEGER AS p,
       md5(range::VARCHAR) AS h,
       md5((range + 7)::VARCHAR) AS h2
FROM range(${BRFS_ROWS_LARGE});
COPY (SELECT * FROM big) TO '${BRFS_DATA}/large.parquet' (FORMAT parquet);
COPY (SELECT *, id % ${BRFS_SMALL_FILES} AS f FROM big WHERE id < ${BRFS_ROWS_SMALL_TOTAL})
  TO '${BRFS_DATA}/small' (FORMAT parquet, PARTITION_BY (f));
" >/dev/null
	printf '%s' "$cfg" >"$stamp"
}

net_up() {
	docker rm -f "$MINIO_NAME" "$AZURITE_NAME" "$TOXI_NAME" >/dev/null 2>&1 || true
	docker run -d --name "$MINIO_NAME" --network host \
		-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
		"$BRFS_MINIO_IMAGE" server /data \
		--address "127.0.0.1:${MINIO_PORT}" --console-address "127.0.0.1:${CONSOLE_PORT}" >/dev/null
	docker run -d --name "$AZURITE_NAME" --network host \
		"$BRFS_AZURITE_IMAGE" azurite-blob \
		--blobHost 127.0.0.1 --blobPort "${AZURITE_PORT}" --skipApiVersionCheck >/dev/null
	docker run -d --name "$TOXI_NAME" --network host \
		"$BRFS_TOXIPROXY_IMAGE" -host 127.0.0.1 -port "${TOXI_PORT}" >/dev/null
	local i
	for i in $(seq 1 30); do
		curl -sf "http://127.0.0.1:${TOXI_PORT}/version" >/dev/null 2>&1 && break
		[[ $i -eq 30 ]] && die "toxiproxy did not come up"
		sleep 1
	done
	docker exec "$TOXI_NAME" /toxiproxy-cli -h "http://127.0.0.1:${TOXI_PORT}" \
		create -l "127.0.0.1:${S3_PORT}" -u "127.0.0.1:${MINIO_PORT}" s3 >/dev/null
	docker exec "$TOXI_NAME" /toxiproxy-cli -h "http://127.0.0.1:${TOXI_PORT}" \
		create -l "127.0.0.1:${AZ_PORT}" -u "127.0.0.1:${AZURITE_PORT}" az >/dev/null
	echo "toxiproxy up (host network): s3 -> 127.0.0.1:${S3_PORT}, az -> 127.0.0.1:${AZ_PORT}" >&2
}

net_down() {
	if [[ "$WAN_TOUCHED" == 1 ]]; then
		tc_host "tc qdisc del dev lo root" >/dev/null 2>&1 || true
	fi
	if [[ "$BRFS_KEEP" == 1 ]]; then
		echo "keeping containers: $MINIO_NAME $AZURITE_NAME $TOXI_NAME" >&2
		return
	fi
	docker rm -f "$MINIO_NAME" "$AZURITE_NAME" "$TOXI_NAME" >/dev/null 2>&1 || true
}

tc_host() {
	docker run --rm --net host --cap-add NET_ADMIN "$BRFS_NETSHOOT_IMAGE" sh -c "$1"
}

wan_set() {
	local lat="$1"
	if [[ "$lat" == none && "$WAN_TOUCHED" == 0 ]]; then
		return
	fi
	WAN_TOUCHED=1
	tc_host "tc qdisc del dev lo root" >/dev/null 2>&1 || true
	[[ "$lat" == none ]] && return
	local loss=""
	[[ "$BRFS_WAN_LOSS_PCT" != 0 ]] && loss="loss ${BRFS_WAN_LOSS_PCT}%"
	local netem="delay ${BRFS_WAN_DELAY_MS}ms ${BRFS_WAN_JITTER_MS}ms 25% distribution paretonormal ${loss}"
	local shape="
tc qdisc add dev lo root handle 1: prio bands 4 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 &&
tc qdisc add dev lo parent 1:4 handle 40: netem NETEM &&
tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip dport ${S3_PORT} 0xffff flowid 1:4 &&
tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip sport ${S3_PORT} 0xffff flowid 1:4 &&
tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip dport ${AZ_PORT} 0xffff flowid 1:4 &&
tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip sport ${AZ_PORT} 0xffff flowid 1:4"
	if ! tc_host "${shape//NETEM/$netem}" >/dev/null 2>&1; then
		echo "netem paretonormal unavailable, falling back to uniform jitter" >&2
		tc_host "tc qdisc del dev lo root" >/dev/null 2>&1 || true
		local fallback="delay ${BRFS_WAN_DELAY_MS}ms ${BRFS_WAN_JITTER_MS}ms 25% ${loss}"
		tc_host "${shape//NETEM/$fallback}" >/dev/null
	fi
	echo "wan profile on: ~$(awk -v d="$BRFS_WAN_DELAY_MS" 'BEGIN{print 2 * d}')ms RTT on proxy ports ${S3_PORT}/${AZ_PORT}" >&2
}

write_prep_py() {
	cat >"$BRFS_DATA/prep_remote.py" <<'PY'
import json
import os
import pathlib
import time

import boto3
from botocore.client import Config

BENCH = pathlib.Path("/bench")
S3_EP = os.environ["BRFS_S3_EP"]
AZ_CONN = os.environ["BRFS_AZ_CONN"]
BUCKET = os.environ["BRFS_BUCKET"]
CONTAINER = os.environ["BRFS_CONTAINER"]
ROWS_ICE = int(os.environ["BRFS_ROWS_ICEBERG"])


def wait(fn, what, tries=60):
    err = None
    for _ in range(tries):
        try:
            return fn()
        except Exception as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"{what} not ready: {err}")


s3 = boto3.client(
    "s3",
    endpoint_url=S3_EP,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
wait(s3.list_buckets, "minio")
try:
    s3.create_bucket(Bucket=BUCKET)
except Exception:
    pass
s3.put_bucket_policy(
    Bucket=BUCKET,
    Policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{BUCKET}/*"],
                }
            ],
        }
    ),
)

files = ["large.parquet"]
files += sorted(str(p.relative_to(BENCH)) for p in (BENCH / "small").rglob("*.parquet"))
for rel in files:
    s3.upload_file(str(BENCH / rel), BUCKET, rel)
print(f"s3: uploaded {len(files)} objects", flush=True)

from azure.storage.blob import BlobServiceClient

blob = BlobServiceClient.from_connection_string(AZ_CONN)
cont = blob.get_container_client(CONTAINER)
wait(cont.exists, "azurite")
if not cont.exists():
    cont.create_container()
for rel in files:
    with open(BENCH / rel, "rb") as f:
        cont.upload_blob(rel, f, overwrite=True, max_concurrency=4)
print(f"azure: uploaded {len(files)} blobs", flush=True)

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

cat = SqlCatalog(
    "bench",
    uri="sqlite:////tmp/brfs_catalog.db",
    warehouse=f"s3://{BUCKET}/iceberg",
    **{
        "s3.endpoint": S3_EP,
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
    },
)
cat.create_namespace_if_not_exists("b")
pf = pq.ParquetFile(BENCH / "large.parquet")
chunk = max(ROWS_ICE // 4, 1)
batches = pf.iter_batches(batch_size=chunk)
first = pa.Table.from_batches([next(batches)])
if first.num_rows > ROWS_ICE:
    first = first.slice(0, ROWS_ICE)
tbl = cat.create_table("b.events", schema=first.schema)
tbl.append(first)
done = first.num_rows
while done < ROWS_ICE:
    t = pa.Table.from_batches([next(batches)])
    if done + t.num_rows > ROWS_ICE:
        t = t.slice(0, ROWS_ICE - done)
    tbl.append(t)
    done += t.num_rows
meta = cat.load_table("b.events").metadata_location
(BENCH / "iceberg_meta.txt").write_text(meta)
print(f"iceberg: {done} rows, metadata at {meta}", flush=True)
PY
}

prep_remote() {
	write_prep_py
	echo "uploading datasets + building iceberg table ..." >&2
	docker run --rm --network host -u "$(id -u):$(id -g)" -e HOME=/tmp \
		-e BRFS_S3_EP="http://127.0.0.1:${MINIO_PORT}" \
		-e BRFS_AZ_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=${AZ_KEY};BlobEndpoint=http://127.0.0.1:${AZURITE_PORT}/devstoreaccount1;" \
		-e BRFS_BUCKET="$BRFS_BUCKET" -e BRFS_CONTAINER="$BRFS_CONTAINER" \
		-e BRFS_ROWS_ICEBERG="$BRFS_ROWS_ICEBERG" \
		-e PIP_CACHE_DIR=/bench/.pipcache \
		-v "$BRFS_DATA:/bench" "$BRFS_PYTHON_IMAGE" bash -c \
		"pip install -q --target /tmp/deps boto3 azure-storage-blob 'pyiceberg[sql-sqlite]==0.11.1' pyarrow==25.0.1 && PYTHONPATH=/tmp/deps python /bench/prep_remote.py" >&2
}

main() {
	need docker
	need python3
	need curl
	[[ -x /usr/bin/time ]] || die "GNU time not found (apt install time)"
	if [[ " $BRFS_ENGINES " == *" sdb "* ]]; then
		[[ -x "$BRFS_SDB_BIN" ]] || die "$BRFS_SDB_BIN not found: build the 'bench' preset or set BRFS_SDB_BIN"
	fi
	mkdir -p "$BRFS_DATA/logs" "$(dirname "$BRFS_RESULTS")"
	S3_PORT="${BRFS_S3_PORT:-$(free_port)}"
	AZ_PORT="${BRFS_AZ_PORT:-$(free_port)}"
	TOXI_PORT="${BRFS_TOXI_PORT:-$(free_port)}"
	MINIO_PORT="${BRFS_MINIO_PORT:-$(free_port)}"
	AZURITE_PORT="${BRFS_AZURITE_PORT:-$(free_port)}"
	CONSOLE_PORT="$(free_port)"

	ensure_duckdb
	gen_datasets
	SZ_LARGE=$(stat -c%s "$BRFS_DATA/large.parquet")
	SZ_SMALL=$(du -sb "$BRFS_DATA/small" | awk '{print $1}')

	trap net_down EXIT
	net_up
	prep_remote

	row_out engine client backend scenario mode reps median_s p90_s rss_mb mbps >"$BRFS_RESULTS"
	cat "$BRFS_RESULTS"
	echo "results: $BRFS_RESULTS" >&2

	local engine backend scen lat mode group_rows
	for lat in $BRFS_LATENCIES; do
		wan_set "$lat"
		for backend in $BRFS_BACKENDS; do
			for scen in $BRFS_SCENARIOS; do
				group_rows=0
				for mode in $BRFS_MODES; do
					SDB_MED=NA SDB_P90=NA SDB_RSS=NA SDB_MBPS=NA
					for engine in $BRFS_ENGINES; do
						cell_ok "$engine" "$backend" "$scen" || continue
						run_cell "$engine" "$backend" "$scen" "$lat" "$mode"
						group_rows=1
					done
				done
				[[ $group_rows == 1 ]] && echo | tee -a "$BRFS_RESULTS"
			done
		done
	done
	wan_set none
	echo "done: $BRFS_RESULTS" >&2
}

main "$@"
