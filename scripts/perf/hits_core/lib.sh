HB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MAIN_BIN=${MAIN_BIN:?set MAIN_BIN to main's serened (a perf build)}
BRANCH_BIN=${BRANCH_BIN:?set BRANCH_BIN to the branch's serened (a perf build)}
HITS_DIR=${HITS_DIR:?set HITS_DIR to the directory with hits_0.parquet ... hits_<N-1>.parquet}
HB_OUT=${HB_OUT:-$PWD/hits_core}
HB_RESULTS=$HB_OUT/results
HB_DATA_ROOT=${HB_DATA_ROOT:-$HB_OUT}
HITS_FILES=${HITS_FILES:-10}
CLK_TCK=$(getconf CLK_TCK)
NEW_ID_SHIFT=4611686018427387904
mkdir -p "$HB_RESULTS" "$HB_DATA_ROOT"

full_or_validate() {
	if [ "$MODE" = validate ]; then
		EVENTS_ROWS=20000
		PGB_T=3
		WRITERS=2
		READERS=1
		REPEAT=2
	else
		EVENTS_ROWS=${HB_EVENTS_ROWS:-1000000}
		PGB_T=${HB_SECONDS:-30}
		WRITERS=${HB_WRITERS:-16}
		READERS=${HB_READERS:-4}
		REPEAT=${HB_REPEAT:-5}
	fi
}

side_bin() {
	case $1 in
	main_duck | main_search) echo "$MAIN_BIN" ;;
	branch) echo "$BRANCH_BIN" ;;
	*)
		echo "unknown side $1, expected main_duck, main_search or branch" >&2
		return 1
		;;
	esac
}

side_with() {
	if [ "$1" = main_search ]; then
		echo " WITH (storage = 'search')"
	fi
}

free_port() {
	local p
	for p in $(seq 7400 7499); do
		if [ -z "$(ss -ltnH "sport = :$p")" ]; then
			echo "$p"
			return 0
		fi
	done
	return 1
}

start_server() {
	local side=$1 dir=$2 log=$3 bin i
	bin=$(side_bin "$side")
	PORT=$(free_port)
	"$bin" "$dir" --listen="postgres://127.0.0.1:$PORT" >"$log" 2>&1 &
	SERVER_PID=$!
	PSQL=(psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -q -v ON_ERROR_STOP=1)
	for i in $(seq 1 240); do
		if "${PSQL[@]}" -Atc 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			echo "serened exited, see $log" >&2
			return 1
		fi
		timeout 0.5 tail -f /dev/null || true
	done
	echo "serened did not come up, see $log" >&2
	return 1
}

stop_server() {
	if [ -n "${SERVER_PID:-}" ]; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
		SERVER_PID=
	fi
}

server_ticks() {
	awk '{print $14 + $15}' "/proc/$SERVER_PID/stat"
}

IDLE_WINDOW=${IDLE_WINDOW:-3}
settle() {
	local start t0 last now idle_since="" now_s idle_ticks=0
	start=$(date +%s.%N)
	t0=$(server_ticks)
	last=$t0
	for _ in $(seq 1 600); do
		timeout 0.5 tail -f /dev/null || true
		now=$(server_ticks)
		now_s=$(date +%s.%N)
		if [ $((now - last)) -le $((CLK_TCK / 40)) ]; then
			if [ -z "$idle_since" ]; then
				idle_since=$now_s
				idle_ticks=$last
			fi
			if [ "$(echo "$now_s - $idle_since >= $IDLE_WINDOW" | bc)" = 1 ]; then
				SETTLE_S=$(echo "$idle_since - $start" | bc)
				BG_CPU_S=$(echo "scale=2; ($idle_ticks - $t0) / $CLK_TCK" | bc)
				return 0
			fi
		else
			idle_since=""
		fi
		last=$now
	done
	SETTLE_S=$(echo "$(date +%s.%N) - $start" | bc)
	BG_CPU_S=$(echo "scale=2; ($(server_ticks) - $t0) / $CLK_TCK" | bc)
}

record() {
	local IFS=$'\t'
	printf '%s\n' "$*" >>"$RESULTS"
}

run_timed() {
	local name=$1 sql=$2 out c0 c1 ms cpu
	c0=$(server_ticks)
	if ! out=$("${PSQL[@]}" -c '\timing on' -c "$sql" 2>&1); then
		record "$name" FAIL "$(printf '%s' "$out" | tr '\n\t' '  ' | cut -c1-300)"
		echo "FAIL $name: $out" >&2
		return 0
	fi
	c1=$(server_ticks)
	ms=$(printf '%s\n' "$out" | awk '/^Time: /{s += $2} END{printf "%.1f", s}')
	cpu=$(echo "scale=2; ($c1 - $c0) / $CLK_TCK" | bc)
	settle
	record "$name" "$ms" "$cpu" "$BG_CPU_S" "$SETTLE_S"
	echo "$name: ${ms} ms, cpu ${cpu} s, background ${BG_CPU_S} s over ${SETTLE_S} s"
}

q() {
	"${PSQL[@]}" -c "$1"
}

qa() {
	"${PSQL[@]}" -Atc "$1"
}
