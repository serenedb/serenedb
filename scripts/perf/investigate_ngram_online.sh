#!/usr/bin/env bash
# Perf-record over serened during the 200K-row ngram online INSERT phase.
# This is where the +12.5% CPU regression showed up in the clean-machine sweep.
set -euo pipefail
BIN="$1"
LABEL="$2"
OUT="$3"
PORT="${4:-6193}"
ROWS="${ROWS:-200000}"
mkdir -p "$OUT"
export PGPASSWORD=postgres

DATA="/tmp/perf-ngramonline-${USER}-${LABEL}"
rm -rf "$DATA"
mkdir -p "$DATA"
"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" >"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT
for _ in $(seq 1 60); do
	psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.3
done
PSQL="psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"

# Build vocab + empty table + index (same prep as sweep's online phase)
$PSQL <<<'\i scripts/perf/iresearch_sweep/cases/_vocab.sql' >/dev/null
$PSQL <<<'
DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_ngram;
CREATE TEXT SEARCH DICTIONARY sweep_ngram(
  template = '"'"'ngram'"'"', mingram = 2, maxgram = 3,
  preserveoriginal = false, frequency = true, position = true);
CREATE TABLE sweep_t (pk INTEGER PRIMARY KEY, body VARCHAR);
' >/dev/null
$PSQL -c "CREATE INDEX sweep_idx ON sweep_t USING inverted(pk, body sweep_ngram);" >/dev/null

# Build the bulk INSERT
INSERT_SQL="INSERT INTO sweep_t SELECT id, (SELECT string_agg(word, ' ') FROM (SELECT v.word FROM range(50) AS t(word_idx) JOIN sweep_vocab v ON v.idx = ((hash(id::BIGINT * 1009 + word_idx) % 256 + 256) % 256)) t) FROM range(${ROWS}) AS r(id);"

echo "record + INSERT (${ROWS} rows)..."
perf record -F 999 -e cycles:P -p "$SERVED_PID" \
	-o "$OUT/${LABEL}.perf.data" -- \
	bash -c "$PSQL <<<\"$INSERT_SQL\" >/dev/null" 2>"$OUT/${LABEL}_perf.stderr"

perf report -i "$OUT/${LABEL}.perf.data" --stdio --no-children --no-inline \
	-g none --percent-limit 0.3 >"$OUT/${LABEL}.report" 2>&1

echo "done: $OUT/${LABEL}.report"
