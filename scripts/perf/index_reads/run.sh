#!/usr/bin/env bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
DATA=${SERENEDB_INDEX_READS_DATA:-$HOME/.cache/serenedb-index-reads}
BIN=${BUILD_DIR:-$REPO/build_perf}/bin
ROW_GROUP=${ROW_GROUP:-122880}
WORKERS=${WORKERS:-$(($(nproc) / 2))}
CORPUS_URL='https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=1'
QUERIES_URL='https://raw.githubusercontent.com/quickwit-oss/search-benchmark-game/master/queries.txt'

prepare() {
	mkdir -p "$DATA"
	[[ -f $DATA/wiki-articles.json.bz2 ]] || curl -sSfL -o "$DATA/wiki-articles.json.bz2" "$CORPUS_URL"
	[[ -f $DATA/queries.txt ]] || curl -sSfL -o "$DATA/queries.txt" "$QUERIES_URL"
	[[ -f $DATA/corpus.json ]] || python3 -I "$HERE/make_corpus.py" "$DATA/wiki-articles.json.bz2" "$DATA/corpus.json"
}

build() {
	local name=$1 layout=$2
	local dir=$DATA/$name
	rm -rf "$dir"
	mkdir -p "$dir"
	(cd "$dir" && "$BIN/search-benchmark-game-build" "$WORKERS" "$layout" "$ROW_GROUP" <"$DATA/corpus.json" >build.log 2>build.err)
	echo "$name: $(grep -E 'Number of (documents|segments)' "$dir/build.log" | tr '\n' ' ')"
}

measure() {
	local dir=$DATA/$1
	python3 -I "$HERE/measure.py" "$BIN/search-benchmark-game-query" "$dir/idx" "$DATA/queries.txt" "$dir/reads.jsonl" count,top_100 "$WORKERS" 2>"$dir/measure.log"
	python3 -I "$HERE/analyze.py" "$dir/reads.jsonl" >"$dir/analysis.txt"
	python3 -I "$HERE/model.py" "$dir/reads.jsonl" >"$dir/model.txt"
	echo "$1: $dir/analysis.txt $dir/model.txt"
}

case ${1:-} in
prepare) prepare ;;
build) build "$2" "$3" ;;
measure) measure "$2" ;;
all)
	prepare
	build single single
	build flushed segment_docs=100000000
	build small segment_docs=78627
	for name in single flushed small; do
		measure "$name"
	done
	;;
*)
	echo "usage: $0 prepare | build <name> <single|tiered|segment_docs=N> | measure <name> | all" >&2
	exit 1
	;;
esac
