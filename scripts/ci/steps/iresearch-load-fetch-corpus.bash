#!/bin/bash
# Fetch the iresearch-load corpus from S3-compatible storage and cache it under
# $IRESEARCH_LOAD_CACHE_DIR. Idempotent: a warm cache is a no-op.
#
# Required env:
#   SEARCHBENCH_S3_KEY_ID      access key
#   SEARCHBENCH_S3_SECRET      secret key
#
# Optional env (with defaults):
#   IRESEARCH_LOAD_DATASET        wiki_small
#   IRESEARCH_LOAD_CACHE_DIR      /mnt/data/searchbench
#   SEARCHBENCH_S3_BUCKET      srn-test-playground-eur-west1-001
#   SEARCHBENCH_S3_PREFIX      searchbench
#   SEARCHBENCH_S3_ENDPOINT    https://storage.googleapis.com
#   SEARCHBENCH_S3_REGION      auto
#
# Writes CORPUS_PATH=<path> to $GITHUB_ENV (when set) and prints the path on stdout.

set -euo pipefail

: "${IRESEARCH_LOAD_DATASET:=wiki_small}"
: "${IRESEARCH_LOAD_CACHE_DIR:=/mnt/data/searchbench}"
: "${SEARCHBENCH_S3_BUCKET:=srn-test-playground-eur-west1-001}"
: "${SEARCHBENCH_S3_PREFIX:=searchbench}"
: "${SEARCHBENCH_S3_ENDPOINT:=https://storage.googleapis.com}"
: "${SEARCHBENCH_S3_REGION:=auto}"

if [[ -z "${SEARCHBENCH_S3_KEY_ID:-}" || -z "${SEARCHBENCH_S3_SECRET:-}" ]]; then
	echo "ERROR: SEARCHBENCH_S3_KEY_ID / SEARCHBENCH_S3_SECRET not set" >&2
	exit 1
fi

dataset_dir="$IRESEARCH_LOAD_CACHE_DIR/$IRESEARCH_LOAD_DATASET"
corpus_path="$dataset_dir/corpus.json"
lock_file="$dataset_dir/.fetch.lock"
mkdir -p "$dataset_dir"

# Serialize concurrent jobs on the same runner so they share one download.
exec 9>"$lock_file"
flock 9

if [[ -s "$corpus_path" ]]; then
	echo "Corpus already cached: $corpus_path ($(du -h "$corpus_path" | cut -f1))" >&2
else
	s3_uri="s3://$SEARCHBENCH_S3_BUCKET/$SEARCHBENCH_S3_PREFIX/$IRESEARCH_LOAD_DATASET/corpus.json.gz"
	tmp_path="$corpus_path.partial"
	echo "Fetching $s3_uri (endpoint=$SEARCHBENCH_S3_ENDPOINT)" >&2

	rm -f "$tmp_path"
	trap 'rm -f "$tmp_path"' EXIT

	AWS_ACCESS_KEY_ID="$SEARCHBENCH_S3_KEY_ID" \
		AWS_SECRET_ACCESS_KEY="$SEARCHBENCH_S3_SECRET" \
		AWS_DEFAULT_REGION="$SEARCHBENCH_S3_REGION" \
		AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
		AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
		aws s3 cp "$s3_uri" - --endpoint-url "$SEARCHBENCH_S3_ENDPOINT" |
		gunzip >"$tmp_path"

	mv "$tmp_path" "$corpus_path"
	trap - EXIT
	echo "Cached: $corpus_path ($(du -h "$corpus_path" | cut -f1))" >&2
fi

if [[ -n "${GITHUB_ENV:-}" ]]; then
	echo "CORPUS_PATH=$corpus_path" >>"$GITHUB_ENV"
fi
echo "$corpus_path"
