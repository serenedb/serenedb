#!/usr/bin/env bash
set -uo pipefail
if [ $# -ne 1 ]; then
	echo "usage: small_all.sh <validate|full>" >&2
	exit 2
fi
MODE=$1
source "$(dirname "$0")/lib.sh"
read -ra forward <<<"${HB_SIDES:-main_duck main_search branch}"
backward=()
for side in "${forward[@]}"; do
	rm -f "$HB_RESULTS/$side.$MODE/small.tsv"
	backward=("$side" "${backward[@]}")
done
n=0
for readers in none column index; do
	for pk in pk nopk; do
		if [ "$pk" = pk ]; then
			ops=(insert update delete upsert replace merge)
		else
			ops=(insert update delete merge)
		fi
		for op in "${ops[@]}"; do
			if [ $((n % 2)) -eq 0 ]; then
				order=("${forward[@]}")
			else
				order=("${backward[@]}")
			fi
			n=$((n + 1))
			for side in "${order[@]}"; do
				bash "$HB_DIR/cell.sh" "$side" "$MODE" "$pk" "$op" "$readers" 2>&1 | tail -1
			done
		done
	done
done
