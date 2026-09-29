#!/usr/bin/env bash
set -u
data=$1
bin=$2
top=${3:-25}
tmp=$(mktemp -d)
perf script -i "$data" -G -F ip 2>/dev/null | awk '{print "0x" $1}' >"$tmp/ips"
total=$(wc -l <"$tmp/ips")
sort "$tmp/ips" | uniq -c | sort -rn >"$tmp/counts"
awk '{print $2}' "$tmp/counts" | llvm-symbolizer --obj="$bin" --functions=short --no-inlines 2>/dev/null | awk 'NR % 3 == 1' >"$tmp/syms"
paste <(awk '{print $1}' "$tmp/counts") "$tmp/syms" | awk -F'\t' '{a[$2]+=$1} END {for (k in a) print a[k] "\t" k}' | sort -rn | head -"$top" | awk -F'\t' -v t="$total" '{printf "%6.2f%%  %5d  %s\n", 100*$1/t, $1, substr($2,1,110)}'
echo "total samples: $total"
rm -rf "$tmp"
