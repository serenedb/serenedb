SCHEMA_FILE=$HB_DIR/hits_schema.txt

hits_ddl_cols() {
	awk '{
    t = $2
    if (t == "VARCHAR") t = "TEXT"
    if (t == "USMALLINT") t = "INTEGER"
    printf "%s%s %s", (NR > 1 ? ", " : ""), tolower($1), t
  }' "$SCHEMA_FILE"
}

hits_select() {
	local id_expr=${1:-}
	awk -v id="$id_expr" '{
    e = "\"" $1 "\""
    if ($2 == "USMALLINT") e = e "::INTEGER"
    if ($1 == "WatchID" && id != "") e = id
    printf "%s%s AS %s", (NR > 1 ? ", " : ""), e, tolower($1)
  }' "$SCHEMA_FILE"
}

hits_cols() {
	local prefix=${1:-}
	awk -v p="$prefix" '{ printf "%s%s%s", (NR > 1 ? ", " : ""), p, tolower($1) }' "$SCHEMA_FILE"
}

hits_cols_replace() {
	local column=$1 expr=$2
	awk -v c="$column" -v e="$expr" '{
    n = tolower($1)
    printf "%s%s", (NR > 1 ? ", " : ""), (n == c ? e " AS " n : n)
  }' "$SCHEMA_FILE"
}

parquet_src() {
	local files=$1
	echo "read_parquet($files, binary_as_string = true)"
}

hits_files() {
	local n=$1 i list=""
	for i in $(seq 0 $((n - 1))); do
		list+="${list:+, }'$HITS_DIR/hits_$i.parquet'"
	done
	echo "[$list]"
}
