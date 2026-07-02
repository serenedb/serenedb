//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "clickhouse_connection.hpp"

namespace duckdb {

class ClickHouseTableEntry : public TableCatalogEntry {
public:
	ClickHouseTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	//! If the table has a single integer primary key that fits in int64 (so it can act as a
	//! stable row identifier for UPDATE/DELETE via `WHERE pk IN (...)`), returns true and sets
	//! `result` to its column name. ClickHouse has no physical rowid, so this PK is the rowid.
	bool TryGetRowIdColumn(string &result) const;

public:
	ClickHouseConnectionParams params;
	string database;
	string table;
	//! ClickHouse server-declared column type strings, in column order (for INSERT block building)
	vector<string> clickhouse_types;
	//! Parallel to clickhouse_types: true when the type has no DuckDB mapping and the
	//! column is surfaced as VARCHAR via a remote toString() projection.
	vector<bool> stringified;

private:
	//! Approximate row count from system.tables.total_rows, fetched lazily on the first
	//! scan and cached for the entry's lifetime (clickhouse_clear_cache rebuilds it).
	//! row_count_known is false when the server reports NULL (non-MergeTree engines).
	idx_t cached_row_count = 0;
	bool cached_row_count_known = false;
	bool row_count_fetched = false;
};

//! Refuse an UPDATE/DELETE that would mutate rows the statement did not target. A
//! ClickHouse MergeTree ORDER BY key is a sorting prefix, not a uniqueness constraint,
//! so `WHERE pk IN (id_list)` can match rows that merely share a key value. Counts the
//! rows the mutation would touch and throws if that exceeds `affected_count` (the rows
//! actually selected). `id_list` is the comma-joined rowid list; `op` is "UPDATE"/"DELETE".
void VerifyRowIdCoverage(ClickHouseConnection &connection, const string &database, const string &table_name,
                         const string &pk_column, const string &id_list, idx_t affected_count, const char *op);

} // namespace duckdb
