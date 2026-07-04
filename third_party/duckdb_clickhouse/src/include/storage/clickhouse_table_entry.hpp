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

#include <mutex>

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
	//! Guarded by row_count_lock: concurrent binds of the same entry race otherwise.
	std::mutex row_count_lock;
	idx_t cached_row_count = 0;
	bool cached_row_count_known = false;
	bool row_count_fetched = false;
};

//! Refuse an UPDATE/DELETE whose key values are not unique in the table. A ClickHouse
//! MergeTree ORDER BY key is a sorting prefix, not a uniqueness constraint, so
//! `WHERE pk IN (id_list)` would mutate every row sharing a key value. Matching the
//! server row count against the number of MATCHED rows is unsound -- a join
//! (UPDATE..FROM / DELETE..USING) can emit the same rowid several times, inflating the
//! matched count until unmatched sibling rows slip through -- so the guard is strict:
//! the mutation must touch exactly one server row per distinct key. `id_list` is the
//! comma-joined DEDUPLICATED rowid list, `distinct_keys` its element count; `op` is
//! "UPDATE"/"DELETE".
void VerifyRowIdCoverage(ClickHouseConnection &connection, const string &database, const string &table_name,
                         const string &pk_column, const string &id_list, idx_t distinct_keys, const char *op);

} // namespace duckdb
