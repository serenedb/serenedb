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
};

} // namespace duckdb
