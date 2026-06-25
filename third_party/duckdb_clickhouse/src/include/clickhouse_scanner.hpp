//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "clickhouse_connection.hpp"

#include <string>

namespace duckdb {

struct ClickHouseBindData : public FunctionData {
	ClickHouseConnectionParams params;
	std::string database;
	std::string table;
	std::string sql;
	bool from_query = false;
	vector<std::string> names;
	//! Column to emit (cast to Int64) for COLUMN_IDENTIFIER_ROW_ID, enabling UPDATE/DELETE.
	//! Empty when the table has no integer primary key usable as a row identifier.
	std::string rowid_column;
	//! The catalog table this scan resolves to (null for ad-hoc clickhouse_scan/clickhouse_query).
	//! Exposed through get_bind_info so the binder recognises it as a base table for UPDATE/DELETE.
	optional_ptr<TableCatalogEntry> table_entry;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

class ClickHouseScanFunction : public TableFunction {
public:
	ClickHouseScanFunction();
};

class ClickHouseScanFunctionFilterPushdown : public TableFunction {
public:
	ClickHouseScanFunctionFilterPushdown();
};

class ClickHouseQueryFunction : public TableFunction {
public:
	ClickHouseQueryFunction();
};

} // namespace duckdb
