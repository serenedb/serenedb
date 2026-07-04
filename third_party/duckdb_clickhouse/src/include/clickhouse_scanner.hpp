//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "duckdb/optimizer/optimizer_extension.hpp"

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
	//! DuckDB types per table column (parallel to names); used to build the local
	//! re-apply expressions for filters the remote WHERE cannot express exactly.
	vector<LogicalType> types;
	//! Per table column (parallel to names): true when the ClickHouse type has no
	//! DuckDB mapping and the column degrades to VARCHAR -- the scan then projects
	//! toString(col) so one exotic column cannot make the whole table unreadable.
	vector<bool> stringified;
	//! Server-declared ClickHouse type string per table column (parallel to names).
	//! Lets pushdown detect types whose remote comparison/ordering diverges from
	//! DuckDB's (Enum/IPv4/IPv6/JSON), which the VARCHAR-mapped DuckDB type hides.
	//! May be empty (older bind paths); callers must bounds-check.
	vector<std::string> clickhouse_types;
	//! Remote LIMIT/OFFSET clause pushed down by ClickHouseOptimizer (empty = none).
	std::string limit;
	//! Remote "ORDER BY ... LIMIT n+offset" row reducer annotated by ClickHouseOptimizer
	//! under a TOP_N that is KEPT in the plan: the local re-sort makes any remote
	//! ordering discrepancy harmless while the transfer shrinks to limit+offset rows.
	std::string order_by;
	//! Column to emit (cast to Int64) for COLUMN_IDENTIFIER_ROW_ID, enabling UPDATE/DELETE.
	//! Empty when the table has no integer primary key usable as a row identifier.
	std::string rowid_column;
	//! The catalog table this scan resolves to (null for ad-hoc clickhouse_scan/clickhouse_query).
	//! Exposed through get_bind_info so the binder recognises it as a base table for UPDATE/DELETE.
	optional_ptr<TableCatalogEntry> table_entry;
	//! Approximate row count (system.tables.total_rows) reported to the optimizer as a
	//! cardinality estimate. has_cardinality is false when the count is unknown (e.g. a
	//! View / Distributed engine reports NULL total_rows) -- then no estimate is given.
	bool has_cardinality = false;
	idx_t approx_row_count = 0;

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

//! clickhouse_execute(connection_string, sql): run an arbitrary side-effecting
//! statement (DDL/DML/OPTIMIZE/etc.) against the server, returning a single
//! BOOLEAN "Success" row. The postgres_execute analog.
class ClickHouseExecuteFunction : public TableFunction {
public:
	ClickHouseExecuteFunction();
};

//! clickhouse_clear_cache(): drop all cached ClickHouse catalog metadata so the
//! next query re-reads it from the server (picks up remote schema drift). The
//! pg_clear_cache analog.
class ClickHouseClearCacheFunction : public TableFunction {
public:
	ClickHouseClearCacheFunction();
};

//! True for the connector's own scan table functions.
bool IsClickHouseScan(const std::string &name);

//! Optimizer extension: pushes a constant LIMIT/OFFSET into the remote scan SQL
//! (mirrors PostgresOptimizer, minus the ctid/parallelism handling ClickHouse
//! does not need).
struct ClickHouseOptimizer {
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
