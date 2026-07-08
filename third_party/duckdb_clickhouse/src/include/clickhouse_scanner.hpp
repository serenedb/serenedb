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

#include "dbconnector/bind_data.hpp"

#include "clickhouse_connection.hpp"

#include <string>

namespace duckdb {

struct ClickHouseBindData : public dbconnector::BindData {
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
	//! Remote ORDER BY / LIMIT clauses folded in by the shared dbconnector
	//! OrderByAndLimitOptimizer (the folded plan node is removed; ClickHouse-unsafe
	//! order keys and limit-over-local-refilter cases are vetoed via the Config
	//! hooks in ClickHouseOptimizer::Optimize, so a fold only happens when the
	//! remote result is exactly DuckDB's).
	dbconnector::optimizer::OrderByAndLimitBindData order_by_and_limit;
	//! Required by the dbconnector::BindData contract; the aggregate optimizer is
	//! not registered for ClickHouse (postgres does not register it either).
	dbconnector::optimizer::AggregateBindData aggregate;
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

	dbconnector::optimizer::OrderByAndLimitBindData &GetOrderByAndLimitBindData() override {
		return order_by_and_limit;
	}
	dbconnector::optimizer::AggregateBindData &GetAggregateBindData() override {
		return aggregate;
	}

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

//! Optimizer extension: runs the shared dbconnector OrderByAndLimitOptimizer
//! over the ClickHouse scans (mirrors PostgresOptimizer, minus the
//! ctid/parallelism handling ClickHouse does not need), with ClickHouse-specific
//! safety vetoes: order keys whose remote ordering diverges from DuckDB's
//! (ClickHouseOrderingUnsafe, toString()-stringified columns) refuse the fold,
//! and LIMIT-carrying folds refuse when the scan carries required table filters
//! (their remote rendering may be inexact and re-applied locally AFTER the
//! remote LIMIT already cut the stream).
struct ClickHouseOptimizer {
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
