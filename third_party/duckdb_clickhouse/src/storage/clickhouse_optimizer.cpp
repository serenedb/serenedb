#include "storage/clickhouse_optimizer.hpp"

#include "dbconnector/optimizer/order_by_and_limit_optimizer.hpp"

namespace duckdb {

// The ClickHouse mirror of PostgresOptimizer: run the shared dbconnector
// ORDER BY / LIMIT / TOP_N pushdown over the ClickHouse scans. The shared rule
// folds the clause into the scan's remote SQL and REMOVES the local plan node,
// so it only fires when the remote result is exactly DuckDB's:
//  - order keys whose native ClickHouse ordering differs from DuckDB's are
//    REWRITTEN by the shared optimizer via the ClickHouse dialect (isNaN
//    prefix for float NaN placement, toString for UUID and text-backed
//    columns -- which matches the local values by construction, since the
//    read path surfaces those columns as their toString() text); compound
//    keys nesting a divergent scalar cannot be rewritten and stay local.
//  - fold_limit_with_table_filters=false: a scan carrying REQUIRED table
//    filters refuses LIMIT-bearing folds -- their remote rendering may be
//    inexact and re-applied locally, and a remote LIMIT would cut the stream
//    BEFORE that re-check. Optional (advisory) filters never change the row
//    set, so they do not block.
//
// PostgresOptimizer's other two passes have no ClickHouse analog by design:
//  - DisableParallelLimit: postgres splits a scan into CTID-range tasks, so a
//    pushed LIMIT must force single-task execution. A ClickHouse scan is one
//    Select cursor (MaxThreads() == 1); the server parallelises internally.
//  - the streaming/materialization gather: postgres scans of one catalog share
//    a single transaction connection, so multi-scan plans must materialize or
//    leave the main thread. ClickHouse scans each lease their own pooled
//    connection, so no such coordination is needed.
void ClickHouseOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// Both the catalog scan (clickhouse_scan_pushdown) and the ad-hoc
	// clickhouse_scan share the bind data, so run the rule for each name.
	for (const char *scan_name : {"clickhouse_scan_pushdown", "clickhouse_scan"}) {
		auto config = dbconnector::optimizer::OrderByAndLimitOptimizer::CreateConfig(
		    input.context, "ch_order_pushdown", '`', dbconnector::query::QuoteEscapeStyle::BACKSLASH, scan_name,
		    dbconnector::query::Dialect::ClickHouse);
		// The dialect rewrites divergent order keys remotely (isNaN prefix for
		// floats, toString for text-backed/UUID columns); the scan re-applies
		// inexact filters locally, so LIMIT folds over a filtered scan are refused.
		config.fold_limit_with_table_filters = false;
		dbconnector::optimizer::OrderByAndLimitOptimizer::Optimize(config, input, plan);
	}
}

} // namespace duckdb
