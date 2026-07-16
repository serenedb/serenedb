//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include <string>

namespace duckdb {

struct ClickHouseBindData;

struct ClickHouseFilterPushdown {
	//! Render the pushable filters into a remote WHERE clause. Rendering is
	//! all-or-nothing per filter: a filter either renders equivalent SQL or renders
	//! nothing. `inexact_filters` receives the projection positions (TableFilterSet
	//! keys) of every required filter that rendered nothing (unsupported comparison,
	//! unsafe column type, ...). The optimizer erases fully-pushed filters from the
	//! plan, so the scan MUST re-apply those entries locally -- an untranslated
	//! required filter is otherwise applied nowhere and returns wrong results.
	//! Filters that are optional by design (zonemap/TopN/join advisory prunes) are
	//! never reported: dropping them only loses pruning, not correctness.
	static std::string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                    const ClickHouseBindData &bind_data, vector<idx_t> &inexact_filters);
};

} // namespace duckdb
