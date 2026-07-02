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

struct ClickHouseFilterPushdown {
	//! Render the pushable filters into a remote WHERE clause. When `inexact_filters` is
	//! non-null it receives the projection positions (TableFilterSet keys) of every
	//! filter whose remote rendering is absent or WIDER than the filter itself (partial
	//! AND, unsupported comparison, ...). The optimizer erases fully-pushed filters from
	//! the plan, so the scan MUST re-apply those entries locally -- an untranslated
	//! required filter is otherwise applied nowhere and returns wrong results. Filters
	//! that are optional by design (zonemap/TopN/join advisory prunes) are never
	//! reported: dropping them only loses pruning, not correctness.
	//! `stringified` (parallel to names, optional) marks columns whose ClickHouse type
	//! has no DuckDB mapping and is projected via toString(): their filter subjects are
	//! wrapped the same way so both sides compare the same text.
	static std::string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                    const vector<std::string> &names, vector<idx_t> *inexact_filters = nullptr,
	                                    const vector<bool> *stringified = nullptr);
};

} // namespace duckdb
