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
	//! Render the pushable filters into a remote WHERE clause. Rendering is
	//! all-or-nothing per filter: a filter either renders equivalent SQL or renders
	//! nothing. When `inexact_filters` is non-null it receives the projection
	//! positions (TableFilterSet keys) of every required filter that rendered
	//! nothing (unsupported comparison, unsafe column type, ...). The optimizer
	//! erases fully-pushed filters from the plan, so the scan MUST re-apply those
	//! entries locally -- an untranslated required filter is otherwise applied
	//! nowhere and returns wrong results. Filters
	//! that are optional by design (zonemap/TopN/join advisory prunes) are never
	//! reported: dropping them only loses pruning, not correctness.
	//! `stringified` (parallel to names, optional) marks columns whose ClickHouse type
	//! has no DuckDB mapping and is projected via toString(): their filter subjects are
	//! wrapped the same way so both sides compare the same text.
	//! `types` / `clickhouse_types` (parallel to names, optional) let a column whose
	//! remote comparison diverges from DuckDB's (NaN floats, timezone timestamps, Enum/IP)
	//! be kept fully local instead of pushed -- reported in `inexact_filters` like any
	//! other un-pushable required filter.
	static std::string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                    const vector<std::string> &names, vector<idx_t> *inexact_filters = nullptr,
	                                    const vector<bool> *stringified = nullptr,
	                                    const vector<LogicalType> *types = nullptr,
	                                    const vector<std::string> *clickhouse_types = nullptr);
};

} // namespace duckdb
