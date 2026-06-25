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
	static std::string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                    const vector<std::string> &names);
};

} // namespace duckdb
