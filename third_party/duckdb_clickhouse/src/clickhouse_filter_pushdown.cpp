#include "duckdb.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "dbconnector/query/query_writer.hpp"
#include "dbconnector/table_scan/filter_pushdown.hpp"

#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_scanner.hpp"

namespace duckdb {

string ClickHouseFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                                  const ClickHouseBindData &bind_data, vector<idx_t> &inexact_filters) {
	if (!filters || !filters->HasFilters()) {
		return string();
	}
	// ClickHouse dialect for the shared filter renderer: backtick identifiers,
	// single-quoted constants, backslash escaping (CH treats backslash as an escape
	// inside both `...` and '...'), BLOB as unhex('HEX'), and the ClickHouse dialect
	// flag (no COLLATE, tupleElement(...) for Tuple fields). `auto` because the
	// Config type is private to FilterPushdown (only CreateConfig may mint one).
	auto config = dbconnector::table_scan::FilterPushdown::CreateConfig(
	    '`', '\'', dbconnector::query::QuoteEscapeStyle::BACKSLASH, "unhex('", ")",
	    dbconnector::query::Dialect::ClickHouse);
	string result;
	for (auto &entry : *filters) {
		auto column_id = column_ids[entry.GetIndex()];
		auto &filter = entry.Filter();
		string filter_text;

		// Keep the filter local (do not push) when:
		//  - virtual column (rowid; checked FIRST -- its column_id is out of bounds
		//    for the per-column metadata), or
		//  - ColumnPushdownUnsafe: the column's type makes a remote comparison
		//    diverge from DuckDB's (NaN floats, timezone-parsed timestamps, Enum/IP
		//    text, decimal) -- pushing could wrongly EXCLUDE rows the local re-check
		//    can't recover -- or the column is stringified (surfaced as VARCHAR via
		//    toString()): the shared renderer quotes the bare column, so it cannot
		//    inject toString().
		if (!IsVirtualColumn(column_id) && !bind_data.ColumnPushdownUnsafe(column_id, false)) {
			filter_text = dbconnector::table_scan::FilterPushdown::TransformFilter(config, bind_data.names[column_id],
			                                                                       filter, column_id);
		}

		if (filter_text.empty()) {
			// The remote WHERE misses this required filter, and the optimizer erased
			// it from the plan believing the scan applies it -- the scan must
			// re-apply it locally or rows leak through unfiltered.
			if (!ExpressionFilter::IsOptionalFilter(filter)) {
				inexact_filters.push_back(entry.GetIndex());
			}
			continue;
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += filter_text;
	}
	return result;
}

} // namespace duckdb
