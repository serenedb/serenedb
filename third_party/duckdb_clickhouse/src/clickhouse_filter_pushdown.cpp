#include "duckdb.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "dbconnector/query/query_writer.hpp"
#include "dbconnector/table_scan/filter_pushdown.hpp"

#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_types.hpp"

namespace duckdb {

string ClickHouseFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                                  const vector<std::string> &names, vector<idx_t> *inexact_filters,
                                                  const vector<bool> *stringified, const vector<LogicalType> *types,
                                                  const vector<std::string> *clickhouse_types) {
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
		bool exact = true;

		// Keep the filter local (do not push) when:
		//  - virtual column (rowid), or
		//  - the column's type makes a remote comparison diverge from DuckDB's
		//    (NaN floats, timezone-parsed timestamps, Enum/IP text, decimal) --
		//    pushing could wrongly EXCLUDE rows the local re-check can't recover, or
		//  - the column is stringified (surfaced as VARCHAR via toString()): the
		//    shared renderer quotes the bare column, so it cannot inject toString().
		std::string ch_type;
		if (clickhouse_types && column_id < clickhouse_types->size()) {
			ch_type = (*clickhouse_types)[column_id];
		}
		bool comparison_unsafe =
		    types && column_id < types->size() && ClickHouseComparisonUnsafe((*types)[column_id], ch_type);
		bool is_stringified = stringified && column_id < stringified->size() && (*stringified)[column_id];
		if (IsVirtualColumn(column_id) || comparison_unsafe || is_stringified) {
			exact = false;
		} else {
			auto rendered = dbconnector::table_scan::FilterPushdown::TransformFilter(config, names[column_id], filter,
			                                                                         column_id);
			filter_text = std::move(rendered.sql);
			exact = rendered.exact;
		}

		if ((filter_text.empty() || !exact) && inexact_filters && !ExpressionFilter::IsOptionalFilter(filter)) {
			// The remote WHERE misses (part of) this required filter, and the optimizer
			// erased it from the plan believing the scan applies it -- the scan must
			// re-apply it locally or rows leak through unfiltered.
			inexact_filters->push_back(entry.GetIndex());
		}
		if (filter_text.empty()) {
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
