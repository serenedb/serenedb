#include "duckdb.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <algorithm>
#include <atomic>

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/exceptions.h>

#include <optional>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_scanner.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_connection_pool.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

unique_ptr<FunctionData> ClickHouseBindData::Copy() const {
	auto result = make_uniq<ClickHouseBindData>();
	result->params = params;
	result->database = database;
	result->table = table;
	result->sql = sql;
	result->from_query = from_query;
	result->names = names;
	result->types = types;
	result->stringified = stringified;
	result->clickhouse_types = clickhouse_types;
	result->order_by_and_limit = order_by_and_limit;
	result->aggregate = aggregate;
	result->rowid_column = rowid_column;
	result->table_entry = table_entry;
	result->has_cardinality = has_cardinality;
	result->approx_row_count = approx_row_count;
	return std::move(result);
}

void ClickHouseBindData::EnsureOrderKeySafety() {
	if (order_key_safety_computed) {
		return;
	}
	order_key_safety_computed = true;
	auto &unsafe_keys = order_by_and_limit.order_key_unsafe;
	unsafe_keys.assign(types.size(), false);
	for (idx_t i = 0; i < types.size(); i++) {
		unsafe_keys[i] = ColumnPushdownUnsafe(i, true);
	}
}

bool ClickHouseBindData::ColumnPushdownUnsafe(idx_t col, bool for_ordering) const {
	if (col < stringified.size() && stringified[col]) {
		return true;
	}
	const auto ch_type = col < clickhouse_types.size() ? clickhouse_types[col] : std::string();
	return for_ordering ? ClickHouseOrderingUnsafe(types[col], ch_type)
	                    : ClickHouseComparisonUnsafe(types[col], ch_type);
}

bool ClickHouseBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ClickHouseBindData>();
	return database == other.database && table == other.table && sql == other.sql &&
	       from_query == other.from_query && names == other.names;
}

struct ClickHouseGlobalState : public GlobalTableFunctionState {
	ClickHouseGlobalState() = default;
	~ClickHouseGlobalState() override {
		// The RAII PooledConnection returns to its pool only when the stream was
		// fully drained (done): an early abort (e.g. LIMIT) or a mid-scan error
		// leaves unread blocks on the wire, so those connections are invalidated
		// (dropped), never pooled. Ad-hoc scans hold a detached (pool-less)
		// connection that simply closes either way.
		if (!done) {
			connection.Invalidate();
		}
	}

	ClickHousePoolConnection connection;
	std::optional<clickhouse::Block> current_block;
	idx_t block_offset = 0;
	bool done = false;
	//! Server rows decoded so far, for table_scan_progress. Atomic: the progress
	//! callback is polled from a different thread than the one running the scan.
	std::atomic<idx_t> rows_seen {0};
	//! Conjunction of the table filters whose remote rendering is absent or wider than
	//! the filter itself (see TransformFilters). The optimizer erased them from the
	//! plan, so the scan re-applies them to every decoded chunk -- otherwise rows leak
	//! through unfiltered.
	unique_ptr<Expression> local_filter;
	unique_ptr<ExpressionExecutor> local_filter_executor;
	//! The remote SQL this scan streams, kept for error messages.
	std::string remote_sql;

	// One Select cursor (NextBlock) per scan; ClickHouse parallelises server-side.
	idx_t MaxThreads() const override {
		return 1;
	}
};

void ClickHouseDiscoverColumns(ClickHouseConnection &connection, const std::string &describe_sql,
                               vector<LogicalType> &return_types, vector<std::string> &names, bool binary_as_blob,
                               vector<bool> &stringified, vector<std::string> &clickhouse_types) {
	auto &client = connection.GetClient();
	ClickHouseConnection::LogQuery(describe_sql);
	client.BeginSelect(describe_sql);
	while (auto block = client.NextBlock()) {
		idx_t name_idx = block->GetColumnCount();
		idx_t type_idx = block->GetColumnCount();
		for (idx_t c = 0; c < block->GetColumnCount(); c++) {
			if (block->GetColumnName(c) == "name") {
				name_idx = c;
			} else if (block->GetColumnName(c) == "type") {
				type_idx = c;
			}
		}
		if (name_idx == block->GetColumnCount() || type_idx == block->GetColumnCount()) {
			continue;
		}
		auto name_col = (*block)[name_idx]->As<clickhouse::ColumnString>();
		auto type_col = (*block)[type_idx]->As<clickhouse::ColumnString>();
		if (!name_col || !type_col) {
			continue;
		}
		for (idx_t row = 0; row < block->GetRowCount(); row++) {
			std::string col_name(name_col->At(row));
			std::string col_type(type_col->At(row));
			names.push_back(col_name);
			LogicalType logical_type;
			bool needs_to_string = false;
			try {
				logical_type = ClickHouseTypeStringToLogicalType(col_type);
			} catch (const NotImplementedException &) {
				// A column type with no DuckDB mapping must not make the WHOLE table
				// unreadable: degrade this column to VARCHAR; the scan projects
				// toString(col) so the wire carries a plain String.
				logical_type = LogicalType::VARCHAR;
				needs_to_string = true;
			}
			// ch_binary_as_blob: read a top-level String / FixedString as BLOB (raw bytes)
			// rather than VARCHAR, so non-UTF-8 payloads survive intact.
			if (binary_as_blob && (col_type == "String" || col_type.rfind("FixedString(", 0) == 0)) {
				logical_type = LogicalType::BLOB;
			}
			stringified.push_back(needs_to_string);
			clickhouse_types.push_back(col_type);
			return_types.push_back(logical_type);
		}
	}
}

static unique_ptr<FunctionData> ClickHouseBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<std::string> &names) {
	auto bind_data = make_uniq<ClickHouseBindData>();

	auto connection_string = input.inputs[0].GetValue<string>();
	bind_data->params = ClickHouseConnectionParams::FromConnectionString(connection_string);
	bind_data->database = input.inputs[1].GetValue<string>();
	bind_data->table = input.inputs[2].GetValue<string>();

	auto describe_sql =
	    StringUtil::Format("DESCRIBE TABLE %s.%s", ClickHouseQuoteIdentifier(bind_data->database),
	                       ClickHouseQuoteIdentifier(bind_data->table));

	Value binary_setting;
	bool binary_as_blob =
	    context.TryGetCurrentSetting("ch_binary_as_blob", binary_setting) && BooleanValue::Get(binary_setting);
	try {
		auto connection = ClickHouseConnection::Open(bind_data->params);
		ClickHouseDiscoverColumns(connection, describe_sql, return_types, names, binary_as_blob,
		                          bind_data->stringified, bind_data->clickhouse_types);
		// Cardinality estimate for the optimizer, mirroring the catalog path
		// (clickhouse_table_entry): without it an ad-hoc clickhouse_scan reports ~1
		// row and joins plan badly. Stats-only -- a failure must never fail the bind.
		try {
			string count_sql =
			    "SELECT ifNull(total_rows, 0), total_rows IS NOT NULL FROM system.tables WHERE database = " +
			    ClickHouseStringLiteral(bind_data->database) + " AND name = " +
			    ClickHouseStringLiteral(bind_data->table);
			ClickHouseConnection::LogQuery(count_sql);
			connection.GetClient().Select(count_sql, [&](const clickhouse::Block &block) {
				if (block.GetColumnCount() < 2 || block.GetRowCount() == 0) {
					return;
				}
				auto n = block[0]->As<clickhouse::ColumnUInt64>();
				auto known = block[1]->As<clickhouse::ColumnUInt8>();
				if (n && known && known->At(0) != 0) {
					bind_data->approx_row_count = static_cast<idx_t>(n->At(0));
					bind_data->has_cardinality = true;
				}
			});
		} catch (...) {
			// No estimate; leave has_cardinality false.
		}
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("describing table", describe_sql, e);
	}

	bind_data->names = names;
	bind_data->types = return_types;
	return std::move(bind_data);
}

static std::string BuildScanSQL(const ClickHouseBindData &bind_data, const vector<column_t> &column_ids,
                                optional_ptr<TableFilterSet> filters, bool filter_pushdown,
                                vector<idx_t> &inexact_filters) {
	std::string col_names;
	for (auto &column_id : column_ids) {
		if (!col_names.empty()) {
			col_names += ", ";
		}
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			if (bind_data.rowid_column.empty()) {
				col_names += "NULL";
			} else {
				col_names += "toInt64(" + ClickHouseQuoteIdentifier(bind_data.rowid_column) + ")";
			}
		} else {
			auto quoted = ClickHouseQuoteIdentifier(bind_data.names[column_id]);
			if (column_id < bind_data.stringified.size() && bind_data.stringified[column_id]) {
				// Unmapped column type surfaced as VARCHAR: project its text form so the
				// wire carries a plain String.
				col_names += "toString(" + quoted + ")";
			} else {
				col_names += quoted;
			}
		}
	}
	if (col_names.empty()) {
		col_names = "NULL";
	}

	std::string query;
	if (bind_data.from_query) {
		query = StringUtil::Format("SELECT %s FROM (%s)", col_names, bind_data.sql);
	} else {
		query = StringUtil::Format("SELECT %s FROM %s.%s", col_names, ClickHouseQuoteIdentifier(bind_data.database),
		                           ClickHouseQuoteIdentifier(bind_data.table));
	}

	if (filter_pushdown && filters) {
		auto filter_string = ClickHouseFilterPushdown::TransformFilters(column_ids, filters, bind_data, inexact_filters);
		if (!filter_string.empty()) {
			query += " WHERE " + filter_string;
		}
	}
	// ORDER BY / LIMIT clauses the optimizer proved safe to push (see
	// ClickHouseOptimizer; the folded plan node was removed).
	query += bind_data.order_by_and_limit.order_by_clause;
	query += bind_data.order_by_and_limit.limit_clause;
	return query;
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalStateInternal(ClientContext &context,
                                                                              TableFunctionInitInput &input,
                                                                              bool filter_pushdown) {
	auto &bind_data = input.bind_data->Cast<ClickHouseBindData>();
	auto result = make_uniq<ClickHouseGlobalState>();

	vector<idx_t> inexact_filters;
	auto sql = BuildScanSQL(bind_data, input.column_ids, input.filters, filter_pushdown, inexact_filters);

	if (!inexact_filters.empty() && input.filters) {
		// Re-apply the filters the remote WHERE cannot express exactly. Each filter is
		// keyed by its projection position, which is also its column in the output chunk.
		vector<unique_ptr<Expression>> conjuncts;
		for (auto &entry : *input.filters) {
			auto proj_idx = entry.GetIndex();
			if (std::find(inexact_filters.begin(), inexact_filters.end(), proj_idx) == inexact_filters.end()) {
				continue;
			}
			auto column_id = input.column_ids[proj_idx];
			auto column_type = column_id == COLUMN_IDENTIFIER_ROW_ID ? LogicalType::BIGINT : bind_data.types[column_id];
			BoundReferenceExpression column_ref(std::move(column_type), proj_idx);
			conjuncts.push_back(entry.Filter().ToExpression(column_ref));
		}
		if (conjuncts.size() == 1) {
			result->local_filter = std::move(conjuncts[0]);
		} else if (!conjuncts.empty()) {
			auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			conjunction->GetChildrenMutable() = std::move(conjuncts);
			result->local_filter = std::move(conjunction);
		}
		if (result->local_filter) {
			result->local_filter_executor = make_uniq<ExpressionExecutor>(context, *result->local_filter);
		}
	}

	try {
		// A catalog-backed scan leases from that catalog's connection pool (its params
		// match bind_data.params); ad-hoc clickhouse_scan/clickhouse_query open a
		// detached (pool-less) connection that closes on release.
		if (bind_data.table_entry) {
			auto &catalog = bind_data.table_entry->catalog.Cast<ClickHouseCatalog>();
			result->connection = catalog.GetConnectionPool().GetConnection();
		} else {
			result->connection = ClickHousePoolConnection(
			    nullptr, make_uniq<ClickHouseConnection>(ClickHouseConnection::Open(bind_data.params)),
			    std::chrono::steady_clock::now());
		}
		ClickHouseConnection::LogQuery(sql);
		result->connection->GetClient().BeginSelect(ClickHouseConnection::MakeQuery(context, sql));
	} catch (const clickhouse::Error &e) {
		result->connection.Invalidate();
		ClickHouseConnection::ThrowError("starting scan", sql, e);
	}
	result->remote_sql = std::move(sql);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return ClickHouseInitGlobalStateInternal(context, input, false);
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalStateFilterPushdown(ClientContext &context,
                                                                                    TableFunctionInitInput &input) {
	return ClickHouseInitGlobalStateInternal(context, input, true);
}

static void ClickHouseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ClickHouseGlobalState>();
	try {
		while (true) {
			if (gstate.done) {
				output.SetChildCardinality(0);
				return;
			}
			if (!gstate.current_block || gstate.block_offset >= gstate.current_block->GetRowCount()) {
				auto block = gstate.connection->GetClient().NextBlock();
				if (!block) {
					gstate.done = true;
					output.SetChildCardinality(0);
					return;
				}
				gstate.rows_seen.fetch_add(block->GetRowCount(), std::memory_order_relaxed);
				gstate.current_block = std::move(block);
				gstate.block_offset = 0;
				continue;
			}
			auto &block = *gstate.current_block;
			idx_t remaining = block.GetRowCount() - gstate.block_offset;
			idx_t count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
			for (idx_t c = 0; c < output.ColumnCount() && c < block.GetColumnCount(); c++) {
				ClickHouseColumnToVector(*block[c], output.data[c], gstate.block_offset, count);
			}
			output.SetChildCardinality(count);
			gstate.block_offset += count;
			if (gstate.local_filter_executor && count > 0) {
				SelectionVector sel(count);
				idx_t approved = gstate.local_filter_executor->SelectExpression(output, sel);
				if (approved == 0) {
					// Every row of this slice was filtered out; an empty chunk would end
					// the scan prematurely, so pull the next slice instead.
					output.SetChildCardinality(0);
					continue;
				}
				if (approved < count) {
					output.Slice(sel, approved);
					output.Flatten();
					output.SetChildCardinality(approved);
				}
			}
			return;
		}
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("during scan", gstate.remote_sql, e);
	}
}

static BindInfo ClickHouseGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ClickHouseBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table_entry;
	return info;
}

static void ClickHouseScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                    const TableFunction &function) {
	throw NotImplementedException("ClickHouseScanSerialize");
}

static unique_ptr<FunctionData> ClickHouseScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("ClickHouseScanDeserialize");
}

// Report the remote table's approximate row count (captured at bind from
// system.tables.total_rows) so the optimizer can order joins sensibly instead of
// assuming ~1 row. Unknown counts yield no estimate. The postgres analog is
// PostgresScanCardinality.
static unique_ptr<NodeStatistics> ClickHouseScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ClickHouseBindData>();
	if (!bind_data.has_cardinality) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(bind_data.approx_row_count);
}

// Scan progress = rows decoded / approx_row_count (the postgres analog is
// PostgresScanProgress over page indices). Returns -1 when the row count is
// unknown, which DuckDB treats as "indeterminate". A pushed LIMIT/filter makes
// the denominator an over-estimate, so progress can finish below 100 -- capped.
static double ClickHouseScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                                     const GlobalTableFunctionState *gstate_p) {
	auto &bind_data = bind_data_p->Cast<ClickHouseBindData>();
	auto &gstate = gstate_p->Cast<ClickHouseGlobalState>();
	if (!bind_data.has_cardinality || bind_data.approx_row_count == 0) {
		return -1;
	}
	double progress =
	    100.0 * static_cast<double>(gstate.rows_seen.load(std::memory_order_relaxed)) /
	    static_cast<double>(bind_data.approx_row_count);
	return MinValue<double>(100.0, progress);
}

// EXPLAIN rendering: name the remote table and surface the pushed-down ORDER BY /
// LIMIT clauses. "Projections" is deliberately NOT emitted so the framework keeps
// appending its own Projections/Filters sections after these keys.
static InsertionOrderPreservingMap<string> ClickHouseScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Function"] = StringUtil::Upper(input.table_function.name.GetIdentifierName());
	if (!input.bind_data) {
		return result;
	}
	auto &bind_data = input.bind_data->Cast<ClickHouseBindData>();
	if (!bind_data.from_query) {
		result["Table"] = bind_data.database + "." + bind_data.table;
	}
	auto trim_leading = [](const std::string &s) {
		auto pos = s.find_first_not_of(' ');
		return pos == std::string::npos ? s : s.substr(pos);
	};
	if (!bind_data.order_by_and_limit.order_by_clause.empty()) {
		result["Pushed Order"] = trim_leading(bind_data.order_by_and_limit.order_by_clause);
	}
	if (!bind_data.order_by_and_limit.limit_clause.empty()) {
		result["Pushed Limit"] = trim_leading(bind_data.order_by_and_limit.limit_clause);
	}
	return result;
}

ClickHouseScanFunction::ClickHouseScanFunction()
    : TableFunction("clickhouse_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ClickHouseScan, ClickHouseBind, ClickHouseInitGlobalState) {
	serialize = ClickHouseScanSerialize;
	deserialize = ClickHouseScanDeserialize;
	get_bind_info = ClickHouseGetBindInfo;
	cardinality = ClickHouseScanCardinality;
	table_scan_progress = ClickHouseScanProgress;
	to_string = ClickHouseScanToString;
	projection_pushdown = true;
}

ClickHouseScanFunctionFilterPushdown::ClickHouseScanFunctionFilterPushdown()
    : TableFunction("clickhouse_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ClickHouseScan, ClickHouseBind, ClickHouseInitGlobalStateFilterPushdown) {
	serialize = ClickHouseScanSerialize;
	deserialize = ClickHouseScanDeserialize;
	get_bind_info = ClickHouseGetBindInfo;
	cardinality = ClickHouseScanCardinality;
	table_scan_progress = ClickHouseScanProgress;
	to_string = ClickHouseScanToString;
	projection_pushdown = true;
	filter_pushdown = true;
}

} // namespace duckdb
