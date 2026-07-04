#include "duckdb.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <algorithm>

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/exceptions.h>

#include <optional>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_scanner.hpp"
#include "storage/clickhouse_catalog.hpp"
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
	result->limit = limit;
	result->order_by = order_by;
	result->rowid_column = rowid_column;
	result->table_entry = table_entry;
	result->has_cardinality = has_cardinality;
	result->approx_row_count = approx_row_count;
	return std::move(result);
}

bool ClickHouseBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ClickHouseBindData>();
	return database == other.database && table == other.table && sql == other.sql &&
	       from_query == other.from_query && names == other.names;
}

struct ClickHouseGlobalState : public GlobalTableFunctionState {
	ClickHouseGlobalState() = default;
	~ClickHouseGlobalState() override {
		// Return the connection to the catalog pool only when the stream was fully
		// drained (done): an early abort (e.g. LIMIT) or a mid-scan error leaves
		// unread blocks on the wire, so those connections are dropped, never pooled.
		// Ad-hoc scans (clickhouse_scan/clickhouse_query) have no owning catalog and
		// always drop.
		if (owner_catalog && done && connection.IsOpen()) {
			owner_catalog->ReturnConnection(std::move(connection));
		}
	}

	ClickHouseConnection connection;
	//! The catalog whose pool this scan's connection was leased from (null for ad-hoc scans).
	optional_ptr<ClickHouseCatalog> owner_catalog;
	std::optional<clickhouse::Block> current_block;
	idx_t block_offset = 0;
	bool done = false;
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
                               vector<bool> &stringified) {
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
	    StringUtil::Format("DESCRIBE TABLE `%s`.`%s`", bind_data->database, bind_data->table);

	Value binary_setting;
	bool binary_as_blob =
	    context.TryGetCurrentSetting("ch_binary_as_blob", binary_setting) && BooleanValue::Get(binary_setting);
	try {
		auto connection = ClickHouseConnection::Open(bind_data->params);
		ClickHouseDiscoverColumns(connection, describe_sql, return_types, names, binary_as_blob,
		                          bind_data->stringified);
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("describing table", describe_sql, e);
	}

	bind_data->names = names;
	bind_data->types = return_types;
	return std::move(bind_data);
}

static std::string BuildScanSQL(const ClickHouseBindData &bind_data, const vector<column_t> &column_ids,
                                optional_ptr<TableFilterSet> filters, bool filter_pushdown,
                                vector<idx_t> *inexact_filters = nullptr) {
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
		auto filter_string = ClickHouseFilterPushdown::TransformFilters(column_ids, filters, bind_data.names,
		                                                                inexact_filters, &bind_data.stringified);
		if (!filter_string.empty()) {
			query += " WHERE " + filter_string;
		}
	}
	// A TOP_N row reducer and/or LIMIT/OFFSET the optimizer proved safe to push
	// (see ClickHouseOptimizer); at most one of the two is ever set.
	query += bind_data.order_by;
	query += bind_data.limit;
	return query;
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalStateInternal(ClientContext &context,
                                                                              TableFunctionInitInput &input,
                                                                              bool filter_pushdown) {
	auto &bind_data = input.bind_data->Cast<ClickHouseBindData>();
	auto result = make_uniq<ClickHouseGlobalState>();

	vector<idx_t> inexact_filters;
	auto sql = BuildScanSQL(bind_data, input.column_ids, input.filters, filter_pushdown, &inexact_filters);

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
		// match bind_data.params); ad-hoc clickhouse_scan/clickhouse_query open direct.
		if (bind_data.table_entry) {
			auto &catalog = bind_data.table_entry->catalog.Cast<ClickHouseCatalog>();
			result->owner_catalog = &catalog;
			result->connection = catalog.OpenConnection();
		} else {
			result->connection = ClickHouseConnection::Open(bind_data.params);
		}
		ClickHouseConnection::LogQuery(sql);
		result->connection.GetClient().BeginSelect(sql);
	} catch (const clickhouse::Error &e) {
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
				auto block = gstate.connection.GetClient().NextBlock();
				if (!block) {
					gstate.done = true;
					output.SetChildCardinality(0);
					return;
				}
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

bool IsClickHouseScan(const std::string &name) {
	return name == "clickhouse_scan" || name == "clickhouse_scan_pushdown";
}

// Push a constant LIMIT/OFFSET directly above a clickhouse scan into the remote
// SQL. Correctness gates: (1) only walk through projections -- an intervening
// ORDER BY or FILTER is NOT a projection, so those plans are left alone (a
// remote LIMIT must never precede a local sort/filter); (2) the scan must carry
// no pushed table_filters, since a remote LIMIT applied before a filter that is
// only partially translatable would drop the wrong rows.
static void OptimizeLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}
		if (child.get().type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = child.get().Cast<LogicalGet>();
			bool const_limit = limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE ||
			                   limit.limit_val.Type() == LimitNodeType::UNSET;
			bool const_offset = limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE ||
			                    limit.offset_val.Type() == LimitNodeType::UNSET;
			if (IsClickHouseScan(get.function.name.GetIdentifierName()) && const_limit && const_offset &&
			    !get.table_filters.HasFilters()) {
				auto &bind_data = get.bind_data->Cast<ClickHouseBindData>();
				string clause;
				if (limit.limit_val.Type() != LimitNodeType::UNSET) {
					clause += " LIMIT " + to_string(limit.limit_val.GetConstantValue());
				}
				if (limit.offset_val.Type() != LimitNodeType::UNSET) {
					clause += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
				}
				if (!clause.empty() && bind_data.limit.empty()) {
					bind_data.limit = clause;
					op = std::move(op->children[0]);
					return;
				}
			}
		}
	}
	for (auto &child : op->children) {
		OptimizeLimitPushdown(child);
	}
}

// Annotate a ClickHouse scan under a TOP_N with a remote "ORDER BY ... LIMIT n+offset"
// row reducer. The TOP_N node itself is KEPT -- the local re-sort makes any remote
// ordering discrepancy (collation, tie order) harmless; the remote clause only shrinks
// the transfer from the whole table to limit+offset rows. Gates: every order key must
// resolve (through pure column-reference projections) to a plain, non-stringified scan
// column with explicit ASC/DESC and NULLS placement, the scan must carry no REQUIRED
// table filters (optional/advisory ones never change the row set, so they are fine),
// and nothing may already be folded into the scan.
static void OptimizeTopNPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op->Cast<LogicalTopN>();
		vector<reference<LogicalProjection>> projections;
		reference<LogicalOperator> child = *op->children[0];
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			projections.push_back(child.get().Cast<LogicalProjection>());
			child = *child.get().children[0];
		}
		if (child.get().type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = child.get().Cast<LogicalGet>();
			if (IsClickHouseScan(get.function.name.GetIdentifierName()) && get.bind_data) {
				auto &bind_data = get.bind_data->Cast<ClickHouseBindData>();
				bool only_optional_filters = true;
				for (auto &entry : get.table_filters) {
					if (!ExpressionFilter::IsOptionalFilter(entry.Filter())) {
						only_optional_filters = false;
						break;
					}
				}
				bool foldable = bind_data.order_by.empty() && bind_data.limit.empty() && only_optional_filters &&
				                topn.limit <= NumericLimits<idx_t>::Maximum() - topn.offset;
				string clause;
				for (auto &order : topn.orders) {
					if (!foldable) {
						break;
					}
					if ((order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) ||
					    (order.null_order != OrderByNullType::NULLS_FIRST &&
					     order.null_order != OrderByNullType::NULLS_LAST)) {
						foldable = false;
						break;
					}
					if (order.expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
						foldable = false;
						break;
					}
					auto binding = order.expression->Cast<BoundColumnRefExpression>().Binding();
					for (auto &proj_ref : projections) {
						auto &proj = proj_ref.get();
						if (binding.table_index != proj.table_index ||
						    binding.column_index.GetIndex() >= proj.expressions.size()) {
							foldable = false;
							break;
						}
						auto &proj_expr = *proj.expressions[binding.column_index.GetIndex()];
						if (proj_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
							foldable = false;
							break;
						}
						binding = proj_expr.Cast<BoundColumnRefExpression>().Binding();
					}
					if (!foldable || binding.table_index != get.table_index) {
						foldable = false;
						break;
					}
					auto &column_ids = get.GetColumnIds();
					if (binding.column_index.GetIndex() >= column_ids.size()) {
						foldable = false;
						break;
					}
					auto column_id = column_ids[binding.column_index.GetIndex()].GetPrimaryIndex();
					if (column_id >= bind_data.names.size()) {
						// Virtual column (rowid) or out of range.
						foldable = false;
						break;
					}
					if (column_id < bind_data.stringified.size() && bind_data.stringified[column_id]) {
						// toString() text order differs from the native value order.
						foldable = false;
						break;
					}
					if (!clause.empty()) {
						clause += ", ";
					}
					clause += ClickHouseQuoteIdentifier(bind_data.names[column_id]);
					clause += order.type == OrderType::DESCENDING ? " DESC" : " ASC";
					clause += order.null_order == OrderByNullType::NULLS_FIRST ? " NULLS FIRST" : " NULLS LAST";
				}
				if (foldable && !clause.empty()) {
					bind_data.order_by = " ORDER BY " + clause + " LIMIT " + to_string(topn.limit + topn.offset);
				}
			}
		}
	}
	for (auto &child : op->children) {
		OptimizeTopNPushdown(child);
	}
}

void ClickHouseOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeLimitPushdown(plan);
	OptimizeTopNPushdown(plan);
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
	if (!bind_data.order_by.empty()) {
		result["Pushed Order"] = trim_leading(bind_data.order_by);
	}
	if (!bind_data.limit.empty()) {
		result["Pushed Limit"] = trim_leading(bind_data.limit);
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
	to_string = ClickHouseScanToString;
	projection_pushdown = true;
	filter_pushdown = true;
}

} // namespace duckdb
