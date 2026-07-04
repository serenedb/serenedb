#include "storage/clickhouse_update.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction.hpp"
#include "storage/clickhouse_table_entry.hpp"
#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"

#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>

#include <unordered_map>

namespace duckdb {

ClickHouseUpdate::ClickHouseUpdate(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                                   vector<PhysicalIndex> columns_p, vector<unique_ptr<Expression>> expressions_p,
                                   string pk_column_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
      columns(std::move(columns_p)), expressions(std::move(expressions_p)), pk_column(std::move(pk_column_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class ClickHouseUpdateGlobalState : public GlobalSinkState {
public:
	explicit ClickHouseUpdateGlobalState(ClickHouseTableEntry &table) : table(table), update_count(0) {
	}

	ClickHouseTableEntry &table;
	idx_t update_count;
	vector<string> column_names;
	vector<int64_t> ids;
	//! Per update column, the rendered new-value literal for each captured row (parallel to `ids`).
	vector<vector<string>> values;
};

unique_ptr<GlobalSinkState> ClickHouseUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto &ch_table = table.Cast<ClickHouseTableEntry>();
	auto result = make_uniq<ClickHouseUpdateGlobalState>(ch_table);
	for (auto &column : columns) {
		result->column_names.push_back(ch_table.GetColumns().GetColumn(LogicalIndex(column.index)).GetName());
	}
	result->values.resize(columns.size());
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType ClickHouseUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ClickHouseUpdateGlobalState>();
	chunk.Flatten();

	auto &row_identifiers = chunk.data[chunk.ColumnCount() - 1];
	auto row_data = FlatVector::GetData<int64_t>(row_identifiers);

	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not supported for updates of a ClickHouse table");
		}
		D_ASSERT(expressions[i]->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
		auto &value_vector = chunk.data[binding.index];
		for (idx_t r = 0; r < chunk.size(); r++) {
			gstate.values[i].push_back(ClickHouseValueLiteral(value_vector.GetValue(r)));
		}
	}
	for (idx_t r = 0; r < chunk.size(); r++) {
		gstate.ids.push_back(row_data[r]);
	}
	gstate.update_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType ClickHouseUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ClickHouseUpdateGlobalState>();
	if (gstate.ids.empty()) {
		return SinkFinalizeType::READY;
	}

	// The same id can reach the sink several times -- rows sharing a non-unique key, or
	// one row matched repeatedly through a join (UPDATE .. FROM). The keyed multiIf can
	// only express ONE value per id: dedup occurrences whose rendered values agree,
	// refuse when they differ -- per-row identities are inexpressible via the key.
	unordered_map<int64_t, idx_t> first_occurrence;
	vector<idx_t> rows;
	for (idx_t r = 0; r < gstate.ids.size(); r++) {
		auto it = first_occurrence.find(gstate.ids[r]);
		if (it == first_occurrence.end()) {
			first_occurrence.emplace(gstate.ids[r], r);
			rows.push_back(r);
			continue;
		}
		for (idx_t c = 0; c < gstate.column_names.size(); c++) {
			if (gstate.values[c][r] != gstate.values[c][it->second]) {
				throw InvalidInputException(
				    "Cannot UPDATE ClickHouse table \"%s\": the key \"%s\" used as the row identifier is not "
				    "unique, and rows sharing key value %lld would receive different new values",
				    gstate.table.table, pk_column, static_cast<long long>(gstate.ids[r]));
			}
		}
	}

	string pk = ClickHouseQuoteIdentifier(pk_column);
	string id_list;
	for (idx_t i = 0; i < rows.size(); i++) {
		if (i > 0) {
			id_list += ",";
		}
		id_list += to_string(gstate.ids[rows[i]]);
	}

	// multiIf rather than CASE: ClickHouse lowers a `CASE pk WHEN ...` chain to
	// transform(), which rejects wide types (e.g. Int128); multiIf takes any type.
	string assignments;
	for (idx_t c = 0; c < gstate.column_names.size(); c++) {
		if (c > 0) {
			assignments += ", ";
		}
		string col = ClickHouseQuoteIdentifier(gstate.column_names[c]);
		assignments += col + " = multiIf(";
		for (auto r : rows) {
			assignments += pk + " = " + to_string(gstate.ids[r]) + ", " + gstate.values[c][r] + ", ";
		}
		assignments += col + ")";
	}

	string sql = "ALTER TABLE " + ClickHouseQuoteIdentifier(gstate.table.database) + "." +
	             ClickHouseQuoteIdentifier(gstate.table.table) + " UPDATE " + assignments + " WHERE " + pk + " IN (" +
	             id_list + ") SETTINGS mutations_sync = 1";

	auto &transaction = ClickHouseTransaction::Get(context, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	// Guard against mutating rows that merely share a (non-unique) key value. `rows`
	// holds one entry per distinct key. Very large id lists can exceed the server's
	// max_query_size -- a loud error (per-statement settings would lift it).
	VerifyRowIdCoverage(connection, gstate.table.database, gstate.table.table, pk_column, id_list, rows.size(),
	                    "UPDATE");
	try {
		ClickHouseConnection::LogQuery(sql);
		connection.GetClient().Execute(sql);
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error during UPDATE: %s", e.what());
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType ClickHouseUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<ClickHouseUpdateGlobalState>();
	chunk.SetChildCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(gstate.update_count));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string ClickHouseUpdate::GetName() const {
	return "CLICKHOUSE_UPDATE";
}

InsertionOrderPreservingMap<string> ClickHouseUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &ClickHouseCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not supported for updates of a ClickHouse table");
	}
	auto &ch_table = op.table.Cast<ClickHouseTableEntry>();
	string pk_column;
	if (!ch_table.TryGetRowIdColumn(pk_column)) {
		throw BinderException("Cannot UPDATE ClickHouse table \"%s\": it has no single integer PRIMARY KEY to use as "
		                      "a row identifier",
		                      ch_table.name);
	}
	auto &update = planner.Make<ClickHouseUpdate>(op, op.table, std::move(op.columns), std::move(op.expressions),
	                                              std::move(pk_column));
	update.children.push_back(plan);
	return update;
}

} // namespace duckdb
