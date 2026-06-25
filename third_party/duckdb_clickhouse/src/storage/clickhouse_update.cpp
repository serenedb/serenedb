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

namespace duckdb {

ClickHouseUpdate::ClickHouseUpdate(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                                   vector<PhysicalIndex> columns_p, vector<unique_ptr<Expression>> expressions_p,
                                   string pk_column_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
      columns(std::move(columns_p)), expressions(std::move(expressions_p)), pk_column(std::move(pk_column_p)) {
}

// Render a DuckDB Value as a ClickHouse SQL literal for use in an ALTER ... UPDATE expression.
static string RenderClickHouseLiteral(const Value &value) {
	if (value.IsNull()) {
		return "NULL";
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return value.GetValue<bool>() ? "1" : "0";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return value.ToString();
	case LogicalTypeId::VARCHAR:
		return ClickHouseStringLiteral(value.ToString());
	case LogicalTypeId::BLOB: {
		// value.ToString() on a BLOB yields a hex-escaped TEXT rendering (\\x00...),
		// which would be stored literally instead of the raw bytes. Emit the raw
		// bytes via ClickHouse unhex('...') so arbitrary binary round-trips exactly.
		auto bytes = value.GetValueUnsafe<string_t>();
		static const char digits[] = "0123456789ABCDEF";
		string hex;
		hex.reserve(bytes.GetSize() * 2);
		for (idx_t i = 0; i < bytes.GetSize(); i++) {
			auto b = static_cast<unsigned char>(bytes.GetData()[i]);
			hex += digits[b >> 4];
			hex += digits[b & 0x0F];
		}
		return "unhex('" + hex + "')";
	}
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return ClickHouseStringLiteral(value.ToString());
	default:
		throw NotImplementedException("UPDATE of ClickHouse column with type %s is not supported",
		                              value.type().ToString());
	}
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
			gstate.values[i].push_back(RenderClickHouseLiteral(value_vector.GetValue(r)));
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

	string pk = ClickHouseQuoteIdentifier(pk_column);
	string id_list;
	for (idx_t r = 0; r < gstate.ids.size(); r++) {
		if (r > 0) {
			id_list += ",";
		}
		id_list += to_string(gstate.ids[r]);
	}

	string assignments;
	for (idx_t c = 0; c < gstate.column_names.size(); c++) {
		if (c > 0) {
			assignments += ", ";
		}
		string col = ClickHouseQuoteIdentifier(gstate.column_names[c]);
		assignments += col + " = CASE " + pk;
		for (idx_t r = 0; r < gstate.ids.size(); r++) {
			assignments += " WHEN " + to_string(gstate.ids[r]) + " THEN " + gstate.values[c][r];
		}
		assignments += " ELSE " + col + " END";
	}

	string sql = "ALTER TABLE " + ClickHouseQuoteIdentifier(gstate.table.database) + "." +
	             ClickHouseQuoteIdentifier(gstate.table.table) + " UPDATE " + assignments + " WHERE " + pk + " IN (" +
	             id_list + ") SETTINGS mutations_sync = 1";

	auto &transaction = ClickHouseTransaction::Get(context, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	try {
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
