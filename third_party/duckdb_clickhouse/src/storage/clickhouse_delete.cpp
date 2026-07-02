#include "storage/clickhouse_delete.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction.hpp"
#include "storage/clickhouse_table_entry.hpp"
#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"

#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>

namespace duckdb {

ClickHouseDelete::ClickHouseDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                                   idx_t row_id_index, string pk_column)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
      row_id_index(row_id_index), pk_column(std::move(pk_column)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class ClickHouseDeleteGlobalState : public GlobalSinkState {
public:
	explicit ClickHouseDeleteGlobalState(ClickHouseTableEntry &table) : table(table), delete_count(0) {
	}

	ClickHouseTableEntry &table;
	string id_list;
	idx_t delete_count;
	//! Number of rowids currently accumulated in id_list (its element count), for the
	//! shared-key coverage guard.
	idx_t id_count = 0;

	void Flush(ClientContext &context) {
		if (id_list.empty()) {
			return;
		}
		auto &transaction = ClickHouseTransaction::Get(context, table.catalog);
		auto &connection = transaction.GetConnection();
		// Guard against deleting rows that merely share a (non-unique) key value.
		VerifyRowIdCoverage(connection, table.database, table.table, pk_column, id_list, id_count, "DELETE");
		string sql = "ALTER TABLE " + ClickHouseQuoteIdentifier(table.database) + "." +
		             ClickHouseQuoteIdentifier(table.table) + " DELETE WHERE " +
		             ClickHouseQuoteIdentifier(pk_column) + " IN (" + id_list + ") SETTINGS mutations_sync = 1";
		try {
			ClickHouseConnection::LogQuery(sql);
			connection.GetClient().Execute(sql);
		} catch (const clickhouse::Error &e) {
			throw IOException("ClickHouse error during DELETE: %s", e.what());
		}
		id_list = "";
		id_count = 0;
	}

	string pk_column;
};

unique_ptr<GlobalSinkState> ClickHouseDelete::GetGlobalSinkState(ClientContext &context) const {
	auto &ch_table = table.Cast<ClickHouseTableEntry>();
	auto result = make_uniq<ClickHouseDeleteGlobalState>(ch_table);
	result->pk_column = pk_column;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType ClickHouseDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ClickHouseDeleteGlobalState>();

	chunk.Flatten();
	auto &row_identifiers = chunk.data[row_id_index];
	auto row_data = FlatVector::GetData<int64_t>(row_identifiers);
	for (idx_t i = 0; i < chunk.size(); i++) {
		if (!gstate.id_list.empty()) {
			gstate.id_list += ",";
		}
		gstate.id_list += to_string(row_data[i]);
		gstate.id_count++;
		if (gstate.id_list.size() > 1000000) {
			gstate.Flush(context.client);
		}
	}
	gstate.delete_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType ClickHouseDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ClickHouseDeleteGlobalState>();
	gstate.Flush(context);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType ClickHouseDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<ClickHouseDeleteGlobalState>();
	chunk.SetChildCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(gstate.delete_count));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string ClickHouseDelete::GetName() const {
	return "CLICKHOUSE_DELETE";
}

InsertionOrderPreservingMap<string> ClickHouseDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &ClickHouseCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not supported for deletion from ClickHouse table");
	}
	auto &ch_table = op.table.Cast<ClickHouseTableEntry>();
	string pk_column;
	if (!ch_table.TryGetRowIdColumn(pk_column)) {
		throw BinderException("Cannot DELETE from ClickHouse table \"%s\": it has no single integer PRIMARY KEY to "
		                      "use as a row identifier",
		                      ch_table.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto &delete_op = planner.Make<ClickHouseDelete>(op, op.table, bound_ref.index, std::move(pk_column));
	delete_op.children.push_back(plan);
	return delete_op;
}

} // namespace duckdb
