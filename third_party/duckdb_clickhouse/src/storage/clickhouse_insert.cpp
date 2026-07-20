#include "storage/clickhouse_insert.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction.hpp"
#include "storage/clickhouse_table_entry.hpp"
#include "storage/clickhouse_schema_entry.hpp"
#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/exceptions.h>

namespace duckdb {

ClickHouseInsert::ClickHouseInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                                   physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

ClickHouseInsert::ClickHouseInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                                   unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class ClickHouseInsertGlobalState : public GlobalSinkState {
public:
	explicit ClickHouseInsertGlobalState(ClickHouseTableEntry &table) : table(table), insert_count(0) {
	}

	ClickHouseTableEntry &table;
	idx_t insert_count;
	string qualified_table;
	vector<string> column_names;
	vector<string> column_types;
	//! The connector's DuckDB-side mapping of column_types; Sink casts incoming
	//! chunks to these before the byte-level append (see the CTAS note there).
	vector<LogicalType> expected_types;
};

// Mirrors PostgresInsert::GetInsertColumns: maps each chunk position to the table column logical index.
static vector<idx_t> GetInsertColumnIndexes(const ClickHouseInsert &insert, ClickHouseTableEntry &entry) {
	vector<idx_t> result;
	auto &columns = entry.GetColumns();
	if (insert.column_index_map.empty()) {
		for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
			result.push_back(i);
		}
		return result;
	}
	idx_t column_count = 0;
	vector<PhysicalIndex> column_indexes;
	column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
	for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
		auto column_index = PhysicalIndex(c);
		auto mapped_index = insert.column_index_map[column_index];
		if (mapped_index == DConstants::INVALID_INDEX) {
			continue;
		}
		column_indexes[mapped_index] = column_index;
		column_count++;
	}
	for (idx_t c = 0; c < column_count; c++) {
		result.push_back(column_indexes[c].index);
	}
	return result;
}

unique_ptr<GlobalSinkState> ClickHouseInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<ClickHouseTableEntry> insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<ClickHouseTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<ClickHouseTableEntry>();
	}
	auto result = make_uniq<ClickHouseInsertGlobalState>(*insert_table);
	result->qualified_table =
	    ClickHouseQuoteIdentifier(insert_table->database) + "." + ClickHouseQuoteIdentifier(insert_table->table);

	auto indexes = GetInsertColumnIndexes(*this, *insert_table);
	auto &columns = insert_table->GetColumns();
	for (auto col_index : indexes) {
		result->column_names.push_back(columns.GetColumn(LogicalIndex(col_index)).GetName().GetIdentifierName());
		if (col_index >= insert_table->clickhouse_types.size()) {
			throw InternalException("ClickHouse INSERT: column index out of range for type metadata");
		}
		result->column_types.push_back(insert_table->clickhouse_types[col_index]);
		result->expected_types.push_back(ClickHouseTypeStringToLogicalType(insert_table->clickhouse_types[col_index]));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType ClickHouseInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ClickHouseInsertGlobalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &transaction = ClickHouseTransaction::Get(context.client, gstate.table.catalog);
	auto &connection = transaction.GetConnection();

	// A CTAS plan delivers the SELECT's own vector types (no binder cast is
	// inserted, unlike INSERT INTO): an ENUM source arrives as dictionary codes
	// and a TIMESTAMP_S/_MS/_NS source as non-microsecond epochs, which the
	// byte-level append below would misread. Cast to the connector's mapped
	// types first; the aligned case costs nothing.
	DataChunk cast_chunk;
	DataChunk *write_chunk = &chunk;
	if (chunk.GetTypes() != gstate.expected_types) {
		cast_chunk.Initialize(context.client, gstate.expected_types);
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			VectorOperations::Cast(context.client, chunk.data[c], cast_chunk.data[c], chunk.size());
		}
		cast_chunk.SetCardinality(chunk.size());
		write_chunk = &cast_chunk;
	}

	clickhouse::Block block;
	for (idx_t c = 0; c < gstate.column_names.size(); c++) {
		auto col = ClickHouseColumnFromVector(gstate.column_types[c], write_chunk->data[c], write_chunk->size());
		block.AppendColumn(gstate.column_names[c], col);
	}
	block.RefreshRowCount();

	try {
		ClickHouseConnection::LogQuery("INSERT INTO " + gstate.qualified_table + " (" + std::to_string(block.GetRowCount()) +
		                               " rows)");
		connection.GetClient().Insert(gstate.qualified_table, block);
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error during INSERT into %s: %s", gstate.qualified_table, e.what());
	}
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType ClickHouseInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType ClickHouseInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<ClickHouseInsertGlobalState>();
	chunk.SetChildCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string ClickHouseInsert::GetName() const {
	return table ? "CLICKHOUSE_INSERT" : "CLICKHOUSE_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> ClickHouseInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name.GetIdentifierName() : info->Base().GetTableName().GetIdentifierName();
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &ClickHouseCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not supported for insertion into ClickHouse table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not supported for insertion into ClickHouse table");
	}
	D_ASSERT(plan);
	auto &insert = planner.Make<ClickHouseInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &ClickHouseCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<ClickHouseInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

} // namespace duckdb
