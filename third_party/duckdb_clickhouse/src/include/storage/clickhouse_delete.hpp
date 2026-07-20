//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class ClickHouseDelete : public PhysicalOperator {
public:
	ClickHouseDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, idx_t row_id_index,
	                 string pk_column);

	//! The table to delete from
	TableCatalogEntry &table;
	idx_t row_id_index;
	//! Primary-key column used as the row identifier in the remote DELETE
	string pk_column;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
