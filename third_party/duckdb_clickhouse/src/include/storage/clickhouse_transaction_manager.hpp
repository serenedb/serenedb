//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction.hpp"

namespace duckdb {

class ClickHouseTransactionManager : public TransactionManager {
public:
	ClickHouseTransactionManager(AttachedDatabase &db_p, ClickHouseCatalog &clickhouse_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	ClickHouseCatalog &clickhouse_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<ClickHouseTransaction>> transactions;
};

} // namespace duckdb
