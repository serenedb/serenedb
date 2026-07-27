#include "duckdb.hpp"

#include "duckdb/main/attached_database.hpp"

#include "storage/clickhouse_transaction_manager.hpp"

namespace duckdb {

ClickHouseTransactionManager::ClickHouseTransactionManager(AttachedDatabase &db_p, ClickHouseCatalog &clickhouse_catalog)
    : TransactionManager(db_p), clickhouse_catalog(clickhouse_catalog) {
}

Transaction &ClickHouseTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<ClickHouseTransaction>(clickhouse_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData ClickHouseTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &clickhouse_transaction = transaction.Cast<ClickHouseTransaction>();
	clickhouse_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void ClickHouseTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &clickhouse_transaction = transaction.Cast<ClickHouseTransaction>();
	clickhouse_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void ClickHouseTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb
