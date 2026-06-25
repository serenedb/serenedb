#include "duckdb.hpp"

#include "storage/clickhouse_transaction.hpp"
#include "storage/clickhouse_catalog.hpp"

namespace duckdb {

ClickHouseTransaction::ClickHouseTransaction(ClickHouseCatalog &clickhouse_catalog, TransactionManager &manager,
                                             ClientContext &context)
    : Transaction(manager, context), clickhouse_catalog(clickhouse_catalog) {
}

ClickHouseTransaction::~ClickHouseTransaction() = default;

void ClickHouseTransaction::Start() {
}

void ClickHouseTransaction::Commit() {
}

void ClickHouseTransaction::Rollback() {
}

ClickHouseConnection &ClickHouseTransaction::GetConnection() {
	if (!connection.IsOpen()) {
		connection = clickhouse_catalog.OpenConnection();
	}
	return connection;
}

ClickHouseTransaction &ClickHouseTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<ClickHouseTransaction>();
}

} // namespace duckdb
