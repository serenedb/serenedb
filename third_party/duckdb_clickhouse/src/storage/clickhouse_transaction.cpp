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
	// A committed statement's connection completed cleanly (discrete Execute/Select),
	// so it is safe to return to the pool for reuse. Read-only transactions never
	// opened `connection` (scans use their own), so this is a no-op for them.
	if (connection.IsOpen()) {
		clickhouse_catalog.ReturnConnection(std::move(connection));
	}
}

void ClickHouseTransaction::Rollback() {
	// On rollback the connection may have thrown mid-statement; drop it rather than
	// risk poisoning the pool (the destructor closes it).
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
