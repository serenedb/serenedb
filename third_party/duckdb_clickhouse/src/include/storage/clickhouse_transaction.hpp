//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/mutex.hpp"
#include "clickhouse_connection.hpp"
#include "storage/clickhouse_connection_pool.hpp"

namespace duckdb {
class ClickHouseCatalog;
class ClickHouseSchemaEntry;
class ClickHouseTableEntry;

class ClickHouseTransaction : public Transaction {
public:
	ClickHouseTransaction(ClickHouseCatalog &clickhouse_catalog, TransactionManager &manager, ClientContext &context);
	~ClickHouseTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	ClickHouseConnection &GetConnection();
	//! Drop the leased connection (protocol desync: unread stream blocks); the
	//! next GetConnection leases a fresh one.
	void InvalidateConnection() {
		connection.Invalidate();
	}

	static ClickHouseTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	ClickHouseCatalog &clickhouse_catalog;
	ClickHousePoolConnection connection;
};

} // namespace duckdb
