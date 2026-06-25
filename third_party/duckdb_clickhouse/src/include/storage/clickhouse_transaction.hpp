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

	static ClickHouseTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	ClickHouseCatalog &clickhouse_catalog;
	ClickHouseConnection connection;
};

} // namespace duckdb
