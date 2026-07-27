//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "dbconnector/pool.hpp"

#include "clickhouse_connection.hpp"

namespace duckdb {
class ClickHouseCatalog;
class ClickHouseConnectionPool;

using ClickHousePoolConnection = dbconnector::pool::PooledConnection<ClickHouseConnection>;

//! The ClickHouse mirror of PostgresConnectionPool: the shared dbconnector pool
//! (health checks on lease, idle/lifetime reaping, FORCE/WAIT/TRY acquire modes,
//! optional thread-local cache), configured from the ch_pool_* settings.
//! Divergences from postgres, both inherent to the native protocol:
//!  - the health check is the protocol-level Ping(), not a SQL query, so there
//!    is no ch_pool_health_check_query knob;
//!  - ResetConnection is a no-op (a ClickHouse connection carries no session
//!    state between discrete queries); a connection abandoned mid-stream must
//!    be Invalidate()d by its holder, and Ping() catches the rest on lease.
class ClickHouseConnectionPool : public dbconnector::pool::ConnectionPool<ClickHouseConnection> {
public:
	ClickHouseConnectionPool(ClickHouseCatalog &clickhouse_catalog, ClientContext &context);

public:
	//! Lease a connection. When ch_connection_cache is off (the dynamic kill
	//! switch, kept from the pre-pool cache) the pool is bypassed entirely: a
	//! fresh detached connection that is simply dropped on release.
	ClickHousePoolConnection GetConnection();

	static void ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter);

protected:
	std::unique_ptr<ClickHouseConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(ClickHouseConnection &conn) override;
	void ResetConnection(ClickHouseConnection &conn) override;

private:
	ClickHouseCatalog &clickhouse_catalog;
};

class ClickHouseConfigurePoolFunction : public TableFunction {
public:
	ClickHouseConfigurePoolFunction();
};

} // namespace duckdb
